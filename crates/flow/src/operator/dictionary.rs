// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, slice, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{FieldRef, SchemaRef};
use reifydb_core::{
	expression::{
		AndExpression, ColumnExpression, ConstantExpression, EqExpression, Expression, InExpression,
		ListExpression, NotEqExpression, OrExpression, PrefixExpression, PrefixOperator, TupleExpression,
		XorExpression,
	},
	interface::{
		catalog::dictionary::Dictionary,
		identifier::{ColumnIdentifier, ColumnObject},
	},
	value::{
		batch::batch,
		column::{builder::ColumnBuilder, dictionary::decode_dictionary_ids},
	},
};
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	value::{
		Value,
		dictionary::{DictionaryEntryId, DictionaryId},
		system_columns::resolve_column,
		value_type::{ValueType, field::from_field},
	},
};

use crate::{
	error::FlowSinkError,
	txn::{Find, Resolve},
};

pub(crate) struct DictionaryBinding {
	schema: SchemaRef,
	expressions: Vec<Expression>,
	ids: Vec<usize>,
	literals: Vec<(usize, Value)>,
	decoded: Vec<usize>,
}

pub(crate) struct BoundLiterals {
	dictionaries: HashMap<usize, Dictionary>,
	ids: Vec<DictionaryEntryId>,
}

impl DictionaryBinding {
	pub(crate) fn new(columns: &RecordBatch, expressions: &[Expression]) -> Result<Self> {
		let schema = columns.schema();
		let mut dictionary_columns = HashMap::new();
		for (position, field) in schema.fields().iter().enumerate() {
			if let Some(dictionary) = dictionary_of(field)? {
				dictionary_columns.insert(position, dictionary);
			}
		}
		let mut binder = Binder {
			columns,
			dictionary_columns: &dictionary_columns,
			ids: Vec::new(),
			literals: Vec::new(),
		};
		let expressions: Vec<Expression> =
			expressions.iter().map(|expression| binder.bind(expression)).collect();
		let (ids, literals) = (binder.ids, binder.literals);
		let mut decoded: Vec<usize> = dictionary_columns
			.keys()
			.copied()
			.filter(|position| {
				let name = schema.field(*position).name();
				expressions.iter().any(|expression| mentions(expression, name))
			})
			.collect();
		decoded.sort_unstable();
		Ok(Self {
			schema,
			expressions,
			ids,
			literals,
			decoded,
		})
	}

	pub(crate) fn matches(&self, schema: &SchemaRef) -> bool {
		self.schema == *schema
	}

	pub(crate) fn expressions(&self) -> &[Expression] {
		&self.expressions
	}

	pub(crate) fn bind_literals<T: Find + ?Sized>(&self, txn: &mut T) -> Result<BoundLiterals> {
		let mut dictionaries: HashMap<usize, Dictionary> = HashMap::new();
		for position in self.decoded.iter().chain(self.literals.iter().map(|(p, _)| p)) {
			if !dictionaries.contains_key(position) {
				let dictionary = self.dictionary(txn, *position)?;
				dictionaries.insert(*position, dictionary);
			}
		}
		let mut ids: Vec<Option<DictionaryEntryId>> = vec![None; self.literals.len()];
		let mut by_dictionary: HashMap<DictionaryId, Vec<usize>> = HashMap::new();
		for (slot, (position, _)) in self.literals.iter().enumerate() {
			by_dictionary.entry(dictionaries[position].id).or_default().push(slot);
		}
		for slots in by_dictionary.values() {
			let dictionary = &dictionaries[&self.literals[slots[0]].0];
			let values: Vec<Value> = slots.iter().map(|slot| self.literals[*slot].1.clone()).collect();
			for (slot, found) in slots.iter().zip(txn.find(dictionary, &values)?) {
				ids[*slot] = found;
			}
		}
		let ids = ids
			.into_iter()
			.zip(&self.literals)
			.map(|(found, (position, _))| match found {
				Some(id) => Ok(id),
				None => DictionaryEntryId::from_u128(0, dictionaries[position].id_type.clone()),
			})
			.collect::<Result<Vec<_>>>()?;
		Ok(BoundLiterals {
			dictionaries,
			ids,
		})
	}

	pub(crate) fn columns<T: Resolve + ?Sized>(
		&self,
		txn: &mut T,
		literals: &BoundLiterals,
		columns: &RecordBatch,
	) -> Result<RecordBatch> {
		if self.ids.is_empty() && self.literals.is_empty() && self.decoded.is_empty() {
			return Ok(columns.clone());
		}
		let mut fields: Vec<(FieldRef, ArrayRef)> =
			columns.schema_ref().fields().iter().cloned().zip(columns.columns().iter().cloned()).collect();
		let mut appended: Vec<(FieldRef, ArrayRef)> = Vec::with_capacity(self.ids.len() + self.literals.len());
		for (slot, position) in self.ids.iter().enumerate() {
			let (field, array) = &fields[*position];
			appended.push((Arc::new(field.as_ref().clone().with_name(id_name(slot))), array.clone()));
		}
		for (slot, ((position, _), id)) in self.literals.iter().zip(&literals.ids).enumerate() {
			let mut builder = ColumnBuilder::with_capacity(ValueType::DictionaryId, columns.num_rows());
			builder.set_dictionary_id(literals.dictionaries[position].id);
			for _ in 0..columns.num_rows() {
				builder.push(*id);
			}
			appended.push(builder.finish(&literal_name(slot)));
		}
		for position in &self.decoded {
			let dictionary = &literals.dictionaries[position];
			let (field, array) = &fields[*position];
			fields[*position] =
				decode_dictionary_ids(field, array, dictionary.value_type.clone(), |ids| {
					txn.resolve(dictionary, ids)
				})?;
		}
		fields.extend(appended);
		batch(fields)
	}

	fn dictionary<T: Resolve + ?Sized>(&self, txn: &mut T, position: usize) -> Result<Dictionary> {
		let field = self.schema.field(position);
		let id = from_field(field)?.dictionary_id.expect("a bound dictionary column carries its dictionary");
		txn.dictionary(id)?.ok_or_else(|| {
			Error::from(FlowSinkError::DictionaryNotFound {
				dictionary_id: format!("{:?}", id),
				column: field.name().clone(),
			})
		})
	}
}

struct Binder<'a> {
	columns: &'a RecordBatch,
	dictionary_columns: &'a HashMap<usize, DictionaryId>,
	ids: Vec<usize>,
	literals: Vec<(usize, Value)>,
}

impl Binder<'_> {
	fn bind(&mut self, expression: &Expression) -> Expression {
		match expression {
			Expression::And(e) => Expression::And(AndExpression {
				left: Box::new(self.bind(&e.left)),
				right: Box::new(self.bind(&e.right)),
				fragment: e.fragment.clone(),
			}),
			Expression::Or(e) => Expression::Or(OrExpression {
				left: Box::new(self.bind(&e.left)),
				right: Box::new(self.bind(&e.right)),
				fragment: e.fragment.clone(),
			}),
			Expression::Xor(e) => Expression::Xor(XorExpression {
				left: Box::new(self.bind(&e.left)),
				right: Box::new(self.bind(&e.right)),
				fragment: e.fragment.clone(),
			}),
			Expression::Tuple(e) if e.expressions.len() == 1 => Expression::Tuple(TupleExpression {
				expressions: vec![self.bind(&e.expressions[0])],
				fragment: e.fragment.clone(),
			}),
			Expression::Prefix(e) if matches!(e.operator, PrefixOperator::Not(_)) => {
				Expression::Prefix(PrefixExpression {
					operator: e.operator.clone(),
					expression: Box::new(self.bind(&e.expression)),
					fragment: e.fragment.clone(),
				})
			}
			Expression::Equal(e) => match self.pair(&e.left, &e.right) {
				Some((left, right)) => Expression::Equal(EqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: e.fragment.clone(),
				}),
				None => expression.clone(),
			},
			Expression::NotEqual(e) => match self.pair(&e.left, &e.right) {
				Some((left, right)) => Expression::NotEqual(NotEqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: e.fragment.clone(),
				}),
				None => expression.clone(),
			},
			Expression::In(e) => match self.list(&e.value, &e.list) {
				Some((value, list)) => Expression::In(InExpression {
					value: Box::new(value),
					list: Box::new(list),
					negated: e.negated,
					fragment: e.fragment.clone(),
				}),
				None => expression.clone(),
			},
			_ => expression.clone(),
		}
	}

	fn pair(&mut self, left: &Expression, right: &Expression) -> Option<(Expression, Expression)> {
		match (self.dictionary_column(left), self.dictionary_column(right)) {
			(Some((l, ld)), Some((r, rd))) if ld == rd => Some((self.id(l), self.id(r))),
			(Some((position, _)), None) => {
				let text = text_constant(right)?;
				Some((self.id(position), self.literal(position, text)))
			}
			(None, Some((position, _))) => {
				let text = text_constant(left)?;
				Some((self.literal(position, text), self.id(position)))
			}
			_ => None,
		}
	}

	fn list(&mut self, value: &Expression, list: &Expression) -> Option<(Expression, Expression)> {
		let (position, _) = self.dictionary_column(value)?;
		let items = match list {
			Expression::Tuple(tuple) => &tuple.expressions,
			Expression::List(list) => &list.expressions,
			single => slice::from_ref(single),
		};
		let texts: Vec<Value> = items.iter().map(text_constant).collect::<Option<_>>()?;
		let bound: Vec<Expression> = texts.into_iter().map(|text| self.literal(position, text)).collect();
		let list = match list {
			Expression::Tuple(tuple) => Expression::Tuple(TupleExpression {
				expressions: bound,
				fragment: tuple.fragment.clone(),
			}),
			Expression::List(list) => Expression::List(ListExpression {
				expressions: bound,
				fragment: list.fragment.clone(),
			}),
			_ => bound.into_iter().next().expect("a single list item binds to one literal"),
		};
		Some((self.id(position), list))
	}

	fn dictionary_column(&self, expression: &Expression) -> Option<(usize, DictionaryId)> {
		let Expression::Column(column) = expression else {
			return None;
		};
		let position = resolve_column(self.columns, column.0.name.text())?;
		self.dictionary_columns.get(&position).map(|dictionary| (position, *dictionary))
	}

	fn id(&mut self, position: usize) -> Expression {
		let slot = match self.ids.iter().position(|p| *p == position) {
			Some(slot) => slot,
			None => {
				self.ids.push(position);
				self.ids.len() - 1
			}
		};
		synthetic(id_name(slot))
	}

	fn literal(&mut self, position: usize, text: Value) -> Expression {
		let slot = match self.literals.iter().position(|(p, v)| *p == position && *v == text) {
			Some(slot) => slot,
			None => {
				self.literals.push((position, text));
				self.literals.len() - 1
			}
		};
		synthetic(literal_name(slot))
	}
}

fn dictionary_of(field: &FieldRef) -> Result<Option<DictionaryId>> {
	let field_type = from_field(field)?;
	let bare = match field_type.value_type {
		Some(ValueType::Option(inner)) => *inner,
		Some(bare) => bare,
		None => return Ok(None),
	};
	Ok(match bare {
		ValueType::DictionaryId => field_type.dictionary_id,
		_ => None,
	})
}

fn text_constant(expression: &Expression) -> Option<Value> {
	match expression {
		Expression::Constant(ConstantExpression::Text {
			fragment,
		}) => Some(Value::Utf8(fragment.text().to_string())),
		_ => None,
	}
}

fn synthetic(name: String) -> Expression {
	Expression::Column(ColumnExpression(ColumnIdentifier {
		object: ColumnObject::Alias(Fragment::internal("")),
		name: Fragment::internal(name),
	}))
}

fn id_name(slot: usize) -> String {
	format!("__dictionary_id_{slot}")
}

fn literal_name(slot: usize) -> String {
	format!("__dictionary_literal_{slot}")
}

fn mentions(expression: &Expression, name: &str) -> bool {
	match expression {
		Expression::Column(column) => column.0.name.text() == name,
		Expression::AccessSource(access) => access.column.name.text() == name,
		Expression::Constant(_)
		| Expression::Parameter(_)
		| Expression::Variable(_)
		| Expression::Type(_)
		| Expression::SumTypeConstructor(_)
		| Expression::IsVariant(_) => false,
		Expression::Call(e) => e.args.iter().any(|arg| mentions(arg, name)),
		Expression::Add(e) => mentions(&e.left, name) || mentions(&e.right, name),
		Expression::Sub(e) => mentions(&e.left, name) || mentions(&e.right, name),
		Expression::Mul(e) => mentions(&e.left, name) || mentions(&e.right, name),
		Expression::Div(e) => mentions(&e.left, name) || mentions(&e.right, name),
		Expression::Rem(e) => mentions(&e.left, name) || mentions(&e.right, name),
		Expression::GreaterThan(e) => mentions(&e.left, name) || mentions(&e.right, name),
		Expression::GreaterThanEqual(e) => mentions(&e.left, name) || mentions(&e.right, name),
		Expression::LessThan(e) => mentions(&e.left, name) || mentions(&e.right, name),
		Expression::LessThanEqual(e) => mentions(&e.left, name) || mentions(&e.right, name),
		Expression::Equal(e) => mentions(&e.left, name) || mentions(&e.right, name),
		Expression::NotEqual(e) => mentions(&e.left, name) || mentions(&e.right, name),
		Expression::And(e) => mentions(&e.left, name) || mentions(&e.right, name),
		Expression::Or(e) => mentions(&e.left, name) || mentions(&e.right, name),
		Expression::Xor(e) => mentions(&e.left, name) || mentions(&e.right, name),
		Expression::Between(e) => {
			mentions(&e.value, name) || mentions(&e.lower, name) || mentions(&e.upper, name)
		}
		Expression::In(e) => mentions(&e.value, name) || mentions(&e.list, name),
		Expression::Contains(e) => mentions(&e.value, name) || mentions(&e.list, name),
		Expression::Cast(e) => mentions(&e.expression, name),
		Expression::Prefix(e) => mentions(&e.expression, name),
		Expression::Alias(e) => mentions(&e.expression, name),
		Expression::FieldAccess(e) => mentions(&e.object, name),
		Expression::If(e) => {
			mentions(&e.condition, name)
				|| mentions(&e.then_expr, name)
				|| e.else_ifs.iter().any(|branch| {
					mentions(&branch.condition, name) || mentions(&branch.then_expr, name)
				}) || e.else_expr.as_ref().is_some_and(|branch| mentions(branch, name))
		}
		Expression::Tuple(e) => e.expressions.iter().any(|item| mentions(item, name)),
		Expression::List(e) => e.expressions.iter().any(|item| mentions(item, name)),
		Expression::Map(e) => e.expressions.iter().any(|item| mentions(item, name)),
		Expression::Extend(e) => e.expressions.iter().any(|item| mentions(item, name)),
	}
}
