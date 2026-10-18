// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, fs, sync::Arc, thread};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{FieldRef, Schema};
use reifydb_core::{
	expression::{
		AccessObjectExpression, AddExpression, AliasExpression, AndExpression, BetweenExpression,
		CallExpression, CastExpression, ColumnExpression, ConstantExpression, ContainsExpression,
		DivExpression, ElseIfExpression, EqExpression, Expression, FieldAccessExpression,
		GreaterThanEqExpression, GreaterThanExpression, IdentExpression, IfExpression, InExpression,
		IsVariantExpression, LessThanEqExpression, LessThanExpression, ListExpression, MapExpression,
		MulExpression, NotEqExpression, OrExpression, PrefixExpression, PrefixOperator, RemExpression,
		SubExpression, TupleExpression, TypeExpression, VariableExpression, XorExpression,
	},
	interface::identifier::{ColumnIdentifier, ColumnObject},
	value::column::factory,
};
use reifydb_evaluate::{
	expression::context::EvalContext,
	lower::LoweredExpr,
	stack::{ClosureValue, SymbolTable, Variable},
};
use reifydb_routine::function::default_in_process_functions;
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::instruction::CompiledClosure;
use reifydb_runtime::context::{RuntimeContext, clock::Clock};
use reifydb_value::{
	Result,
	fragment::Fragment,
	params::Params,
	value::{column_view::ColumnView, constraint::Constraint, identity::IdentityId, value_type::ValueType},
};

pub struct Env {
	params: Params,
	pub symbols: SymbolTable,
	routines: Routines,
	runtime_context: RuntimeContext,
}

impl Env {
	pub fn new() -> Self {
		Self::with_symbols(SymbolTable::new())
	}

	pub fn with_symbols(symbols: SymbolTable) -> Self {
		Self {
			params: Params::None,
			symbols,
			routines: Routines::empty(),
			runtime_context: RuntimeContext::with_clock(Clock::Real),
		}
	}

	pub fn with_params(params: Params) -> Self {
		Self {
			params,
			..Self::new()
		}
	}

	pub fn with_routines(routines: Routines, runtime_context: RuntimeContext) -> Self {
		Self {
			params: Params::None,
			symbols: SymbolTable::new(),
			routines,
			runtime_context,
		}
	}

	pub fn ctx(&self, batch: RecordBatch) -> EvalContext<'_> {
		let row_count = batch.num_rows();
		EvalContext {
			target: None,
			batch,
			row_count,
			take: None,
			params: &self.params,
			symbols: &self.symbols,
			routines: &self.routines,
			runtime_context: &self.runtime_context,
			identity: IdentityId::root(),
		}
	}

	pub fn lowered(&self, expression: &Expression, batch: RecordBatch) -> Result<(FieldRef, ArrayRef)> {
		LoweredExpr::new(expression.clone(), "test").evaluate(&self.ctx(batch))
	}
}

pub fn batch(columns: Vec<(FieldRef, ArrayRef)>) -> RecordBatch {
	let (fields, arrays): (Vec<FieldRef>, Vec<ArrayRef>) = columns.into_iter().unzip();
	RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

pub fn rows(count: usize) -> RecordBatch {
	batch(vec![factory::int4("filler", vec![0; count])])
}

pub fn strings(result: &(FieldRef, ArrayRef)) -> Vec<String> {
	let view = ColumnView::try_from(result).unwrap();
	(0..result.1.len()).map(|i| view.as_string(i)).collect()
}

pub fn frag(text: &str) -> Fragment {
	Fragment::internal(text)
}

pub fn column(name: &str) -> Expression {
	Expression::Column(ColumnExpression(ColumnIdentifier {
		object: ColumnObject::Alias(frag("t")),
		name: frag(name),
	}))
}

pub fn access(object: ColumnObject, name: &str) -> Expression {
	Expression::AccessSource(AccessObjectExpression {
		column: ColumnIdentifier {
			object,
			name: frag(name),
		},
	})
}

pub fn none() -> Expression {
	Expression::Constant(ConstantExpression::None {
		fragment: frag("none"),
	})
}

pub fn boolean(text: &str) -> Expression {
	Expression::Constant(ConstantExpression::Bool {
		fragment: frag(text),
	})
}

pub fn number(text: &str) -> Expression {
	Expression::Constant(ConstantExpression::Number {
		fragment: frag(text),
	})
}

pub fn text(text: &str) -> Expression {
	Expression::Constant(ConstantExpression::Text {
		fragment: frag(text),
	})
}

pub fn temporal(text: &str) -> Expression {
	Expression::Constant(ConstantExpression::Temporal {
		fragment: frag(text),
	})
}

pub fn duration(text: &str) -> Expression {
	Expression::Constant(ConstantExpression::Duration {
		fragment: frag(text),
	})
}

pub fn and(left: Expression, right: Expression) -> Expression {
	Expression::And(AndExpression {
		left: Box::new(left),
		right: Box::new(right),
		fragment: frag("and"),
	})
}

pub fn or(left: Expression, right: Expression) -> Expression {
	Expression::Or(OrExpression {
		left: Box::new(left),
		right: Box::new(right),
		fragment: frag("or"),
	})
}

pub fn xor(left: Expression, right: Expression) -> Expression {
	Expression::Xor(XorExpression {
		left: Box::new(left),
		right: Box::new(right),
		fragment: frag("xor"),
	})
}

pub fn not(inner: Expression) -> Expression {
	prefix(PrefixOperator::Not(frag("not")), inner)
}

pub fn prefix(operator: PrefixOperator, inner: Expression) -> Expression {
	Expression::Prefix(PrefixExpression {
		operator,
		expression: Box::new(inner),
		fragment: frag("prefix"),
	})
}

pub const COMPARE_OPS: [&str; 6] = ["==", "!=", "<", "<=", ">", ">="];

pub fn compare(op: &str, left: Expression, right: Expression) -> Expression {
	let (left, right, fragment) = (Box::new(left), Box::new(right), frag(op));
	match op {
		"==" => Expression::Equal(EqExpression {
			left,
			right,
			fragment,
		}),
		"!=" => Expression::NotEqual(NotEqExpression {
			left,
			right,
			fragment,
		}),
		"<" => Expression::LessThan(LessThanExpression {
			left,
			right,
			fragment,
		}),
		"<=" => Expression::LessThanEqual(LessThanEqExpression {
			left,
			right,
			fragment,
		}),
		">" => Expression::GreaterThan(GreaterThanExpression {
			left,
			right,
			fragment,
		}),
		">=" => Expression::GreaterThanEqual(GreaterThanEqExpression {
			left,
			right,
			fragment,
		}),
		other => panic!("no compare operator {other}"),
	}
}

pub fn between(value: Expression, lower: Expression, upper: Expression) -> Expression {
	Expression::Between(BetweenExpression {
		value: Box::new(value),
		lower: Box::new(lower),
		upper: Box::new(upper),
		fragment: frag("between"),
	})
}

pub const ARITH_OPS: [&str; 5] = ["+", "-", "*", "/", "%"];

pub fn arith(op: &str, left: Expression, right: Expression) -> Expression {
	let (left, right, fragment) = (Box::new(left), Box::new(right), frag(op));
	match op {
		"+" => Expression::Add(AddExpression {
			left,
			right,
			fragment,
		}),
		"-" => Expression::Sub(SubExpression {
			left,
			right,
			fragment,
		}),
		"*" => Expression::Mul(MulExpression {
			left,
			right,
			fragment,
		}),
		"/" => Expression::Div(DivExpression {
			left,
			right,
			fragment,
		}),
		"%" => Expression::Rem(RemExpression {
			left,
			right,
			fragment,
		}),
		other => panic!("no arithmetic operator {other}"),
	}
}

pub fn registry() -> Routines {
	default_in_process_functions(Routines::builder()).configure()
}

pub fn call(name: &str, args: Vec<Expression>) -> Expression {
	Expression::Call(CallExpression {
		func: IdentExpression(frag(name)),
		args,
		fragment: frag("call"),
	})
}

pub fn variable(text: &str) -> Expression {
	Expression::Variable(VariableExpression {
		fragment: frag(text),
	})
}

pub fn field_access(object: Expression, field: &str) -> Expression {
	Expression::FieldAccess(FieldAccessExpression {
		object: Box::new(object),
		field: frag(field),
		fragment: frag("."),
	})
}

pub fn alias(name: &str, inner: Expression) -> Expression {
	Expression::Alias(AliasExpression {
		alias: IdentExpression(frag(name)),
		expression: Box::new(inner),
		fragment: frag(":"),
	})
}

pub fn cast(inner: Expression, to: ValueType) -> Expression {
	Expression::Cast(CastExpression {
		fragment: frag("cast"),
		expression: Box::new(inner),
		to: TypeExpression {
			fragment: frag("type"),
			ty: to,
			constraint: None,
		},
	})
}

pub fn cast_constrained(inner: Expression, to: ValueType, constraint: Constraint) -> Expression {
	Expression::Cast(CastExpression {
		fragment: frag("cast"),
		expression: Box::new(inner),
		to: TypeExpression {
			fragment: frag("type"),
			ty: to,
			constraint: Some(constraint),
		},
	})
}

pub fn symbols(variables: Vec<(&str, Variable)>) -> SymbolTable {
	let mut symbols = SymbolTable::new();
	for (name, variable) in variables {
		symbols.set(name.to_string(), variable, false).unwrap();
	}
	symbols
}

pub fn closure() -> Variable {
	Variable::Closure(ClosureValue {
		def: CompiledClosure {
			parameters: vec![],
			body: vec![],
			captures: vec![],
		},
		captured: HashMap::new(),
	})
}

pub fn in_list(value: Expression, items: Vec<Expression>, negated: bool) -> Expression {
	Expression::In(InExpression {
		value: Box::new(value),
		list: Box::new(Expression::Tuple(TupleExpression {
			expressions: items,
			fragment: frag("(..)"),
		})),
		negated,
		fragment: frag(if negated {
			"not in"
		} else {
			"in"
		}),
	})
}

pub fn conditional(
	condition: Expression,
	then_expr: Expression,
	else_ifs: Vec<(Expression, Expression)>,
	else_expr: Option<Expression>,
) -> Expression {
	Expression::If(IfExpression {
		condition: Box::new(condition),
		then_expr: Box::new(then_expr),
		else_ifs: else_ifs
			.into_iter()
			.map(|(condition, then_expr)| ElseIfExpression {
				condition: Box::new(condition),
				then_expr: Box::new(then_expr),
				fragment: frag("else if"),
			})
			.collect(),
		else_expr: else_expr.map(Box::new),
		fragment: frag("if"),
	})
}

pub fn contains(value: Expression, items: Vec<Expression>) -> Expression {
	Expression::Contains(ContainsExpression {
		value: Box::new(value),
		list: Box::new(Expression::Tuple(TupleExpression {
			expressions: items,
			fragment: frag("(..)"),
		})),
		fragment: frag("contains"),
	})
}

pub fn is_variant(column_name: &str, tag: Option<u8>) -> Expression {
	Expression::IsVariant(IsVariantExpression {
		expression: Box::new(column(column_name)),
		namespace: None,
		sumtype_name: frag("shape"),
		variant_name: frag("circle"),
		tag,
		fragment: frag("is circle"),
	})
}

pub fn tuple(expressions: Vec<Expression>) -> Expression {
	Expression::Tuple(TupleExpression {
		expressions,
		fragment: frag("(..)"),
	})
}

pub fn list(expressions: Vec<Expression>) -> Expression {
	Expression::List(ListExpression {
		expressions,
		fragment: frag("[..]"),
	})
}

pub fn map(expressions: Vec<Expression>) -> Expression {
	Expression::Map(MapExpression {
		expressions,
		fragment: frag("map"),
	})
}

pub fn describe(result: &Result<(FieldRef, ArrayRef)>) -> String {
	match result {
		Ok(column) => format!("{:?} {:?}", column.0, strings(column)),
		Err(error) => format!("{} {:?} {} {:?}", error.code, error.fragment.text(), error.message, error.notes),
	}
	.replace('\n', "\\n")
}

pub struct Frozen {
	answers: Vec<String>,
	next: usize,
}

impl Frozen {
	pub fn load(test: &str) -> Self {
		let path = format!("{}/tests/lower/frozen/{test}.txt", env!("CARGO_MANIFEST_DIR"));
		let answers = fs::read_to_string(&path).unwrap().lines().map(str::to_string).collect();
		Self {
			answers,
			next: 0,
		}
	}

	pub fn inline(answers: &[&str]) -> Self {
		Self {
			answers: answers.iter().map(|answer| answer.to_string()).collect(),
			next: 0,
		}
	}

	pub fn recorded(&mut self) -> String {
		self.next()
	}

	pub fn old(&mut self, env: &Env, expression: &Expression, batch: RecordBatch) -> Result<(FieldRef, ArrayRef)> {
		self.answer(env.lowered(expression, batch))
	}

	pub fn answer(&mut self, result: Result<(FieldRef, ArrayRef)>) -> Result<(FieldRef, ArrayRef)> {
		let expected = self.next();
		assert_eq!(describe(&result), expected, "frozen old answer {}", self.next);
		result
	}

	pub fn next(&mut self) -> String {
		self.next += 1;
		self.answers[self.next - 1].clone()
	}

	pub fn exhausted(&self) -> bool {
		self.next == self.answers.len()
	}
}

impl Drop for Frozen {
	fn drop(&mut self) {
		if !thread::panicking() {
			assert!(
				self.exhausted(),
				"only {} of {} frozen old answers were checked",
				self.next,
				self.answers.len()
			);
		}
	}
}

pub fn code_of(answer: &str) -> &str {
	answer.split(' ').next().unwrap()
}

pub fn name_of(answer: &str) -> String {
	let mut name = String::new();
	let mut chars = answer.strip_prefix("Field { name: \"").unwrap().chars();
	while let Some(c) = chars.next() {
		match c {
			'\\' => name.push(chars.next().unwrap()),
			'"' => break,
			other => name.push(other),
		}
	}
	name
}

pub fn values_of(answer: &str) -> Vec<String> {
	let list = &answer[answer.rfind(" [").unwrap() + 2..answer.len() - 1];
	if list.is_empty() {
		return vec![];
	}
	list.split(", ").map(|value| value.trim_matches('"').to_string()).collect()
}
