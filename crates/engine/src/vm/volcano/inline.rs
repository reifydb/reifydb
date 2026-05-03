// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{BTreeSet, HashMap, HashSet},
	mem,
	sync::Arc,
};

use reifydb_core::{
	interface::{catalog::sumtype::SumType, evaluate::TargetColumn, resolved::ResolvedShape},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
};
use reifydb_rql::expression::{AliasExpression, ConstantExpression, Expression, IdentExpression};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	value::{Value, constraint::Constraint, r#type::Type},
};

use crate::{
	Result,
	expression::{cast::cast_column_data, context::EvalContext, eval::evaluate},
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) struct InlineDataNode {
	rows: Vec<Vec<AliasExpression>>,
	headers: Option<ColumnHeaders>,
	context: Option<Arc<QueryContext>>,
	executed: bool,
}

impl InlineDataNode {
	pub fn new(rows: Vec<Vec<AliasExpression>>, context: Arc<QueryContext>) -> Self {
		let cloned_context = context.clone();
		let headers = cloned_context.source.as_ref().map(|source| {
			let mut layout = Self::create_columns_layout_from_source(source);

			if matches!(source, ResolvedShape::Series(_)) {
				let existing: HashSet<String> =
					layout.columns.iter().map(|c| c.text().to_string()).collect();
				for row in &rows {
					for alias in row {
						let name = alias.alias.0.text().to_string();
						if !existing.contains(&name) {
							layout.columns.push(Fragment::internal(&name));
						}
					}
				}
			}
			layout
		});

		Self {
			rows,
			headers,
			context: Some(context),
			executed: false,
		}
	}

	fn create_columns_layout_from_source(source: &ResolvedShape) -> ColumnHeaders {
		ColumnHeaders {
			columns: source.columns().iter().map(|col| Fragment::internal(&col.name)).collect(),
		}
	}

	fn expand_sumtype_constructors<'a>(&mut self, txn: &mut Transaction<'a>) -> Result<()> {
		let Some(ctx) = self.context.as_ref().cloned() else {
			return Ok(());
		};
		if !rows_need_sumtype_expansion(&self.rows) {
			return Ok(());
		}
		for row in &mut self.rows {
			let original = mem::take(row);
			let mut expanded = Vec::with_capacity(original.len());
			for alias_expr in original {
				match alias_expr.expression.as_ref() {
					Expression::SumTypeConstructor(_) => {
						expand_sumtype_ctor(&ctx, txn, alias_expr, &mut expanded)?;
					}
					Expression::Column(_) => {
						expand_unit_variant_column(&ctx, txn, alias_expr, &mut expanded)?;
					}
					_ => expanded.push(alias_expr),
				}
			}
			*row = expanded;
		}
		Ok(())
	}
}

#[inline]
fn rows_need_sumtype_expansion(rows: &[Vec<AliasExpression>]) -> bool {
	for row in rows {
		for alias_expr in row {
			if matches!(
				alias_expr.expression.as_ref(),
				Expression::SumTypeConstructor(_) | Expression::Column(_)
			) {
				return true;
			}
		}
	}
	false
}

fn expand_sumtype_ctor<'a>(
	ctx: &Arc<QueryContext>,
	txn: &mut Transaction<'a>,
	alias_expr: AliasExpression,
	expanded: &mut Vec<AliasExpression>,
) -> Result<()> {
	let col_name = alias_expr.alias.0.text().to_string();
	let fragment = alias_expr.fragment.clone();

	let Expression::SumTypeConstructor(ctor) = *alias_expr.expression else {
		unreachable!()
	};

	let is_unresolved = ctor.namespace.text() == ctor.variant_name.text()
		&& ctor.sumtype_name.text() == ctor.variant_name.text();

	let sumtype = if is_unresolved {
		resolve_unresolved_sumtype(ctx, txn, &col_name)?
	} else {
		let ns_name = ctor.namespace.text();
		let ns = ctx.services.catalog.find_namespace_by_name(txn, ns_name)?.unwrap();
		let sumtype_name = ctor.sumtype_name.text();
		ctx.services.catalog.find_sumtype_by_name(txn, ns.id(), sumtype_name)?.unwrap()
	};

	let variant_name_lower = ctor.variant_name.text().to_lowercase();
	let variant = sumtype.variants.iter().find(|v| v.name == variant_name_lower).unwrap();

	expanded.push(AliasExpression {
		alias: IdentExpression(Fragment::internal(format!("{}_tag", col_name))),
		expression: Box::new(Expression::Constant(ConstantExpression::Number {
			fragment: Fragment::internal(variant.tag.to_string()),
		})),
		fragment: fragment.clone(),
	});

	for (field_name, field_expr) in ctor.columns {
		let phys_col_name = format!("{}_{}_{}", col_name, variant_name_lower, field_name.text().to_lowercase());
		expanded.push(AliasExpression {
			alias: IdentExpression(Fragment::internal(phys_col_name)),
			expression: Box::new(field_expr),
			fragment: fragment.clone(),
		});
	}

	Ok(())
}

#[inline]
fn resolve_unresolved_sumtype<'a>(
	ctx: &Arc<QueryContext>,
	txn: &mut Transaction<'a>,
	col_name: &str,
) -> Result<SumType> {
	let tag_col_name = format!("{}_tag", col_name);
	let source = ctx.source.as_ref().expect("source required for unresolved sumtype");

	if let Some(tag_col) = source.columns().iter().find(|c| c.name == tag_col_name) {
		let Some(Constraint::SumType(id)) = tag_col.constraint.constraint() else {
			panic!("expected SumType constraint on tag column")
		};
		ctx.services.catalog.get_sumtype(txn, *id)
	} else if let ResolvedShape::Series(series) = source {
		let tag_id = series.def().tag.expect("series tag expected");
		ctx.services.catalog.get_sumtype(txn, tag_id)
	} else {
		panic!("tag column not found: {}", tag_col_name)
	}
}

fn expand_unit_variant_column<'a>(
	ctx: &Arc<QueryContext>,
	txn: &mut Transaction<'a>,
	alias_expr: AliasExpression,
	expanded: &mut Vec<AliasExpression>,
) -> Result<()> {
	let col_name = alias_expr.alias.0.text().to_string();

	let resolved = if let Some(source) = ctx.source.as_ref() {
		let Expression::Column(col) = alias_expr.expression.as_ref() else {
			unreachable!()
		};
		try_resolve_unit_variant(ctx, txn, source, &col_name, col.0.name.text())?
	} else {
		None
	};

	let Some((sumtype, tag)) = resolved else {
		expanded.push(alias_expr);
		return Ok(());
	};

	let fragment = alias_expr.fragment.clone();
	expanded.push(AliasExpression {
		alias: IdentExpression(Fragment::internal(format!("{}_tag", col_name))),
		expression: Box::new(Expression::Constant(ConstantExpression::Number {
			fragment: Fragment::internal(tag.to_string()),
		})),
		fragment: fragment.clone(),
	});
	for v in &sumtype.variants {
		for field in &v.fields {
			let phys_col_name =
				format!("{}_{}_{}", col_name, v.name.to_lowercase(), field.name.to_lowercase());
			expanded.push(AliasExpression {
				alias: IdentExpression(Fragment::internal(phys_col_name)),
				expression: Box::new(Expression::Constant(ConstantExpression::None {
					fragment: fragment.clone(),
				})),
				fragment: fragment.clone(),
			});
		}
	}
	Ok(())
}

#[inline]
fn try_resolve_unit_variant<'a>(
	ctx: &Arc<QueryContext>,
	txn: &mut Transaction<'a>,
	source: &ResolvedShape,
	col_name: &str,
	alias_text: &str,
) -> Result<Option<(SumType, u8)>> {
	let tag_col_name = format!("{}_tag", col_name);

	if let Some(tag_col) = source.columns().iter().find(|c| c.name == tag_col_name) {
		let Some(Constraint::SumType(id)) = tag_col.constraint.constraint() else {
			return Ok(None);
		};
		let sumtype = ctx.services.catalog.get_sumtype(txn, *id)?;
		let variant_name_lower = alias_text.to_lowercase();
		let maybe_tag =
			sumtype.variants.iter().find(|v| v.name.to_lowercase() == variant_name_lower).map(|v| v.tag);
		return Ok(maybe_tag.map(|tag| (sumtype, tag)));
	}

	if let ResolvedShape::Series(series) = source
		&& let Some(tag_id) = series.def().tag
	{
		let sumtype = ctx.services.catalog.get_sumtype(txn, tag_id)?;
		let variant_name_lower = alias_text.to_lowercase();
		let maybe_tag =
			sumtype.variants.iter().find(|v| v.name.to_lowercase() == variant_name_lower).map(|v| v.tag);
		return Ok(maybe_tag.map(|tag| (sumtype, tag)));
	}

	Ok(None)
}

impl QueryNode for InlineDataNode {
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		self.expand_sumtype_constructors(rx)?;
		Ok(())
	}

	fn next<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "InlineDataNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap().clone();

		if self.executed {
			return Ok(None);
		}

		self.executed = true;

		if self.rows.is_empty() {
			let columns = Columns::empty();
			if self.headers.is_none() {
				self.headers = Some(ColumnHeaders::from_columns(&columns));
			}
			return Ok(Some(columns));
		}

		if self.headers.is_some() {
			self.next_with_source(&stored_ctx)
		} else {
			self.next_infer_namespace(&stored_ctx)
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone()
	}
}

impl InlineDataNode {
	fn find_optimal_integer_type(column: &ColumnBuffer) -> Type {
		let mut min_val = i128::MAX;
		let mut max_val = i128::MIN;
		let mut has_values = false;

		for value in column.iter() {
			match value {
				Value::Int16(v) => {
					has_values = true;
					min_val = min_val.min(v);
					max_val = max_val.max(v);
				}
				Value::None {
					..
				} => {}
				_ => {
					return Type::Int16;
				}
			}
		}

		if !has_values {
			return Type::Int1;
		}

		if min_val >= i8::MIN as i128 && max_val <= i8::MAX as i128 {
			Type::Int1
		} else if min_val >= i16::MIN as i128 && max_val <= i16::MAX as i128 {
			Type::Int2
		} else if min_val >= i32::MIN as i128 && max_val <= i32::MAX as i128 {
			Type::Int4
		} else if min_val >= i64::MIN as i128 && max_val <= i64::MAX as i128 {
			Type::Int8
		} else {
			Type::Int16
		}
	}

	fn next_infer_namespace(&mut self, ctx: &QueryContext) -> Result<Option<Columns>> {
		let mut all_columns: BTreeSet<String> = BTreeSet::new();

		for row in &self.rows {
			for keyed_expr in row {
				let column_name = keyed_expr.alias.0.text().to_string();
				all_columns.insert(column_name);
			}
		}

		let mut rows_data: Vec<HashMap<String, &AliasExpression>> = Vec::new();

		for row in &self.rows {
			let mut row_map: HashMap<String, &AliasExpression> = HashMap::new();
			for alias_expr in row {
				let column_name = alias_expr.alias.0.text().to_string();
				row_map.insert(column_name, alias_expr);
			}
			rows_data.push(row_map);
		}

		let session = EvalContext::from_query(ctx);

		let mut columns = Vec::new();

		for column_name in all_columns {
			let mut all_values = Vec::new();
			let mut first_value_type: Option<Type> = None;
			let mut column_fragment: Option<Fragment> = None;

			for row_data in &rows_data {
				if let Some(alias_expr) = row_data.get(&column_name) {
					if column_fragment.is_none() {
						column_fragment = Some(alias_expr.fragment.clone());
					}
					let eval_ctx = session.with_eval_empty();

					let evaluated = evaluate(&eval_ctx, &alias_expr.expression)?;

					let mut iter = evaluated.data().iter();
					if let Some(value) = iter.next() {
						if first_value_type.is_none() && !matches!(value, Value::None { .. }) {
							first_value_type = Some(value.get_type());
						}
						all_values.push(value);
					} else {
						all_values.push(Value::none());
					}
				} else {
					all_values.push(Value::none());
				}
			}

			let wide_type = if let Some(ref fvt) = first_value_type {
				if fvt.is_integer() {
					Some(Type::Int16)
				} else if fvt.is_floating_point() {
					Some(Type::Float8)
				} else if *fvt == Type::Utf8 {
					Some(Type::Utf8)
				} else if *fvt == Type::Boolean {
					Some(Type::Boolean)
				} else {
					None
				}
			} else {
				None
			};

			let mut column_data = if wide_type.is_none() {
				ColumnBuffer::none_typed(Type::Boolean, all_values.len())
			} else {
				let mut data = ColumnBuffer::with_capacity(wide_type.clone().unwrap(), 0);

				for value in &all_values {
					if matches!(value, Value::None { .. }) {
						data.push_none();
					} else if wide_type.as_ref().is_some_and(|wt| value.get_type() == *wt) {
						data.push_value(value.clone());
					} else {
						let temp_data = ColumnBuffer::from(value.clone());
						let eval_ctx = session.with_eval_empty();

						match cast_column_data(
							&eval_ctx,
							&temp_data,
							wide_type.clone().unwrap(),
							Fragment::none,
						) {
							Ok(casted) => {
								if let Some(casted_value) = casted.iter().next() {
									data.push_value(casted_value);
								} else {
									data.push_none();
								}
							}
							Err(_) => {
								data.push_none();
							}
						}
					}
				}

				data
			};

			if wide_type == Some(Type::Int16) {
				let optimal_type = Self::find_optimal_integer_type(&column_data);
				if optimal_type != Type::Int16 {
					let eval_ctx = session.with_eval(Columns::empty(), column_data.len());

					if let Ok(demoted) =
						cast_column_data(&eval_ctx, &column_data, optimal_type, || {
							Fragment::none()
						}) {
						column_data = demoted;
					}
				}
			}

			columns.push(ColumnWithName::new(
				column_fragment.unwrap_or_else(|| Fragment::internal(column_name)),
				column_data,
			));
		}

		let columns = Columns::new(columns);
		self.headers = Some(ColumnHeaders::from_columns(&columns));

		Ok(Some(columns))
	}

	fn next_with_source(&mut self, ctx: &QueryContext) -> Result<Option<Columns>> {
		let source = ctx.source.as_ref().unwrap();
		let headers = self.headers.as_ref().unwrap();
		let session = EvalContext::from_query(ctx);

		let mut rows_data: Vec<HashMap<String, &AliasExpression>> = Vec::new();

		for row in &self.rows {
			let mut row_map: HashMap<String, &AliasExpression> = HashMap::new();
			for alias_expr in row {
				let column_name = alias_expr.alias.0.text().to_string();
				row_map.insert(column_name, alias_expr);
			}
			rows_data.push(row_map);
		}

		let mut columns = Vec::new();

		for column_name in &headers.columns {
			let table_column = source.columns().iter().find(|col| col.name == column_name.text());

			let mut column_data = if let Some(tc) = table_column {
				ColumnBuffer::none_typed(tc.constraint.get_type(), 0)
			} else {
				ColumnBuffer::with_capacity(Type::Int16, 0)
			};
			let mut column_fragment: Option<Fragment> = None;

			for row_data in &rows_data {
				if let Some(alias_expr) = row_data.get(column_name.text()) {
					if column_fragment.is_none() {
						column_fragment = Some(alias_expr.fragment.clone());
					}
					let mut eval_ctx = session.with_eval_empty();
					eval_ctx.target = table_column.map(|tc| TargetColumn::Partial {
						source_name: Some(source.identifier().text().to_string()),
						column_name: Some(tc.name.clone()),
						column_type: tc.constraint.get_type(),
						properties: tc
							.properties
							.iter()
							.map(|cp| cp.property.clone())
							.collect(),
					});

					let evaluated = evaluate(&eval_ctx, &alias_expr.expression)?;

					let eval_len = evaluated.data().len();
					if table_column.is_some() {
						if eval_len == 1 {
							column_data.extend(evaluated.data().clone())?;
						} else if eval_len == 0 {
							column_data.push_value(Value::none());
						} else {
							let first_value =
								evaluated.data().iter().next().unwrap_or(Value::none());
							column_data.push_value(first_value);
						}
					} else {
						let value = if eval_len > 0 {
							evaluated.data().iter().next().unwrap_or(Value::none())
						} else {
							Value::none()
						};
						match &value {
							Value::None {
								..
							} => column_data.push_none(),
							Value::Int16(_) => column_data.push_value(value),
							_ => {
								let temp = ColumnBuffer::from(value.clone());
								match cast_column_data(
									&eval_ctx,
									&temp,
									Type::Int16,
									Fragment::none,
								) {
									Ok(casted) => {
										if let Some(v) = casted.iter().next() {
											column_data.push_value(v);
										} else {
											column_data.push_none();
										}
									}
									Err(_) => column_data.push_value(value),
								}
							}
						}
					}
				} else {
					column_data.push_value(Value::none());
				}
			}

			if table_column.is_none() {
				let optimal_type = Self::find_optimal_integer_type(&column_data);
				if optimal_type != Type::Int16 {
					let eval_ctx = session.with_eval(Columns::empty(), column_data.len());
					if let Ok(demoted) =
						cast_column_data(&eval_ctx, &column_data, optimal_type, || {
							Fragment::none()
						}) {
						column_data = demoted;
					}
				}
			}

			columns.push(ColumnWithName::new(
				column_fragment
					.map(|f| f.with_text(column_name.text()))
					.unwrap_or_else(|| column_name.clone()),
				column_data,
			));
		}

		let columns = Columns::new(columns);

		Ok(Some(columns))
	}
}
