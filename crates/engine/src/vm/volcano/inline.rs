// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{BTreeSet, HashMap},
	sync::Arc,
};

use reifydb_core::{
	interface::{evaluate::TargetColumn, resolved::ResolvedPrimitive},
	value::column::{Column, columns::Columns, data::ColumnData, headers::ColumnHeaders},
};
use reifydb_rql::expression::{AliasExpression, ConstantExpression, Expression, IdentExpression};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	value::{Value, constraint::Constraint, r#type::Type},
};

use crate::{
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
		// Clone the Arc to extract headers without borrowing issues
		let cloned_context = context.clone();
		let headers =
			cloned_context.source.as_ref().map(|source| Self::create_columns_layout_from_source(source));

		Self {
			rows,
			headers,
			context: Some(context),
			executed: false,
		}
	}

	fn create_columns_layout_from_source(source: &ResolvedPrimitive) -> ColumnHeaders {
		ColumnHeaders {
			columns: source.columns().iter().map(|col| Fragment::internal(&col.name)).collect(),
		}
	}

	fn expand_sumtype_constructors<'a>(&mut self, txn: &mut Transaction<'a>) -> crate::Result<()> {
		let ctx = match self.context.as_ref() {
			Some(ctx) => ctx.clone(),
			None => return Ok(()),
		};

		let mut needs_expansion = false;
		for row in &self.rows {
			for alias_expr in row {
				if matches!(
					alias_expr.expression.as_ref(),
					Expression::SumTypeConstructor(_) | Expression::Column(_)
				) {
					needs_expansion = true;
					break;
				}
			}
			if needs_expansion {
				break;
			}
		}
		if !needs_expansion {
			return Ok(());
		}

		for row in &mut self.rows {
			let original = std::mem::take(row);
			let mut expanded = Vec::with_capacity(original.len());

			for alias_expr in original {
				match alias_expr.expression.as_ref() {
					Expression::SumTypeConstructor(ctor) => {
						let col_name = alias_expr.alias.0.text().to_string();
						let fragment = alias_expr.fragment.clone();

						let is_unresolved = ctor.namespace.text() == ctor.variant_name.text()
							&& ctor.sumtype_name.text() == ctor.variant_name.text();

						let Expression::SumTypeConstructor(ctor) = *alias_expr.expression
						else {
							unreachable!()
						};

						let sumtype_def = if is_unresolved {
							// Resolve from column constraint in table schema
							let tag_col_name = format!("{}_tag", col_name);
							let source = ctx
								.source
								.as_ref()
								.expect("source required for unresolved sumtype");
							let tag_col = source
								.columns()
								.iter()
								.find(|c| c.name == tag_col_name)
								.expect("tag column not found");
							let Some(Constraint::SumType(id)) =
								tag_col.constraint.constraint()
							else {
								panic!("expected SumType constraint on tag column")
							};
							ctx.services.catalog.get_sumtype(txn, *id)?
						} else {
							// Resolve from fully-qualified namespace
							let ns_name = ctor.namespace.text();
							let ns = ctx
								.services
								.catalog
								.find_namespace_by_name(txn, ns_name)?
								.unwrap();
							let sumtype_name = ctor.sumtype_name.text();
							ctx.services
								.catalog
								.find_sumtype_by_name(txn, ns.id, sumtype_name)?
								.unwrap()
						};

						let variant_name_lower = ctor.variant_name.text().to_lowercase();
						let variant = sumtype_def
							.variants
							.iter()
							.find(|v| v.name == variant_name_lower)
							.unwrap();

						expanded.push(AliasExpression {
							alias: IdentExpression(Fragment::internal(format!(
								"{}_tag",
								col_name
							))),
							expression: Box::new(Expression::Constant(
								ConstantExpression::Number {
									fragment: Fragment::internal(
										variant.tag.to_string(),
									),
								},
							)),
							fragment: fragment.clone(),
						});

						for (field_name, field_expr) in ctor.columns {
							let phys_col_name = format!(
								"{}_{}_{}",
								col_name,
								variant_name_lower,
								field_name.text().to_lowercase()
							);
							expanded.push(AliasExpression {
								alias: IdentExpression(Fragment::internal(
									phys_col_name,
								)),
								expression: Box::new(field_expr),
								fragment: fragment.clone(),
							});
						}
					}
					Expression::Column(col) => {
						// Check if this bare identifier is a unit variant for a SumType column
						let col_name = alias_expr.alias.0.text().to_string();
						let tag_col_name = format!("{}_tag", col_name);

						let resolved = if let Some(source) = ctx.source.as_ref() {
							if let Some(tag_col) =
								source.columns().iter().find(|c| c.name == tag_col_name)
							{
								if let Some(Constraint::SumType(id)) =
									tag_col.constraint.constraint()
								{
									let sumtype_def = ctx
										.services
										.catalog
										.get_sumtype(txn, *id)?;
									let variant_name_lower =
										col.0.name.text().to_lowercase();
									let maybe_tag = sumtype_def
										.variants
										.iter()
										.find(|v| {
											v.name.to_lowercase()
												== variant_name_lower
										})
										.map(|v| v.tag);
									if let Some(tag) = maybe_tag {
										Some((sumtype_def, tag))
									} else {
										None
									}
								} else {
									None
								}
							} else {
								None
							}
						} else {
							None
						};

						if let Some((sumtype_def, tag)) = resolved {
							let fragment = alias_expr.fragment.clone();
							// Expand unit variant: tag column
							expanded.push(AliasExpression {
								alias: IdentExpression(Fragment::internal(format!(
									"{}_tag",
									col_name
								))),
								expression: Box::new(Expression::Constant(
									ConstantExpression::Number {
										fragment: Fragment::internal(
											tag.to_string(),
										),
									},
								)),
								fragment: fragment.clone(),
							});
							// None for all variant fields (INSERT fills missing columns
							// with None)
							for v in &sumtype_def.variants {
								for field in &v.fields {
									let phys_col_name = format!(
										"{}_{}_{}",
										col_name,
										v.name.to_lowercase(),
										field.name.to_lowercase()
									);
									expanded.push(AliasExpression {
										alias: IdentExpression(Fragment::internal(
											phys_col_name,
										)),
										expression: Box::new(Expression::Constant(
											ConstantExpression::None {
												fragment: fragment.clone(),
											},
										)),
										fragment: fragment.clone(),
									});
								}
							}
						} else {
							expanded.push(alias_expr);
						}
					}
					_ => {
						expanded.push(alias_expr);
					}
				}
			}

			*row = expanded;
		}

		Ok(())
	}
}

impl QueryNode for InlineDataNode {
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &QueryContext) -> crate::Result<()> {
		self.expand_sumtype_constructors(rx)?;
		Ok(())
	}

	fn next<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
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

		// Choose execution path based on whether we have table
		// namespace
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

impl<'a> InlineDataNode {
	/// Determines the optimal (narrowest) integer type that can hold all
	/// values
	fn find_optimal_integer_type(column: &ColumnData) -> Type {
		let mut min_val = i128::MAX;
		let mut max_val = i128::MIN;
		let mut has_values = false;

		for value in column.iter() {
			match value {
				Value::Int16(v) => {
					has_values = true;
					min_val = min_val.min(v as i128);
					max_val = max_val.max(v as i128);
				}
				Value::None {
					..
				} => {
					// Skip undefined values
				}
				_ => {
					// Non-integer value, keep as Int16
					return Type::Int16;
				}
			}
		}

		if !has_values {
			return Type::Int1; // Default to smallest if no values
		}

		// Determine narrowest type that can hold the range
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

	fn next_infer_namespace(&mut self, ctx: &QueryContext) -> crate::Result<Option<Columns>> {
		// Collect all unique column names across all rows
		let mut all_columns: BTreeSet<String> = BTreeSet::new();

		for row in &self.rows {
			for keyed_expr in row {
				let column_name = keyed_expr.alias.0.text().to_string();
				all_columns.insert(column_name);
			}
		}

		// Convert each encoded to a HashMap for easier lookup
		let mut rows_data: Vec<HashMap<String, &AliasExpression>> = Vec::new();

		for row in &self.rows {
			let mut row_map: HashMap<String, &AliasExpression> = HashMap::new();
			for alias_expr in row {
				let column_name = alias_expr.alias.0.text().to_string();
				row_map.insert(column_name, alias_expr);
			}
			rows_data.push(row_map);
		}

		// Create columns - start with wide types
		let mut columns = Vec::new();

		for column_name in all_columns {
			// First pass: collect all values in a wide column
			let mut all_values = Vec::new();
			let mut first_value_type: Option<Type> = None;
			let mut column_fragment: Option<Fragment> = None;

			for row_data in &rows_data {
				if let Some(alias_expr) = row_data.get(&column_name) {
					if column_fragment.is_none() {
						column_fragment = Some(alias_expr.fragment.clone());
					}
					let ctx = EvalContext {
						target: None,
						columns: Columns::empty(),
						row_count: 1,
						take: None,
						params: &ctx.params,
						symbol_table: &ctx.stack,
						is_aggregate_context: false,
						functions: &self.context.as_ref().unwrap().services.functions,
						clock: &self.context.as_ref().unwrap().services.clock,
						arena: None,
					};

					let evaluated = evaluate(
						&ctx,
						&alias_expr.expression,
						&self.context.as_ref().unwrap().services.functions,
						&self.context.as_ref().unwrap().services.clock,
					)?;

					// Take the first value from the
					// evaluated result
					let mut iter = evaluated.data().iter();
					if let Some(value) = iter.next() {
						// Track the first non-undefined
						// value type we see
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

			// Determine the initial wide type based on what we saw
			let wide_type = if let Some(ref fvt) = first_value_type {
				if fvt.is_integer() {
					Some(Type::Int16) // Start with widest integer type
				} else if fvt.is_floating_point() {
					Some(Type::Float8) // Start with widest float type
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

			// Create the wide column and add all values
			let mut column_data = if wide_type.is_none() {
				ColumnData::none_typed(Type::Boolean, all_values.len())
			} else {
				let mut data = ColumnData::with_capacity(wide_type.clone().unwrap(), 0);

				// Add each value, casting to the wide
				// type if needed
				for value in &all_values {
					if matches!(value, Value::None { .. }) {
						data.push_none();
					} else if wide_type.as_ref().map_or(false, |wt| value.get_type() == *wt) {
						data.push_value(value.clone());
					} else {
						// Cast to the wide type
						let temp_data = ColumnData::from(value.clone());
						let ctx = EvalContext {
							target: None,
							columns: Columns::empty(),
							row_count: 1,
							take: None,
							params: &ctx.params,
							symbol_table: &ctx.stack,
							is_aggregate_context: false,
							functions: &self.context.as_ref().unwrap().services.functions,
							clock: &self.context.as_ref().unwrap().services.clock,
							arena: None,
						};

						match cast_column_data(
							&ctx,
							&temp_data,
							wide_type.clone().unwrap(),
							|| Fragment::none(),
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

			// Now optimize: find the narrowest type and demote if
			// possible
			if wide_type == Some(Type::Int16) {
				let optimal_type = Self::find_optimal_integer_type(&column_data);
				if optimal_type != Type::Int16 {
					// Demote to the optimal type
					let ctx = EvalContext {
						target: None,
						columns: Columns::empty(),
						row_count: column_data.len(),
						take: None,
						params: &ctx.params,
						symbol_table: &ctx.stack,
						is_aggregate_context: false,
						functions: &self.context.as_ref().unwrap().services.functions,
						clock: &self.context.as_ref().unwrap().services.clock,
						arena: None,
					};

					if let Ok(demoted) =
						cast_column_data(&ctx, &column_data, optimal_type, || Fragment::none())
					{
						column_data = demoted;
					}
				}
			}
			// Could add similar optimization for Float8 -> Float4
			// if needed

			columns.push(Column {
				name: column_fragment.unwrap_or_else(|| Fragment::internal(column_name)),
				data: column_data,
			});
		}

		let columns = Columns::new(columns);
		self.headers = Some(ColumnHeaders::from_columns(&columns));

		Ok(Some(columns))
	}

	fn next_with_source(&mut self, ctx: &QueryContext) -> crate::Result<Option<Columns>> {
		let source = ctx.source.as_ref().unwrap(); // Safe because headers is Some
		let headers = self.headers.as_ref().unwrap(); // Safe because we're in this path

		// Convert rows to HashMap for easier column lookup
		let mut rows_data: Vec<HashMap<String, &AliasExpression>> = Vec::new();

		for row in &self.rows {
			let mut row_map: HashMap<String, &AliasExpression> = HashMap::new();
			for alias_expr in row {
				let column_name = alias_expr.alias.0.text().to_string();
				row_map.insert(column_name, alias_expr);
			}
			rows_data.push(row_map);
		}

		// Create columns based on table namespace
		let mut columns = Vec::new();

		for column_name in &headers.columns {
			// Find the corresponding source column for policies
			let table_column = source.columns().iter().find(|col| col.name == column_name.text()).unwrap();

			let mut column_data = ColumnData::none_typed(table_column.constraint.get_type(), 0);
			let mut column_fragment: Option<Fragment> = None;

			for row_data in &rows_data {
				if let Some(alias_expr) = row_data.get(column_name.text()) {
					if column_fragment.is_none() {
						column_fragment = Some(alias_expr.fragment.clone());
					}
					let ctx = EvalContext {
						target: Some(TargetColumn::Partial {
							source_name: Some(source.identifier().text().to_string()),
							column_name: Some(table_column.name.clone()),
							column_type: table_column.constraint.get_type(),
							policies: table_column
								.policies
								.iter()
								.map(|cp| cp.policy.clone())
								.collect(),
						}),
						columns: Columns::empty(),
						row_count: 1,
						take: None,
						params: &ctx.params,
						symbol_table: &ctx.stack,
						is_aggregate_context: false,
						functions: &self.context.as_ref().unwrap().services.functions,
						clock: &self.context.as_ref().unwrap().services.clock,
						arena: None,
					};

					let evaluated = evaluate(
						&ctx,
						&alias_expr.expression,
						&self.context.as_ref().unwrap().services.functions,
						&self.context.as_ref().unwrap().services.clock,
					)?;

					// Ensure we always add exactly one
					// value
					let eval_len = evaluated.data().len();
					if eval_len == 1 {
						column_data.extend(evaluated.data().clone())?;
					} else if eval_len == 0 {
						// If evaluation returned empty,
						// push undefined
						column_data.push_value(Value::none());
					} else {
						// This shouldn't happen for
						// single-encoded evaluation
						// but if it does, take only the
						// first value
						let first_value =
							evaluated.data().iter().next().unwrap_or(Value::none());
						column_data.push_value(first_value);
					}
				} else {
					column_data.push_value(Value::none());
				}
			}

			columns.push(Column {
				name: column_fragment
					.map(|f| f.with_text(column_name.text()))
					.unwrap_or_else(|| column_name.clone()),
				data: column_data,
			});
		}

		let columns = Columns::new(columns);

		Ok(Some(columns))
	}
}
