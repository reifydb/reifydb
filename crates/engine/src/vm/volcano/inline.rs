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
use reifydb_rql::expression::AliasExpression;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	value::{Value, r#type::Type},
};

use crate::{
	evaluate::{
		EvalContext,
		column::{cast::cast_column_data, evaluate},
	},
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
}

impl QueryNode for InlineDataNode {
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> crate::Result<()> {
		// Already has context from constructor
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
				Value::Undefined => {
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
			let mut first_value_type = Type::Undefined;

			for row_data in &rows_data {
				if let Some(alias_expr) = row_data.get(&column_name) {
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
						if first_value_type == Type::Undefined
							&& value.get_type() != Type::Undefined
						{
							first_value_type = value.get_type();
						}
						all_values.push(value);
					} else {
						all_values.push(Value::Undefined);
					}
				} else {
					all_values.push(Value::Undefined);
				}
			}

			// Determine the initial wide type based on what we saw
			let wide_type = if first_value_type.is_integer() {
				Type::Int16 // Start with widest integer type
			} else if first_value_type.is_floating_point() {
				Type::Float8 // Start with widest float type
			} else if first_value_type == Type::Utf8 {
				Type::Utf8
			} else if first_value_type == Type::Boolean {
				Type::Boolean
			} else {
				Type::Undefined
			};

			// Create the wide column and add all values
			let mut column_data = if wide_type == Type::Undefined {
				ColumnData::undefined(all_values.len())
			} else {
				let mut data = ColumnData::with_capacity(wide_type, 0);

				// Add each value, casting to the wide
				// type if needed
				for value in &all_values {
					if value.get_type() == Type::Undefined {
						data.push_undefined();
					} else if value.get_type() == wide_type {
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
						};

						match cast_column_data(&ctx, &temp_data, wide_type, || Fragment::none())
						{
							Ok(casted) => {
								if let Some(casted_value) = casted.iter().next() {
									data.push_value(casted_value);
								} else {
									data.push_undefined();
								}
							}
							Err(_) => {
								data.push_undefined();
							}
						}
					}
				}

				data
			};

			// Now optimize: find the narrowest type and demote if
			// possible
			if wide_type == Type::Int16 {
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
				name: Fragment::internal(column_name),
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

			let mut column_data = ColumnData::undefined_typed(table_column.constraint.get_type(), 0);

			for row_data in &rows_data {
				if let Some(alias_expr) = row_data.get(column_name.text()) {
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
						column_data.push_value(Value::Undefined);
					} else {
						// This shouldn't happen for
						// single-encoded evaluation
						// but if it does, take only the
						// first value
						let first_value =
							evaluated.data().iter().next().unwrap_or(Value::Undefined);
						column_data.push_value(first_value);
					}
				} else {
					column_data.push_value(Value::Undefined);
				}
			}

			columns.push(Column {
				name: column_name.clone(),
				data: column_data,
			});
		}

		let columns = Columns::new(columns);

		Ok(Some(columns))
	}
}
