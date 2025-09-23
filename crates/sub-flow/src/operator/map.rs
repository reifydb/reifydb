// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::{FlowChange, FlowDiff, FlowNodeDef},
	interface::{RowEvaluationContext, RowEvaluator, Transaction, expression::Expression},
	value::row::{EncodedRowNamedLayout, Row},
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};
use reifydb_type::{Params, Type, Value};

use crate::Operator;

// Static empty params instance for use in RowEvaluationContext
static EMPTY_PARAMS: Params = Params::None;

pub struct MapOperator {
	expressions: Vec<Expression<'static>>,
	output_schema: Option<FlowNodeDef>,
}

impl MapOperator {
	pub fn new(expressions: Vec<Expression<'static>>, output_schema: Option<FlowNodeDef>) -> Self {
		Self {
			expressions,
			output_schema,
		}
	}
}

impl<T: Transaction> Operator<T> for MapOperator {
	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		let mut result = Vec::new();

		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					source,
					post,
				} => {
					result.push(FlowDiff::Insert {
						source,
						post: self.project_row(&post, evaluator)?,
					});
				}
				FlowDiff::Update {
					source,
					pre,
					post,
				} => {
					result.push(FlowDiff::Update {
						source,
						pre,
						post: self.project_row(&post, evaluator)?,
					});
				}
				FlowDiff::Remove {
					source,
					pre,
				} => {
					// pass through
					result.push(FlowDiff::Remove {
						source,
						pre,
					});
				}
			}
		}

		Ok(FlowChange::new(result))
	}
}

impl MapOperator {
	fn project_row(&self, row: &Row, evaluator: &StandardRowEvaluator) -> crate::Result<Row> {
		// Create evaluation context
		let ctx = RowEvaluationContext {
			row: row.clone(),
			target: None,
			params: &EMPTY_PARAMS,
		};

		// Evaluate all expressions
		let mut values = Vec::with_capacity(self.expressions.len());
		let mut field_names = Vec::with_capacity(self.expressions.len());
		let mut field_types = Vec::with_capacity(self.expressions.len());

		for (i, expr) in self.expressions.iter().enumerate() {
			// Evaluate the expression
			let value = evaluator.evaluate(&ctx, expr)?;

			let (field_name, field_type) = if let Some(ref schema) = self.output_schema {
				// Terminal node: use schema from output_schema
				if let Some(column) = schema.columns.get(i) {
					let value_type = value.get_type();
					let schema_type = column.constraint.get_type();

					// Coerce value to match schema type if needed
					let final_value = if value_type != schema_type && value_type != Type::Undefined
					{
						self.coerce_type(value, schema_type)?
					} else {
						value
					};

					values.push(final_value);
					(column.name.clone(), schema_type)
				} else {
					panic!("MapOperator: output schema has fewer fields than expressions");
				}
			} else {
				// Intermediate node: derive field names from expressions
				values.push(value.clone());

				// Get field name from expression
				let field_name = match expr {
					Expression::Alias(alias_expr) => alias_expr.alias.name().to_string(),
					Expression::Column(col_expr) => col_expr.0.name.text().to_string(),
					Expression::AccessSource(access_expr) => {
						access_expr.column.name.text().to_string()
					}
					_ => expr.full_fragment_owned().text().to_string(),
				};

				// Get the field type from the evaluated value
				let field_type = value.get_type();

				(field_name, field_type)
			};

			field_names.push(field_name);
			field_types.push(field_type);
		}

		// Create the new layout
		let fields: Vec<(String, Type)> = field_names.into_iter().zip(field_types.into_iter()).collect();
		let layout = EncodedRowNamedLayout::new(fields);

		// Allocate and populate the new row
		let mut encoded_row = layout.allocate_row();
		layout.set_values(&mut encoded_row, &values);

		Ok(Row {
			number: row.number,
			encoded: encoded_row,
			layout,
		})
	}

	// FIXME this should not be necessary -> Pass the target_type information into TargetColumn evaluation
	// so that existing coercion implemenmtation kicks in instead
	fn coerce_type(&self, value: Value, target_type: Type) -> crate::Result<Value> {
		// Coerce value to target type
		match (value, target_type) {
			// Same type - no coercion needed
			(v, t) if v.get_type() == t => Ok(v),

			// Int1 to Int4 coercion (common for constant literals)
			(Value::Int1(v), Type::Int4) => Ok(Value::Int4(v as i32)),

			// Int2 to Int4 coercion
			(Value::Int2(v), Type::Int4) => Ok(Value::Int4(v as i32)),

			// Int8 to Int4 coercion (common for arithmetic results)
			(Value::Int8(v), Type::Int4) => {
				// Check if value fits in Int4
				if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
					Ok(Value::Int4(v as i32))
				} else {
					// Value too large for Int4, keep as Int8
					// This will cause type mismatch later but at least won't lose data
					Ok(Value::Int8(v))
				}
			}

			// Int16 to Int4 coercion (can happen with complex nested expressions)
			(Value::Int16(v), Type::Int4) => {
				// Int16 is i128, need to check if it fits in Int4 (i32)
				if v >= i32::MIN as i128 && v <= i32::MAX as i128 {
					Ok(Value::Int4(v as i32))
				} else {
					Ok(Value::Int16(v))
				}
			}

			// Int16 to Int8 coercion
			(Value::Int16(v), Type::Int8) => {
				if v >= i64::MIN as i128 && v <= i64::MAX as i128 {
					Ok(Value::Int8(v as i64))
				} else {
					Ok(Value::Int16(v))
				}
			}

			// Float8 to Int4 coercion (for type casting operations)
			(Value::Float8(v), Type::Int4) => {
				let float_val = *v;
				// Truncate float to integer (following standard SQL behavior)
				Ok(Value::Int4(float_val as i32))
			}

			// Float4 to Int4 coercion
			(Value::Float4(v), Type::Int4) => {
				let float_val = *v;
				Ok(Value::Int4(float_val as i32))
			}

			// Other coercions can be added here as needed

			// Default: return as-is
			(v, _) => Ok(v),
		}
	}
}
