// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::cmp::min;

use reifydb_core::value::column::{Column, ColumnData};
use reifydb_rql::expression::ColumnExpression;
use reifydb_type::{
	Date, DateTime, Decimal, Duration, ROW_NUMBER_COLUMN_NAME, Time, Type, Uint, Value,
	value::{Blob, IdentityId, Uuid4, Uuid7},
};

use crate::{StandardColumnEvaluator, evaluate::ColumnEvaluationContext};

impl StandardColumnEvaluator {
	pub(crate) fn column(&self, ctx: &ColumnEvaluationContext, column: &ColumnExpression) -> crate::Result<Column> {
		let name = column.0.name.text();

		// Check for rownum pseudo-column first
		if name == ROW_NUMBER_COLUMN_NAME && !ctx.columns.row_numbers.is_empty() {
			let row_numbers: Vec<u64> = ctx.columns.row_numbers.iter().map(|r| r.value()).collect();
			return Ok(Column::new(ROW_NUMBER_COLUMN_NAME.to_string(), ColumnData::uint8(row_numbers)));
		}

		if let Some(col) = ctx.columns.iter().find(|c| c.name() == name) {
			return self.extract_column_data(col, ctx);
		}

		Ok(Column::new(name.to_string(), ColumnData::undefined(ctx.row_count)))
	}

	fn extract_column_data<'a>(&self, col: &Column, ctx: &ColumnEvaluationContext) -> crate::Result<Column> {
		let take = ctx.take.unwrap_or(usize::MAX);

		// Use the column's actual data type instead of checking the first value
		// This handles cases where the first value is Undefined
		let col_type = col.data().get_type();

		match col_type {
			Type::Boolean => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Boolean(b) => {
							data.push(b);
							bitvec.push(true);
						}
						_ => {
							data.push(false);
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::bool_with_bitvec(data, bitvec)))
			}
			Type::Float4 => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Float4(v) => {
							data.push(v.value());
							bitvec.push(true);
						}
						_ => {
							data.push(0.0f32);
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::float4_with_bitvec(data, bitvec)))
			}

			Type::Float8 => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Float8(v) => {
							data.push(v.value());
							bitvec.push(true);
						}
						_ => {
							data.push(0.0f64);
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::float8_with_bitvec(data, bitvec)))
			}

			Type::Int1 => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Int1(n) => {
							data.push(n);
							bitvec.push(true);
						}
						_ => {
							data.push(0);
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::int1_with_bitvec(data, bitvec)))
			}

			Type::Int2 => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Int2(n) => {
							data.push(n);
							bitvec.push(true);
						}
						_ => {
							data.push(0);
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::int2_with_bitvec(data, bitvec)))
			}

			Type::Int4 => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Int4(n) => {
							data.push(n);
							bitvec.push(true);
						}
						_ => {
							data.push(0);
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::int4_with_bitvec(data, bitvec)))
			}

			Type::Int8 => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Int8(n) => {
							data.push(n);
							bitvec.push(true);
						}
						_ => {
							data.push(0);
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::int8_with_bitvec(data, bitvec)))
			}

			Type::Int16 => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Int16(n) => {
							data.push(n);
							bitvec.push(true);
						}
						_ => {
							data.push(0);
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::int16_with_bitvec(data, bitvec)))
			}

			Type::Utf8 => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Utf8(s) => {
							data.push(s.clone());
							bitvec.push(true);
						}
						_ => {
							data.push("".to_string());
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::utf8_with_bitvec(data, bitvec)))
			}

			Type::Uint1 => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Uint1(n) => {
							data.push(n);
							bitvec.push(true);
						}
						_ => {
							data.push(0);
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::uint1_with_bitvec(data, bitvec)))
			}

			Type::Uint2 => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Uint2(n) => {
							data.push(n);
							bitvec.push(true);
						}
						_ => {
							data.push(0);
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::uint2_with_bitvec(data, bitvec)))
			}

			Type::Uint4 => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Uint4(n) => {
							data.push(n);
							bitvec.push(true);
						}
						_ => {
							data.push(0);
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::uint4_with_bitvec(data, bitvec)))
			}

			Type::Uint8 => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Uint8(n) => {
							data.push(n);
							bitvec.push(true);
						}
						_ => {
							data.push(0);
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::uint8_with_bitvec(data, bitvec)))
			}

			Type::Uint16 => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Uint16(n) => {
							data.push(n);
							bitvec.push(true);
						}
						_ => {
							data.push(0);
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::uint16_with_bitvec(data, bitvec)))
			}

			Type::Date => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Date(d) => {
							data.push(d.clone());
							bitvec.push(true);
						}
						_ => {
							data.push(Date::default());
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::date_with_bitvec(data, bitvec)))
			}

			Type::DateTime => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::DateTime(dt) => {
							data.push(dt.clone());
							bitvec.push(true);
						}
						_ => {
							data.push(DateTime::default());
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::datetime_with_bitvec(data, bitvec)))
			}

			Type::Time => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Time(t) => {
							data.push(t.clone());
							bitvec.push(true);
						}
						_ => {
							data.push(Time::default());
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::time_with_bitvec(data, bitvec)))
			}

			Type::Duration => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Duration(i) => {
							data.push(i.clone());
							bitvec.push(true);
						}
						_ => {
							data.push(Duration::default());
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::duration_with_bitvec(data, bitvec)))
			}
			Type::IdentityId => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::IdentityId(i) => {
							data.push(i.clone());
							bitvec.push(true);
						}
						_ => {
							data.push(IdentityId::default());
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::identity_id_with_bitvec(data, bitvec)))
			}
			Type::Uuid4 => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Uuid4(i) => {
							data.push(i.clone());
							bitvec.push(true);
						}
						_ => {
							data.push(Uuid4::default());
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::uuid4_with_bitvec(data, bitvec)))
			}
			Type::Uuid7 => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Uuid7(i) => {
							data.push(i.clone());
							bitvec.push(true);
						}
						_ => {
							data.push(Uuid7::default());
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::uuid7_with_bitvec(data, bitvec)))
			}
			Type::Blob => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Blob(b) => {
							data.push(b.clone());
							bitvec.push(true);
						}
						_ => {
							data.push(Blob::new(vec![]));
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::blob_with_bitvec(data, bitvec)))
			}
			Type::Int => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Int(b) => {
							data.push(b.clone());
							bitvec.push(true);
						}
						_ => {
							data.push(reifydb_type::Int::zero());
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::int_with_bitvec(data, bitvec)))
			}
			Type::Uint => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Uint(b) => {
							data.push(b.clone());
							bitvec.push(true);
						}
						_ => {
							data.push(Uint::zero());
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::uint_with_bitvec(data, bitvec)))
			}
			Type::Any => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Any(boxed) => {
							data.push(Box::new(*boxed.clone()));
							bitvec.push(true);
						}
						_ => {
							data.push(Box::new(Value::Undefined));
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::any_with_bitvec(data, bitvec)))
			}
			Type::Decimal => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Decimal(b) => {
							data.push(b.clone());
							bitvec.push(true);
						}
						_ => {
							data.push(Decimal::from_i64(0));
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::decimal_with_bitvec(data, bitvec)))
			}
			Type::Undefined => {
				let count = min(ctx.row_count, take);
				Ok(col.with_new_data(ColumnData::undefined(count)))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		interface::identifier::{ColumnIdentifier, ColumnSource},
		value::column::{Column, ColumnData, Columns},
	};
	use reifydb_rql::expression::ColumnExpression;
	use reifydb_type::{Fragment, Params};

	use crate::{
		evaluate::{ColumnEvaluationContext, column::StandardColumnEvaluator},
		stack::Stack,
	};

	#[tokio::test]
	async fn test_column_not_found_returns_correct_row_count() {
		// Create context with 5 rows
		let columns =
			Columns::new(vec![Column::new("existing_col".to_string(), ColumnData::int4([1, 2, 3, 4, 5]))]);

		let ctx = ColumnEvaluationContext {
			target: None,
			columns,
			row_count: 5,
			take: None,
			params: &Params::None,
			stack: &Stack::new(),
			is_aggregate_context: false,
		};

		let evaluator = StandardColumnEvaluator::default();

		// Try to access a column that doesn't exist
		let result = evaluator
			.column(
				&ctx,
				&ColumnExpression(ColumnIdentifier {
					source: ColumnSource::Alias(Fragment::internal("nonexistent_col")),
					name: Fragment::internal("nonexistent_col"),
				}),
			)
			.unwrap();

		// The column should have 5 rows (matching row_count), not 0
		assert_eq!(
			result.data().len(),
			5,
			"Column not found should return column with ctx.row_count rows, not 0"
		);
	}
}
