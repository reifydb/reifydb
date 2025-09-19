// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::cmp::min;

use reifydb_core::{
	interface::{EvaluationContext, expression::ColumnExpression},
	value::columnar::{Column, ColumnData},
};
use reifydb_type::{
	Date, DateTime, Decimal, Interval, RowNumber, Time, Type, Uint, Value,
	diagnostic::query::column_not_found,
	error,
	value::{Blob, IdentityId, Uuid4, Uuid7},
};

use crate::StandardEvaluator;

impl StandardEvaluator {
	pub(crate) fn column(&self, ctx: &EvaluationContext, column: &ColumnExpression) -> crate::Result<Column> {
		// Get the column name from the ColumnIdentifier
		let name = column.0.name.text().to_string();

		// Check if the name contains dots (qualified reference)
		let parts: Vec<&str> = name.split('.').collect();

		match parts.len() {
			3 => {
				// Fully qualified: namespace.table.column
				let namespace = parts[0];
				let table = parts[1];
				let col_name = parts[2];

				// Find column matching all three parts
				let matching_col = ctx.columns.iter().find(|c| {
					c.name() == col_name
						&& match c {
							Column::FullyQualified(fq) => {
								fq.namespace == namespace && fq.source == table
							}
							_ => false,
						}
				});

				if let Some(col) = matching_col {
					return self.extract_column_data(col, ctx);
				}
			}
			2 => {
				// Source qualified: table.column
				let source = parts[0];
				let col_name = parts[1];

				// Find column matching source and name
				let matching_col = ctx.columns.iter().find(|c| {
					c.name() == col_name
						&& match c {
							Column::FullyQualified(fq) => {
								// Match if table name matches, or namespace.table
								// matches
								fq.source == source
									|| format!("{}.{}", fq.namespace, fq.source)
										== source
							}
							Column::SourceQualified(sq) => sq.source == source,
							_ => false,
						}
				});

				if let Some(col) = matching_col {
					return self.extract_column_data(col, ctx);
				}
			}
			1 => {
				// Unqualified column name - use existing logic
				// First try exact qualified name match
				if let Some(col) = ctx.columns.iter().find(|c| c.qualified_name() == name) {
					return self.extract_column_data(col, ctx);
				}

				// Then find all matches by unqualified name and
				// select the most qualified one
				let all_matches: Vec<_> = ctx.columns.iter().filter(|c| c.name() == name).collect();

				if !all_matches.is_empty() {
					// Always prefer the most qualified
					// column available
					let best_match = all_matches
						.iter()
						.enumerate()
						.max_by_key(|(idx, c)| {
							let qualification_level = match (c.namespace(), c.table()) {
								(Some(_), Some(_)) => 3, // Fully qualified
								(None, Some(_)) => 2,    // Table qualified
								(Some(_), None) => 1,    // Namespace qualified
								// (unusual)
								_ => 0, // Unqualified
							};
							// Use index as
							// secondary sort key to
							// prefer
							// later columns in case
							// of tie
							(qualification_level, *idx)
						})
						.map(|(_, c)| *c)
						.unwrap(); // Safe because we know the list is not empty

					return self.extract_column_data(best_match, ctx);
				}
			}
			_ => {
				// Invalid format with too many dots
				return Err(error!(column_not_found(column.0.name.clone())));
			}
		}

		// If we get here, column was not found
		Err(error!(column_not_found(column.0.name.clone())))
	}

	fn extract_column_data(&self, col: &Column, ctx: &EvaluationContext) -> crate::Result<Column> {
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

			Type::Interval => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::Interval(i) => {
							data.push(i.clone());
							bitvec.push(true);
						}
						_ => {
							data.push(Interval::default());
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::interval_with_bitvec(data, bitvec)))
			}
			Type::RowNumber => {
				let mut data = Vec::new();
				let mut bitvec = Vec::new();
				let mut count = 0;
				for v in col.data().iter() {
					if count >= take {
						break;
					}
					match v {
						Value::RowNumber(i) => {
							data.push(i.clone());
							bitvec.push(true);
						}
						_ => {
							data.push(RowNumber::default());
							bitvec.push(false);
						}
					}
					count += 1;
				}
				Ok(col.with_new_data(ColumnData::row_number_with_bitvec(data, bitvec)))
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
