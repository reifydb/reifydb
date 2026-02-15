// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::cmp::min;

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_rql::expression::ColumnExpression;
use reifydb_type::value::{
	Value,
	blob::Blob,
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	dictionary::DictionaryEntryId,
	duration::Duration,
	identity::IdentityId,
	int::Int,
	row_number::ROW_NUMBER_COLUMN_NAME,
	time::Time,
	r#type::Type,
	uint::Uint,
	uuid::{Uuid4, Uuid7},
};

use crate::expression::context::EvalContext;

macro_rules! extract_typed_column {
	($col:expr, $take:expr, $variant:ident($x:ident) => $transform:expr, $default:expr, $constructor:ident) => {{
		let mut data = Vec::new();
		let mut bitvec = Vec::new();
		let mut count = 0;
		for v in $col.data().iter() {
			if count >= $take {
				break;
			}
			match v {
				Value::$variant($x) => {
					data.push($transform);
					bitvec.push(true);
				}
				_ => {
					data.push($default);
					bitvec.push(false);
				}
			}
			count += 1;
		}
		Ok($col.with_new_data(ColumnData::$constructor(data, bitvec)))
	}};
}

pub(crate) fn column_lookup(ctx: &EvalContext, column: &ColumnExpression) -> crate::Result<Column> {
	let name = column.0.name.text();

	// Check for rownum pseudo-column first
	if name == ROW_NUMBER_COLUMN_NAME && !ctx.columns.row_numbers.is_empty() {
		let row_numbers: Vec<u64> = ctx.columns.row_numbers.iter().map(|r| r.value()).collect();
		return Ok(Column::new(ROW_NUMBER_COLUMN_NAME.to_string(), ColumnData::uint8(row_numbers)));
	}

	if let Some(col) = ctx.columns.iter().find(|c| c.name() == name) {
		return extract_column_data(col, ctx);
	}

	Ok(Column::new(name.to_string(), ColumnData::undefined(ctx.row_count)))
}

fn extract_column_data(col: &Column, ctx: &EvalContext) -> crate::Result<Column> {
	let take = ctx.take.unwrap_or(usize::MAX);

	// Use the column's actual data type instead of checking the first value
	// This handles cases where the first value is Undefined
	let col_type = col.data().get_type();

	match col_type {
		Type::Boolean => extract_typed_column!(col, take, Boolean(b) => b, false, bool_with_bitvec),
		Type::Float4 => extract_typed_column!(col, take, Float4(v) => v.value(), 0.0f32, float4_with_bitvec),
		Type::Float8 => extract_typed_column!(col, take, Float8(v) => v.value(), 0.0f64, float8_with_bitvec),
		Type::Int1 => extract_typed_column!(col, take, Int1(n) => n, 0, int1_with_bitvec),
		Type::Int2 => extract_typed_column!(col, take, Int2(n) => n, 0, int2_with_bitvec),
		Type::Int4 => extract_typed_column!(col, take, Int4(n) => n, 0, int4_with_bitvec),
		Type::Int8 => extract_typed_column!(col, take, Int8(n) => n, 0, int8_with_bitvec),
		Type::Int16 => extract_typed_column!(col, take, Int16(n) => n, 0, int16_with_bitvec),
		Type::Utf8 => extract_typed_column!(col, take, Utf8(s) => s.clone(), "".to_string(), utf8_with_bitvec),
		Type::Uint1 => extract_typed_column!(col, take, Uint1(n) => n, 0, uint1_with_bitvec),
		Type::Uint2 => extract_typed_column!(col, take, Uint2(n) => n, 0, uint2_with_bitvec),
		Type::Uint4 => extract_typed_column!(col, take, Uint4(n) => n, 0, uint4_with_bitvec),
		Type::Uint8 => extract_typed_column!(col, take, Uint8(n) => n, 0, uint8_with_bitvec),
		Type::Uint16 => extract_typed_column!(col, take, Uint16(n) => n, 0, uint16_with_bitvec),
		Type::Date => extract_typed_column!(col, take, Date(d) => d.clone(), Date::default(), date_with_bitvec),
		Type::DateTime => {
			extract_typed_column!(col, take, DateTime(dt) => dt.clone(), DateTime::default(), datetime_with_bitvec)
		}
		Type::Time => extract_typed_column!(col, take, Time(t) => t.clone(), Time::default(), time_with_bitvec),
		Type::Duration => {
			extract_typed_column!(col, take, Duration(i) => i.clone(), Duration::default(), duration_with_bitvec)
		}
		Type::IdentityId => {
			extract_typed_column!(col, take, IdentityId(i) => i.clone(), IdentityId::default(), identity_id_with_bitvec)
		}
		Type::Uuid4 => {
			extract_typed_column!(col, take, Uuid4(i) => i.clone(), Uuid4::default(), uuid4_with_bitvec)
		}
		Type::Uuid7 => {
			extract_typed_column!(col, take, Uuid7(i) => i.clone(), Uuid7::default(), uuid7_with_bitvec)
		}
		Type::DictionaryId => {
			extract_typed_column!(col, take, DictionaryId(i) => i.clone(), DictionaryEntryId::default(), dictionary_id_with_bitvec)
		}
		Type::Blob => {
			extract_typed_column!(col, take, Blob(b) => b.clone(), Blob::new(vec![]), blob_with_bitvec)
		}
		Type::Int => extract_typed_column!(col, take, Int(b) => b.clone(), Int::zero(), int_with_bitvec),
		Type::Uint => extract_typed_column!(col, take, Uint(b) => b.clone(), Uint::zero(), uint_with_bitvec),
		Type::Any => {
			extract_typed_column!(col, take, Any(boxed) => Box::new(*boxed.clone()), Box::new(Value::None), any_with_bitvec)
		}
		Type::Decimal => {
			extract_typed_column!(col, take, Decimal(b) => b.clone(), Decimal::from_i64(0), decimal_with_bitvec)
		}
		Type::Option(_) => {
			let count = min(ctx.row_count, take);
			Ok(col.with_new_data(ColumnData::undefined(count)))
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::identifier::{ColumnIdentifier, ColumnPrimitive},
		value::column::{Column, columns::Columns, data::ColumnData},
	};
	use reifydb_function::registry::Functions;
	use reifydb_rql::expression::ColumnExpression;
	use reifydb_runtime::clock::Clock;
	use reifydb_type::{fragment::Fragment, params::Params};

	use crate::{expression::context::EvalContext, vm::stack::SymbolTable};

	#[test]
	fn test_column_not_found_returns_correct_row_count() {
		// Create context with 5 rows
		let columns =
			Columns::new(vec![Column::new("existing_col".to_string(), ColumnData::int4([1, 2, 3, 4, 5]))]);

		let ctx = EvalContext {
			target: None,
			columns,
			row_count: 5,
			take: None,
			params: &Params::None,
			symbol_table: &SymbolTable::new(),
			is_aggregate_context: false,
			functions: &Functions::empty(),
			clock: &Clock::default(),
			arena: None,
		};

		// Try to access a column that doesn't exist
		let result = super::column_lookup(
			&ctx,
			&ColumnExpression(ColumnIdentifier {
				primitive: ColumnPrimitive::Alias(Fragment::internal("nonexistent_col")),
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
