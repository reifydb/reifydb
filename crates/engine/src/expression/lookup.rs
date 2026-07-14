// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::iter::repeat_n;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer};
use reifydb_rql::expression::ColumnExpression;
use reifydb_value::value::{
	Value,
	blob::Blob,
	date::Date,
	datetime::{CREATED_AT_COLUMN_NAME, DateTime, UPDATED_AT_COLUMN_NAME},
	decimal::Decimal,
	dictionary::DictionaryEntryId,
	duration::Duration,
	identity::IdentityId,
	int::Int,
	row_number::ROW_NUMBER_COLUMN_NAME,
	time::Time,
	uint::Uint,
	uuid::{Uuid4, Uuid7},
	value_type::ValueType,
	vector::VectorValue,
};

use crate::{Result, expression::context::EvalContext, vm::stack::Variable};

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
		Ok($col.with_new_data(ColumnBuffer::$constructor(data, bitvec)))
	}};
}

pub(crate) fn column_lookup(ctx: &EvalContext, column: &ColumnExpression) -> Result<ColumnWithName> {
	let name = column.0.name.text();

	if name == ROW_NUMBER_COLUMN_NAME && !ctx.columns.row_numbers.is_empty() {
		let row_numbers: Vec<u64> = ctx.columns.row_numbers.iter().map(|r| r.value()).collect();
		return Ok(ColumnWithName::new(ROW_NUMBER_COLUMN_NAME.to_string(), ColumnBuffer::uint8(row_numbers)));
	}

	if name == CREATED_AT_COLUMN_NAME && !ctx.columns.created_at.is_empty() {
		return Ok(ColumnWithName::new(
			CREATED_AT_COLUMN_NAME.to_string(),
			ColumnBuffer::datetime(ctx.columns.created_at.to_vec()),
		));
	}

	if name == UPDATED_AT_COLUMN_NAME && !ctx.columns.updated_at.is_empty() {
		return Ok(ColumnWithName::new(
			UPDATED_AT_COLUMN_NAME.to_string(),
			ColumnBuffer::datetime(ctx.columns.updated_at.to_vec()),
		));
	}

	if let Some(col) = ctx.columns.iter().find(|c| c.name() == name) {
		let owned = ColumnWithName::new(col.name().clone(), col.data().clone());
		return extract_column_data(&owned, ctx);
	}

	if let Some(Variable::Columns {
		columns: scalar_cols,
	}) = ctx.symbols.get(name)
		&& scalar_cols.is_scalar()
		&& let Some(col) = scalar_cols.columns.first()
	{
		let owned = ColumnWithName::new(scalar_cols.name_at(0).clone(), col.clone());
		return extract_column_data(&owned, ctx);
	}

	Ok(ColumnWithName::new(name.to_string(), ColumnBuffer::none_typed(ValueType::Boolean, ctx.row_count)))
}

fn extract_column_data(col: &ColumnWithName, ctx: &EvalContext) -> Result<ColumnWithName> {
	let take = ctx.take.unwrap_or(usize::MAX);

	if take >= col.data().len() {
		return Ok(col.clone());
	}

	let col_type = col.data().get_type();
	let effective_type = match col_type {
		ValueType::Option(inner) => *inner,
		other => other,
	};

	extract_column_data_by_type(col, take, effective_type)
}

fn extract_column_data_by_type(col: &ColumnWithName, take: usize, col_type: ValueType) -> Result<ColumnWithName> {
	match col_type {
		ValueType::Boolean => extract_typed_column!(col, take, Boolean(b) => b, false, bool_with_bitvec),
		ValueType::Float4 => {
			extract_typed_column!(col, take, Float4(v) => v.value(), 0.0f32, float4_with_bitvec)
		}
		ValueType::Float8 => {
			extract_typed_column!(col, take, Float8(v) => v.value(), 0.0f64, float8_with_bitvec)
		}
		ValueType::Int1 => extract_typed_column!(col, take, Int1(n) => n, 0, int1_with_bitvec),
		ValueType::Int2 => extract_typed_column!(col, take, Int2(n) => n, 0, int2_with_bitvec),
		ValueType::Int4 => extract_typed_column!(col, take, Int4(n) => n, 0, int4_with_bitvec),
		ValueType::Int8 => extract_typed_column!(col, take, Int8(n) => n, 0, int8_with_bitvec),
		ValueType::Int16 => extract_typed_column!(col, take, Int16(n) => n, 0, int16_with_bitvec),
		ValueType::Utf8 => {
			extract_typed_column!(col, take, Utf8(s) => s.clone(), "".to_string(), utf8_with_bitvec)
		}
		ValueType::Uint1 => extract_typed_column!(col, take, Uint1(n) => n, 0, uint1_with_bitvec),
		ValueType::Uint2 => extract_typed_column!(col, take, Uint2(n) => n, 0, uint2_with_bitvec),
		ValueType::Uint4 => extract_typed_column!(col, take, Uint4(n) => n, 0, uint4_with_bitvec),
		ValueType::Uint8 => extract_typed_column!(col, take, Uint8(n) => n, 0, uint8_with_bitvec),
		ValueType::Uint16 => extract_typed_column!(col, take, Uint16(n) => n, 0, uint16_with_bitvec),
		ValueType::Date => extract_typed_column!(col, take, Date(d) => d, Date::default(), date_with_bitvec),
		ValueType::DateTime => {
			extract_typed_column!(col, take, DateTime(dt) => dt, DateTime::default(), datetime_with_bitvec)
		}
		ValueType::Time => extract_typed_column!(col, take, Time(t) => t, Time::default(), time_with_bitvec),
		ValueType::Duration => {
			extract_typed_column!(col, take, Duration(i) => i, Duration::default(), duration_with_bitvec)
		}
		ValueType::IdentityId => {
			extract_typed_column!(col, take, IdentityId(i) => i, IdentityId::default(), identity_id_with_bitvec)
		}
		ValueType::Uuid4 => {
			extract_typed_column!(col, take, Uuid4(i) => i, Uuid4::default(), uuid4_with_bitvec)
		}
		ValueType::Uuid7 => {
			extract_typed_column!(col, take, Uuid7(i) => i, Uuid7::default(), uuid7_with_bitvec)
		}
		ValueType::DictionaryId => {
			extract_typed_column!(col, take, DictionaryId(i) => i, DictionaryEntryId::default(), dictionary_id_with_bitvec)
		}
		ValueType::Blob => {
			extract_typed_column!(col, take, Blob(b) => b.clone(), Blob::new(vec![]), blob_with_bitvec)
		}
		ValueType::Int => extract_typed_column!(col, take, Int(b) => b.clone(), Int::zero(), int_with_bitvec),
		ValueType::Uint => {
			extract_typed_column!(col, take, Uint(b) => b.clone(), Uint::zero(), uint_with_bitvec)
		}
		ValueType::Any => {
			extract_typed_column!(col, take, Any(boxed) => Box::new(*boxed.clone()), Box::new(Value::none()), any_with_bitvec)
		}
		ValueType::Decimal => {
			extract_typed_column!(col, take, Decimal(b) => b.clone(), Decimal::from_i64(0), decimal_with_bitvec)
		}

		ValueType::Vector(_) => {
			let mut rows: Vec<Option<VectorValue>> = Vec::new();
			for v in col.data().iter().take(take) {
				match v {
					Value::Vector(vec) => rows.push(Some(vec.clone())),
					_ => rows.push(None),
				}
			}
			let dims: u32 = rows.iter().flatten().map(|v| v.dims() as u32).next().unwrap_or(1);
			let mut data: Vec<f32> = Vec::with_capacity(rows.len() * dims as usize);
			let mut bitvec: Vec<bool> = Vec::with_capacity(rows.len());
			for row in &rows {
				match row {
					Some(v) if v.dims() == dims as usize => {
						data.extend_from_slice(v.as_slice());
						bitvec.push(true);
					}
					_ => {
						data.extend(repeat_n(0.0f32, dims as usize));
						bitvec.push(false);
					}
				}
			}
			Ok(col.with_new_data(ColumnBuffer::vector_with_bitvec(dims, data, bitvec)))
		}
		ValueType::Option(inner) => extract_column_data_by_type(col, take, *inner),
		ValueType::List(_) => {
			extract_typed_column!(col, take, Any(boxed) => Box::new(*boxed.clone()), Box::new(Value::none()), any_with_bitvec)
		}
		ValueType::Record(_) => {
			extract_typed_column!(col, take, Any(boxed) => Box::new(*boxed.clone()), Box::new(Value::none()), any_with_bitvec)
		}
		ValueType::Tuple(_) => {
			extract_typed_column!(col, take, Any(boxed) => Box::new(*boxed.clone()), Box::new(Value::none()), any_with_bitvec)
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::identifier::{ColumnIdentifier, ColumnShape},
		value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
	};
	use reifydb_routine::routine::registry::Routines;
	use reifydb_rql::expression::ColumnExpression;
	use reifydb_runtime::context::{RuntimeContext, clock::Clock};
	use reifydb_value::{fragment::Fragment, params::Params, value::identity::IdentityId};

	use super::column_lookup;
	use crate::{expression::context::EvalContext, vm::stack::SymbolTable};

	#[test]
	fn test_column_not_found_returns_correct_row_count() {
		// Create context with 5 rows
		let columns = Columns::new(vec![ColumnWithName::new(
			"existing_col".to_string(),
			ColumnBuffer::int4([1, 2, 3, 4, 5]),
		)]);

		let runtime_ctx = RuntimeContext::with_clock(Clock::Real);
		let routines = Routines::empty();
		let base = EvalContext {
			params: &Params::None,
			symbols: &SymbolTable::new(),
			routines: &routines,
			runtime_context: &runtime_ctx,
			arena: None,
			identity: IdentityId::root(),
			is_aggregate_context: false,
			columns: Columns::empty(),
			row_count: 1,
			target: None,
			take: None,
		};
		let ctx = base.with_eval(columns, 5);

		// Try to access a column that doesn't exist
		let result = column_lookup(
			&ctx,
			&ColumnExpression(ColumnIdentifier {
				shape: ColumnShape::Alias(Fragment::internal("nonexistent_col")),
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
