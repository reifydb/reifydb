// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use reifydb_core::{
	error::diagnostic::query::column_not_found,
	expression::ColumnExpression,
	value::{
		batch::is_scalar,
		column::factory::{self, rename},
	},
};
use reifydb_value::{
	error,
	value::{
		Value,
		blob::Blob,
		column_view::{ColumnView, ViewData},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		system_columns::{is_system_field, resolve_column, user_columns},
		time::Time,
		uuid::{Uuid4, Uuid7},
		value_type::{
			ValueType,
			field::{from_field, named},
		},
	},
};

use crate::{Result, expression::context::EvalContext, stack::Variable};

macro_rules! extract_typed_column {
	($col:expr, $take:expr, $variant:ident($x:ident) => $transform:expr, $default:expr, $constructor:ident $(, $arg:expr)*) => {{
		let mut data = Vec::new();
		let mut bitvec = Vec::new();
		let mut count = 0;
		let view = ColumnView::try_from($col)?;
		for v in view.iter() {
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
		Ok(factory::$constructor($col.0.name(), $($arg,)* data, bitvec))
	}};
}

pub(crate) fn column_lookup(ctx: &EvalContext, column: &ColumnExpression) -> Result<(FieldRef, ArrayRef)> {
	let name = column.0.name.text();

	if let Some(index) = resolve_column(&ctx.batch, name) {
		let owned = (ctx.batch.schema_ref().fields()[index].clone(), ctx.batch.column(index).clone());
		if is_system_field(&owned.0) {
			return Ok(rename(owned, name));
		}
		return extract_column_data(&owned, ctx);
	}

	if let Some(Variable::Columns {
		batch: scalar_batch,
	}) = ctx.symbols.get(name)
		&& is_scalar(scalar_batch)
		&& let Some((field, array)) = user_columns(scalar_batch).next()
	{
		return extract_column_data(&(field.clone(), array.clone()), ctx);
	}

	Err(error!(column_not_found(column.0.name.clone())))
}

fn extract_column_data(col: &(FieldRef, ArrayRef), ctx: &EvalContext) -> Result<(FieldRef, ArrayRef)> {
	let take = ctx.take.unwrap_or(usize::MAX);

	if take >= col.1.len() {
		return Ok(col.clone());
	}

	let col_type = ColumnView::try_from(col)?.get_type();
	let effective_type = match col_type {
		ValueType::Option(inner) => *inner,
		other => other,
	};

	extract_column_data_by_type(col, take, effective_type)
}

fn extract_any_column(col: &(FieldRef, ArrayRef), take: usize) -> Result<(FieldRef, ArrayRef)> {
	let view = ColumnView::try_from(col)?;
	let values = view.iter().take(take).map(|value| match value {
		Value::Any(boxed) => Some(*boxed),
		_ => None,
	});
	Ok(factory::any_optional(col.0.name(), values))
}

fn extract_column_data_by_type(
	col: &(FieldRef, ArrayRef),
	take: usize,
	col_type: ValueType,
) -> Result<(FieldRef, ArrayRef)> {
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
			let dictionary_id = match ColumnView::try_from(col)?.data {
				ViewData::DictionaryId {
					dictionary_id,
					..
				} => dictionary_id,
				_ => None,
			};
			let taken: Result<(FieldRef, ArrayRef)> = extract_typed_column!(col, take, DictionaryId(i) => i, DictionaryEntryId::default(), dictionary_id_with_bitvec);
			let taken = taken?;
			if let Some(id) = dictionary_id
				&& matches!(ColumnView::try_from(&taken)?.data, ViewData::DictionaryId { .. })
			{
				let mut field_type = from_field(&taken.0)?;
				field_type.dictionary_id = Some(id);
				return Ok(named(taken.0.name(), field_type, taken.1));
			}
			Ok(taken)
		}
		ValueType::Blob => {
			extract_typed_column!(col, take, Blob(b) => b.clone(), Blob::new(vec![]), blob_with_bitvec)
		}
		ValueType::Any => extract_any_column(col, take),
		ValueType::Decimal {
			precision,
			scale,
		} => {
			extract_typed_column!(col, take, Decimal(b) => b.clone(), Decimal::zero(), decimal_with_bitvec, precision, scale)
		}
		ValueType::Option(inner) => extract_column_data_by_type(col, take, *inner),
		ValueType::List(_) => extract_any_column(col, take),
		ValueType::Record(_) => extract_any_column(col, take),
		ValueType::Tuple(_) => extract_any_column(col, take),
		ValueType::Digest {
			..
		} => Ok((col.0.clone(), col.1.slice(0, take))),
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		expression::ColumnExpression,
		interface::identifier::{ColumnIdentifier, ColumnObject},
		value::{
			batch::{batch, empty_batch},
			column::factory::int4,
		},
	};
	use reifydb_routine_abi::registry::Routines;
	use reifydb_runtime::context::{RuntimeContext, clock::Clock};
	use reifydb_value::{fragment::Fragment, params::Params, value::identity::IdentityId};

	use super::column_lookup;
	use crate::{expression::context::EvalContext, stack::SymbolTable};

	#[test]
	fn test_column_not_found_returns_correct_row_count() {
		// A missing column must be an error, otherwise a typo silently evaluates to none in every row.
		let columns = batch(vec![int4("existing_col", [1, 2, 3, 4, 5])]).unwrap();

		let runtime_ctx = RuntimeContext::with_clock(Clock::Real);
		let routines = Routines::empty();
		let base = EvalContext {
			params: &Params::None,
			symbols: &SymbolTable::new(),
			routines: &routines,
			runtime_context: &runtime_ctx,
			identity: IdentityId::root(),
			is_aggregate_context: false,
			batch: empty_batch(),
			row_count: 1,
			target: None,
			take: None,
		};
		let ctx = base.with_eval(columns, 5);

		let Err(err) = column_lookup(
			&ctx,
			&ColumnExpression(ColumnIdentifier {
				object: ColumnObject::Alias(Fragment::internal("nonexistent_col")),
				name: Fragment::internal("nonexistent_col"),
			}),
		) else {
			panic!("a missing column must be an error, not a column of none");
		};

		assert_eq!(err.code, "QUERY_001", "a missing column must report column not found: {err:?}");
		assert_eq!(err.fragment.text(), "nonexistent_col", "the error must name the missing column: {err:?}");
	}
}
