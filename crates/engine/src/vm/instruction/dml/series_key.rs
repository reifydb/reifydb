// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::series::{SeriesDef, SeriesKey, TimestampPrecision},
	value::column::data::ColumnData,
};
use reifydb_type::value::{Value, datetime::DateTime, r#type::Type};

/// Convert a Value to i64 for series key encoding.
///
/// Handles both integer and datetime key types. For datetime keys, the nanos-since-epoch
/// value is divided by the precision factor to recover the original i64 key.
pub(crate) fn value_to_i64(value: Value, key: &SeriesKey) -> Option<i64> {
	match value {
		Value::Int1(v) => Some(v as i64),
		Value::Int2(v) => Some(v as i64),
		Value::Int4(v) => Some(v as i64),
		Value::Int8(v) => Some(v),
		Value::Int16(v) => Some(v as i64),
		Value::Uint1(v) => Some(v as i64),
		Value::Uint2(v) => Some(v as i64),
		Value::Uint4(v) => Some(v as i64),
		Value::Uint8(v) => Some(v as i64),
		Value::Uint16(v) => Some(v as i64),
		Value::DateTime(dt) => {
			let nanos = dt.to_nanos_since_epoch();
			match key {
				SeriesKey::DateTime {
					precision,
					..
				} => Some(match precision {
					TimestampPrecision::Second => nanos / 1_000_000_000,
					TimestampPrecision::Millisecond => nanos / 1_000_000,
					TimestampPrecision::Microsecond => nanos / 1_000,
					TimestampPrecision::Nanosecond => nanos,
				}),
				_ => Some(nanos),
			}
		}
		_ => None,
	}
}

/// Convert an i64 key value back to the appropriate Value type for encoding.
pub(crate) fn value_from_i64(v: i64, ty: Option<&Type>, key: &SeriesKey) -> Value {
	match ty {
		Some(Type::Int1) => Value::Int1(v as i8),
		Some(Type::Int2) => Value::Int2(v as i16),
		Some(Type::Int4) => Value::Int4(v as i32),
		Some(Type::Uint1) => Value::Uint1(v as u8),
		Some(Type::Uint2) => Value::Uint2(v as u16),
		Some(Type::Uint4) => Value::Uint4(v as u32),
		Some(Type::Uint8) => Value::Uint8(v as u64),
		Some(Type::Uint16) => Value::Uint16(v as u128),
		Some(Type::Int16) => Value::Int16(v as i128),
		Some(Type::DateTime) => {
			let nanos = match key {
				SeriesKey::DateTime {
					precision,
					..
				} => match precision {
					TimestampPrecision::Second => v * 1_000_000_000,
					TimestampPrecision::Millisecond => v * 1_000_000,
					TimestampPrecision::Microsecond => v * 1_000,
					TimestampPrecision::Nanosecond => v,
				},
				_ => v,
			};
			Value::DateTime(DateTime::from_nanos_since_epoch(nanos))
		}
		_ => Value::Int8(v),
	}
}

/// Build a ColumnData from i64 key values using the proper key column type.
pub(crate) fn column_data_from_i64_keys(keys: Vec<i64>, series_def: &SeriesDef, key: &SeriesKey) -> ColumnData {
	let key_column_name = key.column();
	let key_type = series_def.columns.iter().find(|c| c.name == key_column_name).map(|c| c.constraint.get_type());

	match &key_type {
		Some(ty) => {
			let mut data = ColumnData::with_capacity(ty.clone(), keys.len());
			for k in keys {
				data.push_value(value_from_i64(k, key_type.as_ref(), key));
			}
			data
		}
		None => ColumnData::int8(keys),
	}
}
