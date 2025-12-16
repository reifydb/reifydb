// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Value encoding utilities for storage operations.

use reifydb_core::value::encoded::{EncodedValues, EncodedValuesLayout};
use reifydb_type::Value;

/// Encode a single value into an encoded row at the specified column index.
pub fn encode_value(layout: &EncodedValuesLayout, row: &mut EncodedValues, idx: usize, value: &Value) {
	match value {
		Value::Boolean(v) => layout.set_bool(row, idx, *v),
		Value::Float4(v) => layout.set_f32(row, idx, **v),
		Value::Float8(v) => layout.set_f64(row, idx, **v),
		Value::Int1(v) => layout.set_i8(row, idx, *v),
		Value::Int2(v) => layout.set_i16(row, idx, *v),
		Value::Int4(v) => layout.set_i32(row, idx, *v),
		Value::Int8(v) => layout.set_i64(row, idx, *v),
		Value::Int16(v) => layout.set_i128(row, idx, *v),
		Value::Utf8(v) => layout.set_utf8(row, idx, v),
		Value::Uint1(v) => layout.set_u8(row, idx, *v),
		Value::Uint2(v) => layout.set_u16(row, idx, *v),
		Value::Uint4(v) => layout.set_u32(row, idx, *v),
		Value::Uint8(v) => layout.set_u64(row, idx, *v),
		Value::Uint16(v) => layout.set_u128(row, idx, *v),
		Value::Date(v) => layout.set_date(row, idx, *v),
		Value::DateTime(v) => layout.set_datetime(row, idx, *v),
		Value::Time(v) => layout.set_time(row, idx, *v),
		Value::Duration(v) => layout.set_duration(row, idx, *v),
		Value::RowNumber(_) => {
			// Row numbers are not stored in the encoded row - they are managed separately
		}
		Value::IdentityId(v) => layout.set_identity_id(row, idx, *v),
		Value::Uuid4(v) => layout.set_uuid4(row, idx, *v),
		Value::Uuid7(v) => layout.set_uuid7(row, idx, *v),
		Value::Blob(v) => layout.set_blob(row, idx, v),
		Value::Int(v) => layout.set_int(row, idx, v),
		Value::Uint(v) => layout.set_uint(row, idx, v),
		Value::Decimal(v) => layout.set_decimal(row, idx, v),
		Value::Undefined => layout.set_undefined(row, idx),
		Value::Any(_) => {
			unreachable!("Any type cannot be stored")
		}
	}
}
