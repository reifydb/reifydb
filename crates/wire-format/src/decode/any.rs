// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Decoding for the dynamic Any type with per-value type tags.

use std::str;

use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use reifydb_type::value::{
	Value,
	blob::Blob,
	container::any::AnyContainer,
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	duration::Duration,
	frame::data::FrameColumnData,
	identity::IdentityId,
	int::Int,
	ordered_f32::OrderedF32,
	ordered_f64::OrderedF64,
	time::Time,
	r#type::Type,
	uint::Uint,
	uuid::{Uuid4, Uuid7},
};
use uuid::Uuid;

use crate::error::DecodeError;

/// Decode an Any-typed column from plain-encoded data.
pub(crate) fn decode_any_column(row_count: usize, data: &[u8]) -> Result<FrameColumnData, DecodeError> {
	let mut values = Vec::with_capacity(row_count);
	let mut dpos = 0;
	for _ in 0..row_count {
		let (val, new_pos) = decode_any_value(data, dpos)?;
		values.push(Box::new(val));
		dpos = new_pos;
	}
	Ok(FrameColumnData::Any(AnyContainer::new(values)))
}

/// Decode a single type-tagged Value from a byte slice.
pub(crate) fn decode_any_value(data: &[u8], pos: usize) -> Result<(Value, usize), DecodeError> {
	if pos >= data.len() {
		return Err(DecodeError::UnexpectedEof {
			expected: 1,
			available: 0,
		});
	}
	let type_tag = data[pos];
	let ty = Type::from_u8(type_tag);
	let pos = pos + 1;

	match ty {
		Type::Boolean => Ok((Value::Boolean(data[pos] != 0), pos + 1)),
		Type::Float4 => {
			let v = f32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
			let ordered = OrderedF32::try_from(v)
				.map_err(|e| DecodeError::InvalidData(format!("invalid float4: {}", e)))?;
			Ok((Value::Float4(ordered), pos + 4))
		}
		Type::Float8 => {
			let v = f64::from_le_bytes([
				data[pos],
				data[pos + 1],
				data[pos + 2],
				data[pos + 3],
				data[pos + 4],
				data[pos + 5],
				data[pos + 6],
				data[pos + 7],
			]);
			let ordered = OrderedF64::try_from(v)
				.map_err(|e| DecodeError::InvalidData(format!("invalid float8: {}", e)))?;
			Ok((Value::Float8(ordered), pos + 8))
		}
		Type::Int1 => Ok((Value::Int1(data[pos] as i8), pos + 1)),
		Type::Int2 => {
			let v = i16::from_le_bytes([data[pos], data[pos + 1]]);
			Ok((Value::Int2(v), pos + 2))
		}
		Type::Int4 => {
			let v = i32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
			Ok((Value::Int4(v), pos + 4))
		}
		Type::Int8 => {
			let v = i64::from_le_bytes([
				data[pos],
				data[pos + 1],
				data[pos + 2],
				data[pos + 3],
				data[pos + 4],
				data[pos + 5],
				data[pos + 6],
				data[pos + 7],
			]);
			Ok((Value::Int8(v), pos + 8))
		}
		Type::Int16 => {
			let mut bytes = [0u8; 16];
			bytes.copy_from_slice(&data[pos..pos + 16]);
			Ok((Value::Int16(i128::from_le_bytes(bytes)), pos + 16))
		}
		Type::Uint1 => Ok((Value::Uint1(data[pos]), pos + 1)),
		Type::Uint2 => {
			let v = u16::from_le_bytes([data[pos], data[pos + 1]]);
			Ok((Value::Uint2(v), pos + 2))
		}
		Type::Uint4 => {
			let v = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
			Ok((Value::Uint4(v), pos + 4))
		}
		Type::Uint8 => {
			let v = u64::from_le_bytes([
				data[pos],
				data[pos + 1],
				data[pos + 2],
				data[pos + 3],
				data[pos + 4],
				data[pos + 5],
				data[pos + 6],
				data[pos + 7],
			]);
			Ok((Value::Uint8(v), pos + 8))
		}
		Type::Uint16 => {
			let mut bytes = [0u8; 16];
			bytes.copy_from_slice(&data[pos..pos + 16]);
			Ok((Value::Uint16(u128::from_le_bytes(bytes)), pos + 16))
		}
		Type::Date => {
			let days = i32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
			let date = Date::from_days_since_epoch(days)
				.ok_or_else(|| DecodeError::InvalidData(format!("invalid date: {}", days)))?;
			Ok((Value::Date(date), pos + 4))
		}
		Type::DateTime => {
			let nanos = u64::from_le_bytes([
				data[pos],
				data[pos + 1],
				data[pos + 2],
				data[pos + 3],
				data[pos + 4],
				data[pos + 5],
				data[pos + 6],
				data[pos + 7],
			]);
			Ok((Value::DateTime(DateTime::from_nanos(nanos)), pos + 8))
		}
		Type::Time => {
			let nanos = u64::from_le_bytes([
				data[pos],
				data[pos + 1],
				data[pos + 2],
				data[pos + 3],
				data[pos + 4],
				data[pos + 5],
				data[pos + 6],
				data[pos + 7],
			]);
			let time = Time::from_nanos_since_midnight(nanos)
				.ok_or_else(|| DecodeError::InvalidData(format!("invalid time: {}", nanos)))?;
			Ok((Value::Time(time), pos + 8))
		}
		Type::Duration => {
			let months = i32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
			let days = i32::from_le_bytes([data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7]]);
			let nanos = i64::from_le_bytes([
				data[pos + 8],
				data[pos + 9],
				data[pos + 10],
				data[pos + 11],
				data[pos + 12],
				data[pos + 13],
				data[pos + 14],
				data[pos + 15],
			]);
			let dur = Duration::new(months, days, nanos)
				.map_err(|e| DecodeError::InvalidData(format!("invalid duration: {}", e)))?;
			Ok((Value::Duration(dur), pos + 16))
		}
		Type::IdentityId => {
			let mut bytes = [0u8; 16];
			bytes.copy_from_slice(&data[pos..pos + 16]);
			let uuid = Uuid::from_bytes(bytes);
			Ok((Value::IdentityId(IdentityId::new(Uuid7(uuid))), pos + 16))
		}
		Type::Uuid4 => {
			let mut bytes = [0u8; 16];
			bytes.copy_from_slice(&data[pos..pos + 16]);
			Ok((Value::Uuid4(Uuid4(Uuid::from_bytes(bytes))), pos + 16))
		}
		Type::Uuid7 => {
			let mut bytes = [0u8; 16];
			bytes.copy_from_slice(&data[pos..pos + 16]);
			Ok((Value::Uuid7(Uuid7(Uuid::from_bytes(bytes))), pos + 16))
		}
		Type::Utf8 => {
			let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
			let s = str::from_utf8(&data[pos + 4..pos + 4 + len])
				.map_err(|e| DecodeError::InvalidData(format!("invalid UTF-8: {}", e)))?;
			Ok((Value::Utf8(s.to_string()), pos + 4 + len))
		}
		Type::Blob => {
			let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
			let bytes = data[pos + 4..pos + 4 + len].to_vec();
			Ok((Value::Blob(Blob::new(bytes)), pos + 4 + len))
		}
		Type::Int => {
			let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
			let big = BigInt::from_signed_bytes_le(&data[pos + 4..pos + 4 + len]);
			Ok((Value::Int(Int(big)), pos + 4 + len))
		}
		Type::Uint => {
			let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
			let big = BigInt::from_signed_bytes_le(&data[pos + 4..pos + 4 + len]);
			Ok((Value::Uint(Uint(big)), pos + 4 + len))
		}
		Type::Decimal => {
			let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
			let s = str::from_utf8(&data[pos + 4..pos + 4 + len])
				.map_err(|e| DecodeError::InvalidData(format!("invalid decimal: {}", e)))?;
			let dec: BigDecimal =
				s.parse().map_err(|e| DecodeError::InvalidData(format!("invalid decimal: {}", e)))?;
			Ok((Value::Decimal(Decimal::new(dec)), pos + 4 + len))
		}
		_ => Err(DecodeError::UnsupportedType(format!("{:?}", ty))),
	}
}
