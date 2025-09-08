// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::Value;

use crate::value::columnar::ColumnData;

impl ColumnData {
	pub fn from_many(value: Value, row_count: usize) -> Self {
		match value {
			Value::Boolean(v) => {
				ColumnData::bool(vec![v; row_count])
			}
			Value::Float4(v) => ColumnData::float4([v.value()]),
			Value::Float8(v) => ColumnData::float8([v.value()]),
			Value::Int1(v) => ColumnData::int1(vec![v; row_count]),
			Value::Int2(v) => ColumnData::int2(vec![v; row_count]),
			Value::Int4(v) => ColumnData::int4(vec![v; row_count]),
			Value::Int8(v) => ColumnData::int8(vec![v; row_count]),
			Value::Int16(v) => {
				ColumnData::int16(vec![v; row_count])
			}
			Value::Utf8(v) => ColumnData::utf8(vec![v; row_count]),
			Value::Uint1(v) => {
				ColumnData::uint1(vec![v; row_count])
			}
			Value::Uint2(v) => {
				ColumnData::uint2(vec![v; row_count])
			}
			Value::Uint4(v) => {
				ColumnData::uint4(vec![v; row_count])
			}
			Value::Uint8(v) => {
				ColumnData::uint8(vec![v; row_count])
			}
			Value::Uint16(v) => {
				ColumnData::uint16(vec![v; row_count])
			}
			Value::Date(v) => ColumnData::date(vec![v; row_count]),
			Value::DateTime(v) => {
				ColumnData::datetime(vec![v; row_count])
			}
			Value::Time(v) => ColumnData::time(vec![v; row_count]),
			Value::Interval(v) => {
				ColumnData::interval(vec![v; row_count])
			}
			Value::RowNumber(v) => {
				ColumnData::row_number(vec![v; row_count])
			}
			Value::IdentityId(v) => {
				ColumnData::identity_id(vec![v; row_count])
			}
			Value::Uuid4(v) => {
				ColumnData::uuid4(vec![v; row_count])
			}
			Value::Uuid7(v) => {
				ColumnData::uuid7(vec![v; row_count])
			}
			Value::Blob(v) => ColumnData::blob(vec![v; row_count]),
			Value::VarInt(v) => {
				ColumnData::varint(vec![v; row_count])
			}
			Value::VarUint(v) => {
				ColumnData::varuint(vec![v; row_count])
			}
			Value::Decimal(v) => {
				ColumnData::decimal(vec![v; row_count])
			}
			Value::Undefined => ColumnData::undefined(row_count),
		}
	}
}

impl From<Value> for ColumnData {
	fn from(value: Value) -> Self {
		Self::from_many(value, 1)
	}
}
