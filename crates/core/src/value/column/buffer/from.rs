// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::Value;

use crate::value::column::ColumnBuffer;

impl ColumnBuffer {
	pub fn from_many(value: Value, row_count: usize) -> Self {
		match value {
			Value::Boolean(v) => ColumnBuffer::bool(vec![v; row_count]),
			Value::Float4(v) => ColumnBuffer::float4([v.value()]),
			Value::Float8(v) => ColumnBuffer::float8([v.value()]),
			Value::Int1(v) => ColumnBuffer::int1(vec![v; row_count]),
			Value::Int2(v) => ColumnBuffer::int2(vec![v; row_count]),
			Value::Int4(v) => ColumnBuffer::int4(vec![v; row_count]),
			Value::Int8(v) => ColumnBuffer::int8(vec![v; row_count]),
			Value::Int16(v) => ColumnBuffer::int16(vec![v; row_count]),
			Value::Utf8(v) => ColumnBuffer::utf8(vec![v; row_count]),
			Value::Uint1(v) => ColumnBuffer::uint1(vec![v; row_count]),
			Value::Uint2(v) => ColumnBuffer::uint2(vec![v; row_count]),
			Value::Uint4(v) => ColumnBuffer::uint4(vec![v; row_count]),
			Value::Uint8(v) => ColumnBuffer::uint8(vec![v; row_count]),
			Value::Uint16(v) => ColumnBuffer::uint16(vec![v; row_count]),
			Value::Date(v) => ColumnBuffer::date(vec![v; row_count]),
			Value::DateTime(v) => ColumnBuffer::datetime(vec![v; row_count]),
			Value::Time(v) => ColumnBuffer::time(vec![v; row_count]),
			Value::Duration(v) => ColumnBuffer::duration(vec![v; row_count]),
			Value::IdentityId(v) => ColumnBuffer::identity_id(vec![v; row_count]),
			Value::Uuid4(v) => ColumnBuffer::uuid4(vec![v; row_count]),
			Value::Uuid7(v) => ColumnBuffer::uuid7(vec![v; row_count]),
			Value::Blob(v) => ColumnBuffer::blob(vec![v; row_count]),
			Value::Int(v) => ColumnBuffer::int(vec![v; row_count]),
			Value::Uint(v) => ColumnBuffer::uint(vec![v; row_count]),
			Value::Decimal(v) => ColumnBuffer::decimal(vec![v; row_count]),
			Value::DictionaryId(v) => ColumnBuffer::dictionary_id(vec![v; row_count]),
			Value::None {
				inner,
			} => ColumnBuffer::none_typed(inner, row_count),
			Value::Type(t) => ColumnBuffer::any(vec![Box::new(Value::Type(t)); row_count]),
			Value::Any(v) => ColumnBuffer::any(vec![v.clone(); row_count]),
			Value::List(v) => ColumnBuffer::any(vec![Box::new(Value::List(v)); row_count]),
			Value::Record(v) => ColumnBuffer::any(vec![Box::new(Value::Record(v)); row_count]),
			Value::Tuple(v) => ColumnBuffer::any(vec![Box::new(Value::Tuple(v)); row_count]),
		}
	}
}

impl From<Value> for ColumnBuffer {
	fn from(value: Value) -> Self {
		Self::from_many(value, 1)
	}
}
