// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::ColumnValues;
use reifydb_core::Value;

impl ColumnValues {
    pub fn from_many(value: Value, row_count: usize) -> Self {
        match value {
            Value::Bool(v) => ColumnValues::bool(vec![v; row_count]),
            Value::Float4(v) => ColumnValues::float4([v.value()]),
            Value::Float8(v) => ColumnValues::float8([v.value()]),
            Value::Int1(v) => ColumnValues::int1(vec![v; row_count]),
            Value::Int2(v) => ColumnValues::int2(vec![v; row_count]),
            Value::Int4(v) => ColumnValues::int4(vec![v; row_count]),
            Value::Int8(v) => ColumnValues::int8(vec![v; row_count]),
            Value::Int16(v) => ColumnValues::int16(vec![v; row_count]),
            Value::Utf8(v) => ColumnValues::utf8(vec![v; row_count]),
            Value::Uint1(v) => ColumnValues::uint1(vec![v; row_count]),
            Value::Uint2(v) => ColumnValues::uint2(vec![v; row_count]),
            Value::Uint4(v) => ColumnValues::uint4(vec![v; row_count]),
            Value::Uint8(v) => ColumnValues::uint8(vec![v; row_count]),
            Value::Uint16(v) => ColumnValues::uint16(vec![v; row_count]),
            Value::Date(v) => ColumnValues::date(vec![v; row_count]),
            Value::DateTime(v) => ColumnValues::datetime(vec![v; row_count]),
            Value::Time(v) => ColumnValues::time(vec![v; row_count]),
            Value::Interval(v) => ColumnValues::interval(vec![v; row_count]),
            Value::RowId(v) => ColumnValues::row_id(vec![v; row_count]),
            Value::Uuid4(v) => ColumnValues::uuid4(vec![v; row_count]),
            Value::Uuid7(v) => ColumnValues::uuid7(vec![v; row_count]),
            Value::Blob(v) => ColumnValues::blob(vec![v; row_count]),
            Value::Undefined => ColumnValues::undefined(row_count),
        }
    }
}

impl From<Value> for ColumnValues {
    fn from(value: Value) -> Self {
        Self::from_many(value, 1)
    }
}
