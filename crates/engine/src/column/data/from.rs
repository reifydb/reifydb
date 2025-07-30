// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::EngineColumnData;
use reifydb_core::Value;

impl EngineColumnData {
    pub fn from_many(value: Value, row_count: usize) -> Self {
        match value {
            Value::Bool(v) => EngineColumnData::bool(vec![v; row_count]),
            Value::Float4(v) => EngineColumnData::float4([v.value()]),
            Value::Float8(v) => EngineColumnData::float8([v.value()]),
            Value::Int1(v) => EngineColumnData::int1(vec![v; row_count]),
            Value::Int2(v) => EngineColumnData::int2(vec![v; row_count]),
            Value::Int4(v) => EngineColumnData::int4(vec![v; row_count]),
            Value::Int8(v) => EngineColumnData::int8(vec![v; row_count]),
            Value::Int16(v) => EngineColumnData::int16(vec![v; row_count]),
            Value::Utf8(v) => EngineColumnData::utf8(vec![v; row_count]),
            Value::Uint1(v) => EngineColumnData::uint1(vec![v; row_count]),
            Value::Uint2(v) => EngineColumnData::uint2(vec![v; row_count]),
            Value::Uint4(v) => EngineColumnData::uint4(vec![v; row_count]),
            Value::Uint8(v) => EngineColumnData::uint8(vec![v; row_count]),
            Value::Uint16(v) => EngineColumnData::uint16(vec![v; row_count]),
            Value::Date(v) => EngineColumnData::date(vec![v; row_count]),
            Value::DateTime(v) => EngineColumnData::datetime(vec![v; row_count]),
            Value::Time(v) => EngineColumnData::time(vec![v; row_count]),
            Value::Interval(v) => EngineColumnData::interval(vec![v; row_count]),
            Value::RowId(v) => EngineColumnData::row_id(vec![v; row_count]),
            Value::Uuid4(v) => EngineColumnData::uuid4(vec![v; row_count]),
            Value::Uuid7(v) => EngineColumnData::uuid7(vec![v; row_count]),
            Value::Blob(v) => EngineColumnData::blob(vec![v; row_count]),
            Value::Undefined => EngineColumnData::undefined(row_count),
        }
    }
}

impl From<Value> for EngineColumnData {
    fn from(value: Value) -> Self {
        Self::from_many(value, 1)
    }
}
