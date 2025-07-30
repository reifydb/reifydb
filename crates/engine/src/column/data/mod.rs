// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod extend;
mod factory;
mod filter;
mod from;
mod get;
mod reorder;
mod slice;
mod take;

use reifydb_core::value::container::{
    BlobContainer, BoolContainer, NumberContainer, RowIdContainer, StringContainer,
    TemporalContainer, UndefinedContainer, UuidContainer,
};
use reifydb_core::value::{Uuid4, Uuid7};
use reifydb_core::{BitVec, Date, DateTime, Interval, Time, Type, Value};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum EngineColumnData {
    Bool(BoolContainer),
    Float4(NumberContainer<f32>),
    Float8(NumberContainer<f64>),
    Int1(NumberContainer<i8>),
    Int2(NumberContainer<i16>),
    Int4(NumberContainer<i32>),
    Int8(NumberContainer<i64>),
    Int16(NumberContainer<i128>),
    Uint1(NumberContainer<u8>),
    Uint2(NumberContainer<u16>),
    Uint4(NumberContainer<u32>),
    Uint8(NumberContainer<u64>),
    Uint16(NumberContainer<u128>),
    Utf8(StringContainer),
    Date(TemporalContainer<Date>),
    DateTime(TemporalContainer<DateTime>),
    Time(TemporalContainer<Time>),
    Interval(TemporalContainer<Interval>),
    RowId(RowIdContainer),
    Uuid4(UuidContainer<Uuid4>),
    Uuid7(UuidContainer<Uuid7>),
    Blob(BlobContainer),
    // special case: all undefined
    Undefined(UndefinedContainer),
}

impl EngineColumnData {
    pub fn get_type(&self) -> Type {
        match self {
            EngineColumnData::Bool(_) => Type::Bool,
            EngineColumnData::Float4(_) => Type::Float4,
            EngineColumnData::Float8(_) => Type::Float8,
            EngineColumnData::Int1(_) => Type::Int1,
            EngineColumnData::Int2(_) => Type::Int2,
            EngineColumnData::Int4(_) => Type::Int4,
            EngineColumnData::Int8(_) => Type::Int8,
            EngineColumnData::Int16(_) => Type::Int16,
            EngineColumnData::Uint1(_) => Type::Uint1,
            EngineColumnData::Uint2(_) => Type::Uint2,
            EngineColumnData::Uint4(_) => Type::Uint4,
            EngineColumnData::Uint8(_) => Type::Uint8,
            EngineColumnData::Uint16(_) => Type::Uint16,
            EngineColumnData::Utf8(_) => Type::Utf8,
            EngineColumnData::Date(_) => Type::Date,
            EngineColumnData::DateTime(_) => Type::DateTime,
            EngineColumnData::Time(_) => Type::Time,
            EngineColumnData::Interval(_) => Type::Interval,
            EngineColumnData::RowId(_) => Type::RowId,
            EngineColumnData::Uuid4(_) => Type::Uuid4,
            EngineColumnData::Uuid7(_) => Type::Uuid7,
            EngineColumnData::Blob(_) => Type::Blob,
            EngineColumnData::Undefined(_) => Type::Undefined,
        }
    }

    pub fn is_defined(&self, idx: usize) -> bool {
        match self {
            EngineColumnData::Bool(container) => container.is_defined(idx),
            EngineColumnData::Float4(container) => container.is_defined(idx),
            EngineColumnData::Float8(container) => container.is_defined(idx),
            EngineColumnData::Int1(container) => container.is_defined(idx),
            EngineColumnData::Int2(container) => container.is_defined(idx),
            EngineColumnData::Int4(container) => container.is_defined(idx),
            EngineColumnData::Int8(container) => container.is_defined(idx),
            EngineColumnData::Int16(container) => container.is_defined(idx),
            EngineColumnData::Uint1(container) => container.is_defined(idx),
            EngineColumnData::Uint2(container) => container.is_defined(idx),
            EngineColumnData::Uint4(container) => container.is_defined(idx),
            EngineColumnData::Uint8(container) => container.is_defined(idx),
            EngineColumnData::Uint16(container) => container.is_defined(idx),
            EngineColumnData::Utf8(container) => container.is_defined(idx),
            EngineColumnData::Date(container) => container.is_defined(idx),
            EngineColumnData::DateTime(container) => container.is_defined(idx),
            EngineColumnData::Time(container) => container.is_defined(idx),
            EngineColumnData::Interval(container) => container.is_defined(idx),
            EngineColumnData::RowId(container) => container.is_defined(idx),
            EngineColumnData::Uuid4(container) => container.is_defined(idx),
            EngineColumnData::Uuid7(container) => container.is_defined(idx),
            EngineColumnData::Blob(container) => container.is_defined(idx),
            EngineColumnData::Undefined(_) => false,
        }
    }

    pub fn is_bool(&self) -> bool {
        self.get_type() == Type::Bool
    }

    pub fn is_float(&self) -> bool {
        self.get_type() == Type::Float4 || self.get_type() == Type::Float8
    }

    pub fn is_utf8(&self) -> bool {
        self.get_type() == Type::Utf8
    }

    pub fn is_number(&self) -> bool {
        matches!(
            self.get_type(),
            Type::Float4
                | Type::Float8
                | Type::Int1
                | Type::Int2
                | Type::Int4
                | Type::Int8
                | Type::Int16
                | Type::Uint1
                | Type::Uint2
                | Type::Uint4
                | Type::Uint8
                | Type::Uint16
        )
    }

    pub fn is_text(&self) -> bool {
        self.get_type() == Type::Utf8
    }

    pub fn is_temporal(&self) -> bool {
        matches!(self.get_type(), Type::Date | Type::DateTime | Type::Time | Type::Interval)
    }

    pub fn is_uuid(&self) -> bool {
        matches!(self.get_type(), Type::Uuid4 | Type::Uuid7)
    }
}

impl EngineColumnData {
    pub fn bitvec(&self) -> &BitVec {
        match self {
            EngineColumnData::Bool(container) => container.bitvec(),
            EngineColumnData::Float4(container) => container.bitvec(),
            EngineColumnData::Float8(container) => container.bitvec(),
            EngineColumnData::Int1(container) => container.bitvec(),
            EngineColumnData::Int2(container) => container.bitvec(),
            EngineColumnData::Int4(container) => container.bitvec(),
            EngineColumnData::Int8(container) => container.bitvec(),
            EngineColumnData::Int16(container) => container.bitvec(),
            EngineColumnData::Uint1(container) => container.bitvec(),
            EngineColumnData::Uint2(container) => container.bitvec(),
            EngineColumnData::Uint4(container) => container.bitvec(),
            EngineColumnData::Uint8(container) => container.bitvec(),
            EngineColumnData::Uint16(container) => container.bitvec(),
            EngineColumnData::Utf8(container) => container.bitvec(),
            EngineColumnData::Date(container) => container.bitvec(),
            EngineColumnData::DateTime(container) => container.bitvec(),
            EngineColumnData::Time(container) => container.bitvec(),
            EngineColumnData::Interval(container) => container.bitvec(),
            EngineColumnData::RowId(container) => container.bitvec(),
            EngineColumnData::Uuid4(container) => container.bitvec(),
            EngineColumnData::Uuid7(container) => container.bitvec(),
            EngineColumnData::Blob(container) => container.bitvec(),
            EngineColumnData::Undefined(_) => unreachable!(),
        }
    }
}

impl EngineColumnData {
    pub fn with_capacity(target: Type, capacity: usize) -> Self {
        match target {
            Type::Bool => Self::bool_with_capacity(capacity),
            Type::Float4 => Self::float4_with_capacity(capacity),
            Type::Float8 => Self::float8_with_capacity(capacity),
            Type::Int1 => Self::int1_with_capacity(capacity),
            Type::Int2 => Self::int2_with_capacity(capacity),
            Type::Int4 => Self::int4_with_capacity(capacity),
            Type::Int8 => Self::int8_with_capacity(capacity),
            Type::Int16 => Self::int16_with_capacity(capacity),
            Type::Uint1 => Self::uint1_with_capacity(capacity),
            Type::Uint2 => Self::uint2_with_capacity(capacity),
            Type::Uint4 => Self::uint4_with_capacity(capacity),
            Type::Uint8 => Self::uint8_with_capacity(capacity),
            Type::Uint16 => Self::uint16_with_capacity(capacity),
            Type::Utf8 => Self::utf8_with_capacity(capacity),
            Type::Date => Self::date_with_capacity(capacity),
            Type::DateTime => Self::datetime_with_capacity(capacity),
            Type::Time => Self::time_with_capacity(capacity),
            Type::Interval => Self::interval_with_capacity(capacity),
            Type::RowId => Self::row_id_with_capacity(capacity),
            Type::Uuid4 => Self::uuid4_with_capacity(capacity),
            Type::Uuid7 => Self::uuid7_with_capacity(capacity),
            Type::Blob => Self::blob_with_capacity(capacity),
            Type::Undefined => Self::undefined(capacity),
        }
    }

    pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = Value> + 'a> {
        Box::new((0..self.len()).map(move |i| self.get_value(i)))
    }
}

impl EngineColumnData {
    pub fn len(&self) -> usize {
        match self {
            EngineColumnData::Bool(container) => container.len(),
            EngineColumnData::Float4(container) => container.len(),
            EngineColumnData::Float8(container) => container.len(),
            EngineColumnData::Int1(container) => container.len(),
            EngineColumnData::Int2(container) => container.len(),
            EngineColumnData::Int4(container) => container.len(),
            EngineColumnData::Int8(container) => container.len(),
            EngineColumnData::Int16(container) => container.len(),
            EngineColumnData::Uint1(container) => container.len(),
            EngineColumnData::Uint2(container) => container.len(),
            EngineColumnData::Uint4(container) => container.len(),
            EngineColumnData::Uint8(container) => container.len(),
            EngineColumnData::Uint16(container) => container.len(),
            EngineColumnData::Utf8(container) => container.len(),
            EngineColumnData::Date(container) => container.len(),
            EngineColumnData::DateTime(container) => container.len(),
            EngineColumnData::Time(container) => container.len(),
            EngineColumnData::Interval(container) => container.len(),
            EngineColumnData::RowId(container) => container.len(),
            EngineColumnData::Uuid4(container) => container.len(),
            EngineColumnData::Uuid7(container) => container.len(),
            EngineColumnData::Blob(container) => container.len(),
            EngineColumnData::Undefined(container) => container.len(),
        }
    }

    pub fn capacity(&self) -> usize {
        match self {
            EngineColumnData::Bool(container) => container.capacity(),
            EngineColumnData::Float4(container) => container.capacity(),
            EngineColumnData::Float8(container) => container.capacity(),
            EngineColumnData::Int1(container) => container.capacity(),
            EngineColumnData::Int2(container) => container.capacity(),
            EngineColumnData::Int4(container) => container.capacity(),
            EngineColumnData::Int8(container) => container.capacity(),
            EngineColumnData::Int16(container) => container.capacity(),
            EngineColumnData::Uint1(container) => container.capacity(),
            EngineColumnData::Uint2(container) => container.capacity(),
            EngineColumnData::Uint4(container) => container.capacity(),
            EngineColumnData::Uint8(container) => container.capacity(),
            EngineColumnData::Uint16(container) => container.capacity(),
            EngineColumnData::Utf8(container) => container.capacity(),
            EngineColumnData::Date(container) => container.capacity(),
            EngineColumnData::DateTime(container) => container.capacity(),
            EngineColumnData::Time(container) => container.capacity(),
            EngineColumnData::Interval(container) => container.capacity(),
            EngineColumnData::RowId(container) => container.capacity(),
            EngineColumnData::Uuid4(container) => container.capacity(),
            EngineColumnData::Uuid7(container) => container.capacity(),
            EngineColumnData::Blob(container) => container.capacity(),
            EngineColumnData::Undefined(container) => container.capacity(),
        }
    }

    pub fn as_string(&self, index: usize) -> String {
        match self {
            EngineColumnData::Bool(container) => container.as_string(index),
            EngineColumnData::Float4(container) => container.as_string(index),
            EngineColumnData::Float8(container) => container.as_string(index),
            EngineColumnData::Int1(container) => container.as_string(index),
            EngineColumnData::Int2(container) => container.as_string(index),
            EngineColumnData::Int4(container) => container.as_string(index),
            EngineColumnData::Int8(container) => container.as_string(index),
            EngineColumnData::Int16(container) => container.as_string(index),
            EngineColumnData::Uint1(container) => container.as_string(index),
            EngineColumnData::Uint2(container) => container.as_string(index),
            EngineColumnData::Uint4(container) => container.as_string(index),
            EngineColumnData::Uint8(container) => container.as_string(index),
            EngineColumnData::Uint16(container) => container.as_string(index),
            EngineColumnData::Utf8(container) => container.as_string(index),
            EngineColumnData::Date(container) => container.as_string(index),
            EngineColumnData::DateTime(container) => container.as_string(index),
            EngineColumnData::Time(container) => container.as_string(index),
            EngineColumnData::Interval(container) => container.as_string(index),
            EngineColumnData::RowId(container) => container.as_string(index),
            EngineColumnData::Uuid4(container) => container.as_string(index),
            EngineColumnData::Uuid7(container) => container.as_string(index),
            EngineColumnData::Blob(container) => container.as_string(index),
            EngineColumnData::Undefined(container) => container.as_string(index),
        }
    }
}
