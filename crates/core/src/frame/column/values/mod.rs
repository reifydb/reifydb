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

use crate::frame::column::container::*;
use crate::value::uuid::{Uuid4, Uuid7};
use crate::{BitVec, Type, Value};
use crate::{Date, DateTime, Interval, Time};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ColumnValues {
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

impl ColumnValues {
    pub fn get_type(&self) -> Type {
        match self {
            ColumnValues::Bool(_) => Type::Bool,
            ColumnValues::Float4(_) => Type::Float4,
            ColumnValues::Float8(_) => Type::Float8,
            ColumnValues::Int1(_) => Type::Int1,
            ColumnValues::Int2(_) => Type::Int2,
            ColumnValues::Int4(_) => Type::Int4,
            ColumnValues::Int8(_) => Type::Int8,
            ColumnValues::Int16(_) => Type::Int16,
            ColumnValues::Uint1(_) => Type::Uint1,
            ColumnValues::Uint2(_) => Type::Uint2,
            ColumnValues::Uint4(_) => Type::Uint4,
            ColumnValues::Uint8(_) => Type::Uint8,
            ColumnValues::Uint16(_) => Type::Uint16,
            ColumnValues::Utf8(_) => Type::Utf8,
            ColumnValues::Date(_) => Type::Date,
            ColumnValues::DateTime(_) => Type::DateTime,
            ColumnValues::Time(_) => Type::Time,
            ColumnValues::Interval(_) => Type::Interval,
            ColumnValues::RowId(_) => Type::RowId,
            ColumnValues::Uuid4(_) => Type::Uuid4,
            ColumnValues::Uuid7(_) => Type::Uuid7,
            ColumnValues::Blob(_) => Type::Blob,
            ColumnValues::Undefined(_) => Type::Undefined,
        }
    }

    pub fn is_defined(&self, idx: usize) -> bool {
        match self {
            ColumnValues::Bool(container) => container.is_defined(idx),
            ColumnValues::Float4(container) => container.is_defined(idx),
            ColumnValues::Float8(container) => container.is_defined(idx),
            ColumnValues::Int1(container) => container.is_defined(idx),
            ColumnValues::Int2(container) => container.is_defined(idx),
            ColumnValues::Int4(container) => container.is_defined(idx),
            ColumnValues::Int8(container) => container.is_defined(idx),
            ColumnValues::Int16(container) => container.is_defined(idx),
            ColumnValues::Uint1(container) => container.is_defined(idx),
            ColumnValues::Uint2(container) => container.is_defined(idx),
            ColumnValues::Uint4(container) => container.is_defined(idx),
            ColumnValues::Uint8(container) => container.is_defined(idx),
            ColumnValues::Uint16(container) => container.is_defined(idx),
            ColumnValues::Utf8(container) => container.is_defined(idx),
            ColumnValues::Date(container) => container.is_defined(idx),
            ColumnValues::DateTime(container) => container.is_defined(idx),
            ColumnValues::Time(container) => container.is_defined(idx),
            ColumnValues::Interval(container) => container.is_defined(idx),
            ColumnValues::RowId(container) => container.is_defined(idx),
            ColumnValues::Uuid4(container) => container.is_defined(idx),
            ColumnValues::Uuid7(container) => container.is_defined(idx),
            ColumnValues::Blob(container) => container.is_defined(idx),
            ColumnValues::Undefined(_) => false,
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

impl ColumnValues {
    pub fn bitvec(&self) -> &BitVec {
        match self {
            ColumnValues::Bool(container) => container.bitvec(),
            ColumnValues::Float4(container) => container.bitvec(),
            ColumnValues::Float8(container) => container.bitvec(),
            ColumnValues::Int1(container) => container.bitvec(),
            ColumnValues::Int2(container) => container.bitvec(),
            ColumnValues::Int4(container) => container.bitvec(),
            ColumnValues::Int8(container) => container.bitvec(),
            ColumnValues::Int16(container) => container.bitvec(),
            ColumnValues::Uint1(container) => container.bitvec(),
            ColumnValues::Uint2(container) => container.bitvec(),
            ColumnValues::Uint4(container) => container.bitvec(),
            ColumnValues::Uint8(container) => container.bitvec(),
            ColumnValues::Uint16(container) => container.bitvec(),
            ColumnValues::Utf8(container) => container.bitvec(),
            ColumnValues::Date(container) => container.bitvec(),
            ColumnValues::DateTime(container) => container.bitvec(),
            ColumnValues::Time(container) => container.bitvec(),
            ColumnValues::Interval(container) => container.bitvec(),
            ColumnValues::RowId(container) => container.bitvec(),
            ColumnValues::Uuid4(container) => container.bitvec(),
            ColumnValues::Uuid7(container) => container.bitvec(),
            ColumnValues::Blob(container) => container.bitvec(),
            ColumnValues::Undefined(_) => unreachable!(),
        }
    }
}

impl ColumnValues {
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

impl ColumnValues {
    pub fn len(&self) -> usize {
        match self {
            ColumnValues::Bool(container) => container.len(),
            ColumnValues::Float4(container) => container.len(),
            ColumnValues::Float8(container) => container.len(),
            ColumnValues::Int1(container) => container.len(),
            ColumnValues::Int2(container) => container.len(),
            ColumnValues::Int4(container) => container.len(),
            ColumnValues::Int8(container) => container.len(),
            ColumnValues::Int16(container) => container.len(),
            ColumnValues::Uint1(container) => container.len(),
            ColumnValues::Uint2(container) => container.len(),
            ColumnValues::Uint4(container) => container.len(),
            ColumnValues::Uint8(container) => container.len(),
            ColumnValues::Uint16(container) => container.len(),
            ColumnValues::Utf8(container) => container.len(),
            ColumnValues::Date(container) => container.len(),
            ColumnValues::DateTime(container) => container.len(),
            ColumnValues::Time(container) => container.len(),
            ColumnValues::Interval(container) => container.len(),
            ColumnValues::RowId(container) => container.len(),
            ColumnValues::Uuid4(container) => container.len(),
            ColumnValues::Uuid7(container) => container.len(),
            ColumnValues::Blob(container) => container.len(),
            ColumnValues::Undefined(container) => container.len(),
        }
    }

    pub fn capacity(&self) -> usize {
        match self {
            ColumnValues::Bool(container) => container.capacity(),
            ColumnValues::Float4(container) => container.capacity(),
            ColumnValues::Float8(container) => container.capacity(),
            ColumnValues::Int1(container) => container.capacity(),
            ColumnValues::Int2(container) => container.capacity(),
            ColumnValues::Int4(container) => container.capacity(),
            ColumnValues::Int8(container) => container.capacity(),
            ColumnValues::Int16(container) => container.capacity(),
            ColumnValues::Uint1(container) => container.capacity(),
            ColumnValues::Uint2(container) => container.capacity(),
            ColumnValues::Uint4(container) => container.capacity(),
            ColumnValues::Uint8(container) => container.capacity(),
            ColumnValues::Uint16(container) => container.capacity(),
            ColumnValues::Utf8(container) => container.capacity(),
            ColumnValues::Date(container) => container.capacity(),
            ColumnValues::DateTime(container) => container.capacity(),
            ColumnValues::Time(container) => container.capacity(),
            ColumnValues::Interval(container) => container.capacity(),
            ColumnValues::RowId(container) => container.capacity(),
            ColumnValues::Uuid4(container) => container.capacity(),
            ColumnValues::Uuid7(container) => container.capacity(),
            ColumnValues::Blob(container) => container.capacity(),
            ColumnValues::Undefined(container) => container.capacity(),
        }
    }

    pub fn as_string(&self, index: usize) -> String {
        match self {
            ColumnValues::Bool(container) => container.as_string(index),
            ColumnValues::Float4(container) => container.as_string(index),
            ColumnValues::Float8(container) => container.as_string(index),
            ColumnValues::Int1(container) => container.as_string(index),
            ColumnValues::Int2(container) => container.as_string(index),
            ColumnValues::Int4(container) => container.as_string(index),
            ColumnValues::Int8(container) => container.as_string(index),
            ColumnValues::Int16(container) => container.as_string(index),
            ColumnValues::Uint1(container) => container.as_string(index),
            ColumnValues::Uint2(container) => container.as_string(index),
            ColumnValues::Uint4(container) => container.as_string(index),
            ColumnValues::Uint8(container) => container.as_string(index),
            ColumnValues::Uint16(container) => container.as_string(index),
            ColumnValues::Utf8(container) => container.as_string(index),
            ColumnValues::Date(container) => container.as_string(index),
            ColumnValues::DateTime(container) => container.as_string(index),
            ColumnValues::Time(container) => container.as_string(index),
            ColumnValues::Interval(container) => container.as_string(index),
            ColumnValues::RowId(container) => container.as_string(index),
            ColumnValues::Uuid4(container) => container.as_string(index),
            ColumnValues::Uuid7(container) => container.as_string(index),
            ColumnValues::Blob(container) => container.as_string(index),
            ColumnValues::Undefined(container) => container.as_string(index),
        }
    }
}
