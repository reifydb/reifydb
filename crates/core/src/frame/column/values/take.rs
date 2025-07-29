// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::ColumnValues;

impl ColumnValues {
    pub fn take(&self, num: usize) -> ColumnValues {
        match self {
            ColumnValues::Bool(container) => ColumnValues::Bool(container.take(num)),
            ColumnValues::Float4(container) => ColumnValues::Float4(container.take(num)),
            ColumnValues::Float8(container) => ColumnValues::Float8(container.take(num)),
            ColumnValues::Int1(container) => ColumnValues::Int1(container.take(num)),
            ColumnValues::Int2(container) => ColumnValues::Int2(container.take(num)),
            ColumnValues::Int4(container) => ColumnValues::Int4(container.take(num)),
            ColumnValues::Int8(container) => ColumnValues::Int8(container.take(num)),
            ColumnValues::Int16(container) => ColumnValues::Int16(container.take(num)),
            ColumnValues::Utf8(container) => ColumnValues::Utf8(container.take(num)),
            ColumnValues::Uint1(container) => ColumnValues::Uint1(container.take(num)),
            ColumnValues::Uint2(container) => ColumnValues::Uint2(container.take(num)),
            ColumnValues::Uint4(container) => ColumnValues::Uint4(container.take(num)),
            ColumnValues::Uint8(container) => ColumnValues::Uint8(container.take(num)),
            ColumnValues::Uint16(container) => ColumnValues::Uint16(container.take(num)),
            ColumnValues::Date(container) => ColumnValues::Date(container.take(num)),
            ColumnValues::DateTime(container) => ColumnValues::DateTime(container.take(num)),
            ColumnValues::Time(container) => ColumnValues::Time(container.take(num)),
            ColumnValues::Interval(container) => ColumnValues::Interval(container.take(num)),
            ColumnValues::Undefined(container) => ColumnValues::Undefined(container.take(num)),
            ColumnValues::RowId(container) => ColumnValues::RowId(container.take(num)),
            ColumnValues::Uuid4(container) => ColumnValues::Uuid4(container.take(num)),
            ColumnValues::Uuid7(container) => ColumnValues::Uuid7(container.take(num)),
            ColumnValues::Blob(container) => ColumnValues::Blob(container.take(num)),
        }
    }
}
