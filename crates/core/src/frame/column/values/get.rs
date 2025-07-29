// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Value;
use crate::frame::ColumnValues;

impl ColumnValues {
    pub fn get_value(&self, index: usize) -> Value {
        match self {
            ColumnValues::Bool(container) => container.get_value(index),
            ColumnValues::Float4(container) => container.get_value(index),
            ColumnValues::Float8(container) => container.get_value(index),
            ColumnValues::Int1(container) => container.get_value(index),
            ColumnValues::Int2(container) => container.get_value(index),
            ColumnValues::Int4(container) => container.get_value(index),
            ColumnValues::Int8(container) => container.get_value(index),
            ColumnValues::Int16(container) => container.get_value(index),
            ColumnValues::Uint1(container) => container.get_value(index),
            ColumnValues::Uint2(container) => container.get_value(index),
            ColumnValues::Uint4(container) => container.get_value(index),
            ColumnValues::Uint8(container) => container.get_value(index),
            ColumnValues::Uint16(container) => container.get_value(index),
            ColumnValues::Utf8(container) => container.get_value(index),
            ColumnValues::Date(container) => container.get_value(index),
            ColumnValues::DateTime(container) => container.get_value(index),
            ColumnValues::Time(container) => container.get_value(index),
            ColumnValues::Interval(container) => container.get_value(index),
            ColumnValues::RowId(container) => container.get_value(index),
            ColumnValues::Uuid4(container) => container.get_value(index),
            ColumnValues::Uuid7(container) => container.get_value(index),
            ColumnValues::Blob(container) => container.get_value(index),
            ColumnValues::Undefined(container) => container.get_value(index),
        }
    }
}
