// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::ColumnData;
use reifydb_core::Value;

impl ColumnData {
    pub fn get_value(&self, index: usize) -> Value {
        match self {
            ColumnData::Bool(container) => container.get_value(index),
            ColumnData::Float4(container) => container.get_value(index),
            ColumnData::Float8(container) => container.get_value(index),
            ColumnData::Int1(container) => container.get_value(index),
            ColumnData::Int2(container) => container.get_value(index),
            ColumnData::Int4(container) => container.get_value(index),
            ColumnData::Int8(container) => container.get_value(index),
            ColumnData::Int16(container) => container.get_value(index),
            ColumnData::Uint1(container) => container.get_value(index),
            ColumnData::Uint2(container) => container.get_value(index),
            ColumnData::Uint4(container) => container.get_value(index),
            ColumnData::Uint8(container) => container.get_value(index),
            ColumnData::Uint16(container) => container.get_value(index),
            ColumnData::Utf8(container) => container.get_value(index),
            ColumnData::Date(container) => container.get_value(index),
            ColumnData::DateTime(container) => container.get_value(index),
            ColumnData::Time(container) => container.get_value(index),
            ColumnData::Interval(container) => container.get_value(index),
            ColumnData::RowId(container) => container.get_value(index),
            ColumnData::Uuid4(container) => container.get_value(index),
            ColumnData::Uuid7(container) => container.get_value(index),
            ColumnData::Blob(container) => container.get_value(index),
            ColumnData::Undefined(container) => container.get_value(index),
        }
    }
}
