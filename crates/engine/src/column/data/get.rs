// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::Value;
use crate::column::EngineColumnData;

impl EngineColumnData {
    pub fn get_value(&self, index: usize) -> Value {
        match self {
            EngineColumnData::Bool(container) => container.get_value(index),
            EngineColumnData::Float4(container) => container.get_value(index),
            EngineColumnData::Float8(container) => container.get_value(index),
            EngineColumnData::Int1(container) => container.get_value(index),
            EngineColumnData::Int2(container) => container.get_value(index),
            EngineColumnData::Int4(container) => container.get_value(index),
            EngineColumnData::Int8(container) => container.get_value(index),
            EngineColumnData::Int16(container) => container.get_value(index),
            EngineColumnData::Uint1(container) => container.get_value(index),
            EngineColumnData::Uint2(container) => container.get_value(index),
            EngineColumnData::Uint4(container) => container.get_value(index),
            EngineColumnData::Uint8(container) => container.get_value(index),
            EngineColumnData::Uint16(container) => container.get_value(index),
            EngineColumnData::Utf8(container) => container.get_value(index),
            EngineColumnData::Date(container) => container.get_value(index),
            EngineColumnData::DateTime(container) => container.get_value(index),
            EngineColumnData::Time(container) => container.get_value(index),
            EngineColumnData::Interval(container) => container.get_value(index),
            EngineColumnData::RowId(container) => container.get_value(index),
            EngineColumnData::Uuid4(container) => container.get_value(index),
            EngineColumnData::Uuid7(container) => container.get_value(index),
            EngineColumnData::Blob(container) => container.get_value(index),
            EngineColumnData::Undefined(container) => container.get_value(index),
        }
    }
}
