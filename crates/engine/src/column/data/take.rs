// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::EngineColumnData;

impl EngineColumnData {
    pub fn take(&self, num: usize) -> EngineColumnData {
        match self {
            EngineColumnData::Bool(container) => EngineColumnData::Bool(container.take(num)),
            EngineColumnData::Float4(container) => EngineColumnData::Float4(container.take(num)),
            EngineColumnData::Float8(container) => EngineColumnData::Float8(container.take(num)),
            EngineColumnData::Int1(container) => EngineColumnData::Int1(container.take(num)),
            EngineColumnData::Int2(container) => EngineColumnData::Int2(container.take(num)),
            EngineColumnData::Int4(container) => EngineColumnData::Int4(container.take(num)),
            EngineColumnData::Int8(container) => EngineColumnData::Int8(container.take(num)),
            EngineColumnData::Int16(container) => EngineColumnData::Int16(container.take(num)),
            EngineColumnData::Utf8(container) => EngineColumnData::Utf8(container.take(num)),
            EngineColumnData::Uint1(container) => EngineColumnData::Uint1(container.take(num)),
            EngineColumnData::Uint2(container) => EngineColumnData::Uint2(container.take(num)),
            EngineColumnData::Uint4(container) => EngineColumnData::Uint4(container.take(num)),
            EngineColumnData::Uint8(container) => EngineColumnData::Uint8(container.take(num)),
            EngineColumnData::Uint16(container) => EngineColumnData::Uint16(container.take(num)),
            EngineColumnData::Date(container) => EngineColumnData::Date(container.take(num)),
            EngineColumnData::DateTime(container) => EngineColumnData::DateTime(container.take(num)),
            EngineColumnData::Time(container) => EngineColumnData::Time(container.take(num)),
            EngineColumnData::Interval(container) => EngineColumnData::Interval(container.take(num)),
            EngineColumnData::Undefined(container) => EngineColumnData::Undefined(container.take(num)),
            EngineColumnData::RowId(container) => EngineColumnData::RowId(container.take(num)),
            EngineColumnData::Uuid4(container) => EngineColumnData::Uuid4(container.take(num)),
            EngineColumnData::Uuid7(container) => EngineColumnData::Uuid7(container.take(num)),
            EngineColumnData::Blob(container) => EngineColumnData::Blob(container.take(num)),
        }
    }
}
