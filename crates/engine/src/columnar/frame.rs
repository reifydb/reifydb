// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::Columns;
use crate::columnar::{Column, ColumnData};
use reifydb_core::{Frame, FrameColumn, FrameColumnData};

impl From<ColumnData> for FrameColumnData {
    fn from(value: ColumnData) -> Self {
        match value {
            ColumnData::Bool(container) => FrameColumnData::Bool(container),
            ColumnData::Float4(container) => FrameColumnData::Float4(container),
            ColumnData::Float8(container) => FrameColumnData::Float8(container),
            ColumnData::Int1(container) => FrameColumnData::Int1(container),
            ColumnData::Int2(container) => FrameColumnData::Int2(container),
            ColumnData::Int4(container) => FrameColumnData::Int4(container),
            ColumnData::Int8(container) => FrameColumnData::Int8(container),
            ColumnData::Int16(container) => FrameColumnData::Int16(container),
            ColumnData::Uint1(container) => FrameColumnData::Uint1(container),
            ColumnData::Uint2(container) => FrameColumnData::Uint2(container),
            ColumnData::Uint4(container) => FrameColumnData::Uint4(container),
            ColumnData::Uint8(container) => FrameColumnData::Uint8(container),
            ColumnData::Uint16(container) => FrameColumnData::Uint16(container),
            ColumnData::Utf8(container) => FrameColumnData::Utf8(container),
            ColumnData::Date(container) => FrameColumnData::Date(container),
            ColumnData::DateTime(container) => FrameColumnData::DateTime(container),
            ColumnData::Time(container) => FrameColumnData::Time(container),
            ColumnData::Interval(container) => FrameColumnData::Interval(container),
            ColumnData::RowId(container) => FrameColumnData::RowId(container),
            ColumnData::Uuid4(container) => FrameColumnData::Uuid4(container),
            ColumnData::Uuid7(container) => FrameColumnData::Uuid7(container),
            ColumnData::Blob(container) => FrameColumnData::Blob(container),
            ColumnData::Undefined(container) => FrameColumnData::Undefined(container),
        }
    }
}

impl From<Column> for FrameColumn {
    fn from(value: Column) -> Self {
        match value {
            Column::FullyQualified(col) => FrameColumn {
                schema: Some(col.schema),
                table: Some(col.table),
                name: col.name,
                data: col.data.into(),
            },
            Column::TableQualified(col) => FrameColumn {
                schema: None,
                table: Some(col.table),
                name: col.name,
                data: col.data.into(),
            },
            Column::ColumnQualified(col) => {
                FrameColumn { schema: None, table: None, name: col.name, data: col.data.into() }
            }
            Column::Unqualified(col) => {
                FrameColumn { schema: None, table: None, name: col.name, data: col.data.into() }
            }
        }
    }
}

impl From<Columns> for Frame {
    fn from(columns: Columns) -> Self {
        Self::new(columns.into_iter().map(|col| col.into()).collect())
    }
}
