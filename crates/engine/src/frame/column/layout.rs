// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, FrameColumn};
use reifydb_core::Type;

#[derive(Debug, Clone)]
pub struct FrameColumnLayout {
    pub name: String,
    pub ty: Type,
}

impl FrameColumnLayout {
    pub fn from_column(column: &FrameColumn) -> Self {
        Self {
            name: column.name.clone(),
            ty: match column.values {
                ColumnValues::Bool(_, _) => Type::Bool,
                ColumnValues::Float4(_, _) => Type::Float4,
                ColumnValues::Float8(_, _) => Type::Float8,
                ColumnValues::Int1(_, _) => Type::Int1,
                ColumnValues::Int2(_, _) => Type::Int2,
                ColumnValues::Int4(_, _) => Type::Int4,
                ColumnValues::Int8(_, _) => Type::Int8,
                ColumnValues::Int16(_, _) => Type::Int16,
                ColumnValues::Utf8(_, _) => Type::Utf8,
                ColumnValues::Uint1(_, _) => Type::Uint1,
                ColumnValues::Uint2(_, _) => Type::Uint2,
                ColumnValues::Uint4(_, _) => Type::Uint4,
                ColumnValues::Uint8(_, _) => Type::Uint8,
                ColumnValues::Uint16(_, _) => Type::Uint16,
                ColumnValues::Date(_, _) => Type::Date,
                ColumnValues::DateTime(_, _) => Type::DateTime,
                ColumnValues::Time(_, _) => Type::Time,
                ColumnValues::Interval(_, _) => Type::Interval,
                ColumnValues::Undefined(_) => Type::Undefined,
            },
        }
    }
}
