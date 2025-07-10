// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{Column, ColumnValues};
use reifydb_core::DataType;

#[derive(Debug, Clone)]
pub struct ColumnLayout {
    pub name: String,
    pub data_type: DataType,
}

impl ColumnLayout {
    pub fn from_column(column: &Column) -> Self {
        Self {
            name: column.name.clone(),
            data_type: match column.values {
                ColumnValues::Bool(_, _) => DataType::Bool,
                ColumnValues::Float4(_, _) => DataType::Float4,
                ColumnValues::Float8(_, _) => DataType::Float8,
                ColumnValues::Int1(_, _) => DataType::Int1,
                ColumnValues::Int2(_, _) => DataType::Int2,
                ColumnValues::Int4(_, _) => DataType::Int4,
                ColumnValues::Int8(_, _) => DataType::Int8,
                ColumnValues::Int16(_, _) => DataType::Int16,
                ColumnValues::Utf8(_, _) => DataType::Utf8,
                ColumnValues::Uint1(_, _) => DataType::Uint1,
                ColumnValues::Uint2(_, _) => DataType::Uint2,
                ColumnValues::Uint4(_, _) => DataType::Uint4,
                ColumnValues::Uint8(_, _) => DataType::Uint8,
                ColumnValues::Uint16(_, _) => DataType::Uint16,
                ColumnValues::Undefined(_) => DataType::Undefined,
            },
        }
    }
}
