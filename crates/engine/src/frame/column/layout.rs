// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{Column, ColumnValues};
use reifydb_core::Kind;

#[derive(Debug, Clone)]
pub struct ColumnLayout {
    pub name: String,
    pub kind: Kind,
}

impl ColumnLayout {
    pub fn from_column(column: &Column) -> Self {
        Self {
            name: column.name.clone(),
            kind: match column.values {
                ColumnValues::Bool(_, _) => Kind::Bool,
                ColumnValues::Float4(_, _) => Kind::Float4,
                ColumnValues::Float8(_, _) => Kind::Float8,
                ColumnValues::Int1(_, _) => Kind::Int1,
                ColumnValues::Int2(_, _) => Kind::Int2,
                ColumnValues::Int4(_, _) => Kind::Int4,
                ColumnValues::Int8(_, _) => Kind::Int8,
                ColumnValues::Int16(_, _) => Kind::Int16,
                ColumnValues::String(_, _) => Kind::Text,
                ColumnValues::Uint1(_, _) => Kind::Uint1,
                ColumnValues::Uint2(_, _) => Kind::Uint2,
                ColumnValues::Uint4(_, _) => Kind::Uint4,
                ColumnValues::Uint8(_, _) => Kind::Uint8,
                ColumnValues::Uint16(_, _) => Kind::Uint16,
                ColumnValues::Undefined(_) => Kind::Undefined,
            },
        }
    }
}
