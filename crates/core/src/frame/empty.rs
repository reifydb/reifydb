// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Type;
use crate::frame::{ColumnValues, Frame, FrameColumn, TableQualified};
use crate::interface::Table;
use std::collections::HashMap;

impl Frame {
    pub fn empty() -> Self {
        Self {
            name: "frame".to_string(),
            columns: vec![],
            index: HashMap::new(),
            frame_index: HashMap::new(),
        }
    }

    pub fn empty_from_table(table: &Table) -> Self {
        let columns: Vec<FrameColumn> = table
            .columns
            .iter()
            .map(|col| {
                let name = col.name.clone();
                let data = match col.ty {
                    Type::Bool => ColumnValues::bool(vec![]),
                    Type::Float4 => ColumnValues::float4(vec![]),
                    Type::Float8 => ColumnValues::float8(vec![]),
                    Type::Int1 => ColumnValues::int1(vec![]),
                    Type::Int2 => ColumnValues::int2(vec![]),
                    Type::Int4 => ColumnValues::int4(vec![]),
                    Type::Int8 => ColumnValues::int8(vec![]),
                    Type::Int16 => ColumnValues::int16(vec![]),
                    Type::Utf8 => ColumnValues::utf8(Vec::<String>::new()),
                    Type::Uint1 => ColumnValues::uint1(vec![]),
                    Type::Uint2 => ColumnValues::uint2(vec![]),
                    Type::Uint4 => ColumnValues::uint4(vec![]),
                    Type::Uint8 => ColumnValues::uint8(vec![]),
                    Type::Uint16 => ColumnValues::uint16(vec![]),
                    Type::Date => ColumnValues::date(vec![]),
                    Type::DateTime => ColumnValues::datetime(vec![]),
                    Type::Time => ColumnValues::time(vec![]),
                    Type::Interval => ColumnValues::interval(vec![]),
                    Type::RowId => ColumnValues::row_id(vec![]),
                    Type::Uuid4 => ColumnValues::uuid4(vec![]),
                    Type::Uuid7 => ColumnValues::uuid7(vec![]),
                    Type::Blob => ColumnValues::blob(vec![]),
                    Type::Undefined => ColumnValues::undefined(0),
                };
                FrameColumn::TableQualified(TableQualified {
                    table: table.name.clone(),
                    name,
                    values: data,
                })
            })
            .collect();

        Self::new_with_name(columns, table.name.clone())
    }
}
