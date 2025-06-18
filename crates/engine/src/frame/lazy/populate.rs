// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::lazy::Source;
use crate::frame::{Column, ColumnValues, Frame, LazyFrame};
use reifydb_catalog::Catalog;
use reifydb_catalog::key::TableRowKey;
use reifydb_core::ValueKind;
use reifydb_core::row::Layout;
use reifydb_transaction::Rx;

impl LazyFrame {

    pub(crate) fn populate_frame(&mut self, rx: &mut impl Rx) -> crate::frame::Result<()> {
        let table = match &self.source {
            Source::Table { schema, table } => {
                let schema = Catalog::get_schema_by_name(rx, &schema).unwrap().unwrap(); // FIXME
                Catalog::get_table_by_name(rx, schema.id, &table).unwrap().unwrap() // FIXME
            }
            Source::None => unreachable!(),
        };

        let columns = table.columns;

        let values = columns.iter().map(|c| c.value).collect::<Vec<_>>();
        let layout = Layout::new(&values);

        let columns: Vec<Column> = columns
            .iter()
            .map(|col| {
                let name = col.name.clone();
                let data = match col.value {
                    ValueKind::Bool => ColumnValues::bool(vec![]),
                    ValueKind::Float4 => ColumnValues::float4(vec![]),
                    ValueKind::Float8 => ColumnValues::float8(vec![]),
                    ValueKind::Int1 => ColumnValues::int1(vec![]),
                    ValueKind::Int2 => ColumnValues::int2(vec![]),
                    ValueKind::Int4 => ColumnValues::int4(vec![]),
                    ValueKind::Int8 => ColumnValues::int8(vec![]),
                    ValueKind::Int16 => ColumnValues::int16(vec![]),
                    ValueKind::String => ColumnValues::string(vec![]),
                    ValueKind::Uint1 => ColumnValues::uint1(vec![]),
                    ValueKind::Uint2 => ColumnValues::uint2(vec![]),
                    ValueKind::Uint4 => ColumnValues::uint4(vec![]),
                    ValueKind::Uint8 => ColumnValues::uint8(vec![]),
                    ValueKind::Uint16 => ColumnValues::uint16(vec![]),
                    ValueKind::Undefined => ColumnValues::Undefined(0),
                };
                Column { name, data }
            })
            .collect();

        self.frame = Frame::new(columns);

        self.frame
            .append_rows(
                &layout,
                rx.scan_range(TableRowKey::full_scan(table.id))
                    .unwrap()
                    .into_iter()
                    .map(|versioned| versioned.row),
            )
            .unwrap();

        Ok(())
    }
}
