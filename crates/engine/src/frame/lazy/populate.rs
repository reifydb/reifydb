// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::lazy::Source;
use crate::frame::{Column, ColumnValues, Frame, LazyFrame};
use reifydb_catalog::Catalog;
use reifydb_catalog::key::TableRowKey;
use reifydb_core::Kind;
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
                    Kind::Bool => ColumnValues::bool(vec![]),
                    Kind::Float4 => ColumnValues::float4(vec![]),
                    Kind::Float8 => ColumnValues::float8(vec![]),
                    Kind::Int1 => ColumnValues::int1(vec![]),
                    Kind::Int2 => ColumnValues::int2(vec![]),
                    Kind::Int4 => ColumnValues::int4(vec![]),
                    Kind::Int8 => ColumnValues::int8(vec![]),
                    Kind::Int16 => ColumnValues::int16(vec![]),
                    Kind::Text => ColumnValues::string(vec![]),
                    Kind::Uint1 => ColumnValues::uint1(vec![]),
                    Kind::Uint2 => ColumnValues::uint2(vec![]),
                    Kind::Uint4 => ColumnValues::uint4(vec![]),
                    Kind::Uint8 => ColumnValues::uint8(vec![]),
                    Kind::Uint16 => ColumnValues::uint16(vec![]),
                    Kind::Undefined => ColumnValues::Undefined(0),
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
