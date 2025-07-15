// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
use crate::frame::{ColumnValues, Frame, FrameColumn, FrameLayout};
use reifydb_catalog::key::{EncodableKey, Key, TableRowKey};
use reifydb_catalog::table::Table;
use reifydb_core::BitVec;
use reifydb_core::DataType;
use reifydb_core::EncodedKeyRange;
use reifydb_core::interface::Rx;
use reifydb_core::row::Layout;
use std::sync::Arc;

pub(crate) struct ScanFrameNode {
    table: Table,
    context: Arc<ExecutionContext>,
    layout: FrameLayout,
    row_layout: Layout,
    last_key: Option<TableRowKey>,
    exhausted: bool,
}

impl ScanFrameNode {
    pub fn new(table: Table, context: Arc<ExecutionContext>) -> crate::Result<Self> {
        let values = table.columns.iter().map(|c| c.data_type).collect::<Vec<_>>();
        let row_layout = Layout::new(&values);

        let frame = create_empty_frame(&table);

        Ok(Self {
            table,
            context,
            layout: FrameLayout::from_frame(&frame),
            row_layout,
            last_key: None,
            exhausted: false,
        })
    }
}

impl ExecutionPlan for ScanFrameNode {
    fn next(&mut self, rx: &mut dyn Rx) -> crate::Result<Option<Batch>> {
        if self.exhausted {
            return Ok(None);
        }

        let batch_size = self.context.batch_size;

        let range = if let Some(last_key) = &self.last_key {
            let start_key = last_key.encode();
            let end_key = TableRowKey::table_end(self.table.id);
            EncodedKeyRange::start_end(Some(start_key), Some(end_key))
        } else {
            TableRowKey::full_scan(self.table.id)
        };

        let mut batch_rows = Vec::new();
        let mut rows_collected = 0;
        let mut new_last_key = None;

        let versioned_rows: Vec<_> = rx.scan_range(range)?.into_iter().collect();

        for versioned in versioned_rows.into_iter() {
            // Skip the first row if it matches our last_key (to avoid duplicates)
            if let Some(last_key) = &self.last_key {
                if Key::decode(&versioned.key).and_then(|k| match k {
                    Key::TableRow(tr_key) => Some(tr_key),
                    _ => None,
                }) == Some(last_key.clone())
                {
                    continue;
                }
            }

            batch_rows.push(versioned.row);
            new_last_key = Key::decode(&versioned.key).and_then(|k| match k {
                Key::TableRow(tr_key) => Some(tr_key),
                _ => None,
            });
            rows_collected += 1;

            if rows_collected >= batch_size {
                break;
            }
        }

        if batch_rows.is_empty() {
            self.exhausted = true;
            return Ok(None);
        }

        self.last_key = new_last_key;

        let mut frame = create_empty_frame(&self.table);
        frame.append_rows(&self.row_layout, batch_rows.into_iter())?;

        let mask = BitVec::new(frame.row_count(), true);
        Ok(Some(Batch { frame, mask }))
    }

    fn layout(&self) -> Option<FrameLayout> {
        Some(self.layout.clone())
    }
}

fn create_empty_frame(table: &Table) -> Frame {
    let columns: Vec<FrameColumn> = table
        .columns
        .iter()
        .map(|col| {
            let name = col.name.clone();
            let data = match col.data_type {
                DataType::Bool => ColumnValues::bool(vec![]),
                DataType::Float4 => ColumnValues::float4(vec![]),
                DataType::Float8 => ColumnValues::float8(vec![]),
                DataType::Int1 => ColumnValues::int1(vec![]),
                DataType::Int2 => ColumnValues::int2(vec![]),
                DataType::Int4 => ColumnValues::int4(vec![]),
                DataType::Int8 => ColumnValues::int8(vec![]),
                DataType::Int16 => ColumnValues::int16(vec![]),
                DataType::Utf8 => ColumnValues::utf8(vec![]),
                DataType::Uint1 => ColumnValues::uint1(vec![]),
                DataType::Uint2 => ColumnValues::uint2(vec![]),
                DataType::Uint4 => ColumnValues::uint4(vec![]),
                DataType::Uint8 => ColumnValues::uint8(vec![]),
                DataType::Uint16 => ColumnValues::uint16(vec![]),
                DataType::Undefined => ColumnValues::Undefined(0),
            };
            FrameColumn { name, values: data }
        })
        .collect();

    Frame::new_with_name(columns, table.name.clone())
}
