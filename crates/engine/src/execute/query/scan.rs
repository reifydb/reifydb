// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
use reifydb_catalog::table::Table;
use reifydb_core::BitVec;
use reifydb_core::EncodedKeyRange;
use reifydb_core::Type;
use reifydb_core::frame::{ColumnValues, Frame, FrameColumn, FrameLayout};
use reifydb_core::interface::Rx;
use reifydb_core::interface::{EncodableKey, Key, TableRowKey};
use reifydb_core::row::Layout;
use reifydb_core::value::row_id::ROW_ID_COLUMN_NAME;
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
        let values = table.columns.iter().map(|c| c.ty).collect::<Vec<_>>();
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
    fn next(&mut self, ctx: &ExecutionContext, rx: &mut dyn Rx) -> crate::Result<Option<Batch>> {
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
        let mut row_ids = if ctx.preserve_row_ids { Some(Vec::new()) } else { None };
        let mut rows_collected = 0;
        let mut new_last_key = None;

        let versioned_rows: Vec<_> = rx.scan_range(range)?.into_iter().collect();

        for versioned in versioned_rows.into_iter() {
            // Decode the table row key to extract the RowId
            let table_row_key = Key::decode(&versioned.key).and_then(|k| match k {
                Key::TableRow(tr_key) => Some(tr_key),
                _ => None,
            });

            // Skip the first row if it matches our last_key (to avoid duplicates)
            if let Some(last_key) = &self.last_key {
                if table_row_key == Some(last_key.clone()) {
                    continue;
                }
            }

            if let Some(tr_key) = &table_row_key {
                batch_rows.push(versioned.row);
                if let Some(ref mut row_ids_vec) = row_ids {
                    row_ids_vec.push(tr_key.row);
                }
                new_last_key = table_row_key;
                rows_collected += 1;

                if rows_collected >= batch_size {
                    break;
                }
            }
        }

        if batch_rows.is_empty() {
            self.exhausted = true;
            return Ok(None);
        }

        self.last_key = new_last_key;

        let mut frame = create_empty_frame(&self.table);
        frame.append_rows(&self.row_layout, batch_rows.into_iter())?;

        // Add the RowId column to the frame if requested
        if let Some(row_ids_vec) = row_ids {
            if !row_ids_vec.is_empty() {
                let row_id_column = FrameColumn::new(
                    Some(self.table.name.clone()),
                    ROW_ID_COLUMN_NAME.to_string(),
                    ColumnValues::row_id(row_ids_vec),
                );
                frame.columns.push(row_id_column);
                frame.index.insert(ROW_ID_COLUMN_NAME.to_string(), frame.columns.len() - 1);
            }
        }

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
            let data = match col.ty {
                Type::Bool => ColumnValues::bool(vec![]),
                Type::Float4 => ColumnValues::float4(vec![]),
                Type::Float8 => ColumnValues::float8(vec![]),
                Type::Int1 => ColumnValues::int1(vec![]),
                Type::Int2 => ColumnValues::int2(vec![]),
                Type::Int4 => ColumnValues::int4(vec![]),
                Type::Int8 => ColumnValues::int8(vec![]),
                Type::Int16 => ColumnValues::int16(vec![]),
                Type::Utf8 => ColumnValues::utf8(vec![]),
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
                Type::Undefined => ColumnValues::Undefined(0),
            };
            FrameColumn::new(
                Some(table.name.clone()),
                name,
                data,
            )
        })
        .collect();

    Frame::new_with_name(columns, table.name.clone())
}
