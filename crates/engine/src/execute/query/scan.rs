// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
use reifydb_core::EncodedKeyRange;
use reifydb_core::Type;
use reifydb_core::frame::{
    ColumnValues, Frame, FrameColumn, FrameColumnLayout, FrameLayout, TableQualified,
};
use reifydb_core::interface::table::Table;
use reifydb_core::interface::{EncodableKey, TableRowKey};
use reifydb_core::interface::{EncodableKeyRange, Rx, TableRowKeyRange};
use reifydb_core::row::Layout;
use reifydb_core::value::row_id::ROW_ID_COLUMN_NAME;
use reifydb_core::{BitVec, EncodedKey};
use std::ops::Bound::{Excluded, Included};
use std::sync::Arc;

pub(crate) struct ScanFrameNode {
    table: Table,
    context: Arc<ExecutionContext>,
    layout: FrameLayout,
    row_layout: Layout,
    last_key: Option<EncodedKey>,
    exhausted: bool,
}

impl ScanFrameNode {
    pub fn new(table: Table, context: Arc<ExecutionContext>) -> crate::Result<Self> {
        let values = table.columns.iter().map(|c| c.ty).collect::<Vec<_>>();
        let row_layout = Layout::new(&values);

        let layout = FrameLayout {
            columns: table
                .columns
                .iter()
                .map(|col| FrameColumnLayout { schema: None, table: None, name: col.name.clone() })
                .collect(),
        };

        Ok(Self { table, context, layout, row_layout, last_key: None, exhausted: false })
    }
}

impl ExecutionPlan for ScanFrameNode {
    fn next(&mut self, ctx: &ExecutionContext, rx: &mut dyn Rx) -> crate::Result<Option<Batch>> {
        if self.exhausted {
            return Ok(None);
        }

        let batch_size = self.context.batch_size;
        let range = TableRowKeyRange { table: self.table.id };

        let range = if let Some(_) = &self.last_key {
            EncodedKeyRange::new(
                Excluded(self.last_key.clone().unwrap()),
                Included(range.end().unwrap()),
            )
        } else {
            EncodedKeyRange::new(Included(range.start().unwrap()), Included(range.end().unwrap()))
        };

        let mut batch_rows = Vec::new();
        let mut row_ids = Vec::new();
        let mut rows_collected = 0;
        let mut new_last_key = None;

        let versioned_rows: Vec<_> = rx.scan_range(range)?.into_iter().collect();

        for versioned in versioned_rows.into_iter() {
            if let Some(key) = TableRowKey::decode(&versioned.key) {
                batch_rows.push(versioned.row);
                row_ids.push(key.row);
                new_last_key = Some(versioned.key);
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
        if ctx.preserve_row_ids {
            let row_id_column = FrameColumn::TableQualified(TableQualified {
                table: self.table.name.clone(),
                name: ROW_ID_COLUMN_NAME.to_string(),
                values: ColumnValues::row_id(row_ids),
            });
            frame.columns.push(row_id_column);
            frame.index.insert(ROW_ID_COLUMN_NAME.to_string(), frame.columns.len() - 1);
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
            FrameColumn::TableQualified(TableQualified {
                table: table.name.clone(),
                name,
                values: data,
            })
        })
        .collect();

    Frame::new_with_name(columns, table.name.clone())
}
