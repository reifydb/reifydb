// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::frame::Frame;
use crate::column::layout::{EngineColumnLayout, FrameLayout};
use crate::column::{EngineColumn, EngineColumnData, TableQualified};
use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
use reifydb_core::EncodedKey;
use reifydb_core::EncodedKeyRange;
use reifydb_core::interface::{EncodableKey, Table, TableRowKey};
use reifydb_core::interface::{EncodableKeyRange, Rx, TableRowKeyRange};
use reifydb_core::row::Layout;
use reifydb_core::value::row_id::ROW_ID_COLUMN_NAME;
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
        let data = table.columns.iter().map(|c| c.ty).collect::<Vec<_>>();
        let row_layout = Layout::new(&data);

        let layout = FrameLayout {
            columns: table
                .columns
                .iter()
                .map(|col| EngineColumnLayout { schema: None, table: None, name: col.name.clone() })
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

        let mut frame = Frame::empty_from_table(&self.table);
        frame.append_rows(&self.row_layout, batch_rows.into_iter())?;

        // Add the RowId column to the frame if requested
        if ctx.preserve_row_ids {
            let row_id_column = EngineColumn::TableQualified(TableQualified {
                table: self.table.name.clone(),
                name: ROW_ID_COLUMN_NAME.to_string(),
                data: EngineColumnData::row_id(row_ids),
            });
            frame.columns.push(row_id_column);
            frame.index.insert(ROW_ID_COLUMN_NAME.to_string(), frame.columns.len() - 1);
        }

        Ok(Some(Batch { frame }))
    }

    fn layout(&self) -> Option<FrameLayout> {
        Some(self.layout.clone())
    }
}
