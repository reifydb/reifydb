// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::Columns;
use crate::columnar::layout::{ColumnLayout, ColumnsLayout};
use crate::columnar::{Column, ColumnData, TableQualified};
use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
use reifydb_core::EncodedKey;
use reifydb_core::EncodedKeyRange;
use reifydb_core::interface::{EncodableKey, Table, TableRowKey};
use reifydb_core::interface::{EncodableKeyRange, VersionedReadTransaction, TableRowKeyRange};
use reifydb_core::row::Layout;
use reifydb_core::value::row_id::ROW_ID_COLUMN_NAME;
use std::ops::Bound::{Excluded, Included};
use std::sync::Arc;

pub(crate) struct ScanColumnsNode {
    table: Table,
    context: Arc<ExecutionContext>,
    layout: ColumnsLayout,
    row_layout: Layout,
    last_key: Option<EncodedKey>,
    exhausted: bool,
}

impl ScanColumnsNode {
    pub fn new(table: Table, context: Arc<ExecutionContext>) -> crate::Result<Self> {
        let data = table.columns.iter().map(|c| c.ty).collect::<Vec<_>>();
        let row_layout = Layout::new(&data);

        let layout = ColumnsLayout {
            columns: table
                .columns
                .iter()
                .map(|col| ColumnLayout { schema: None, table: None, name: col.name.clone() })
                .collect(),
        };

        Ok(Self { table, context, layout, row_layout, last_key: None, exhausted: false })
    }
}

impl ExecutionPlan for ScanColumnsNode {
    fn next(&mut self, ctx: &ExecutionContext, rx: &mut dyn VersionedReadTransaction) -> crate::Result<Option<Batch>> {
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

        let versioned_rows: Vec<_> = rx.range(range)?.into_iter().collect();

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

        let mut columns = Columns::empty_from_table(&self.table);
        columns.append_rows(&self.row_layout, batch_rows.into_iter())?;

        // Add the RowId column to the columns if requested
        if ctx.preserve_row_ids {
            let row_id_column = Column::TableQualified(TableQualified {
                table: self.table.name.clone(),
                name: ROW_ID_COLUMN_NAME.to_string(),
                data: ColumnData::row_id(row_ids),
            });
            columns.0.push(row_id_column);
        }

        Ok(Some(Batch { columns }))
    }

    fn layout(&self) -> Option<ColumnsLayout> {
        Some(self.layout.clone())
    }
}
