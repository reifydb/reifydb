// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
use crate::frame::{ColumnValues, Frame, FrameColumn, FrameLayout};
use reifydb_catalog::key::TableRowKey;
use reifydb_catalog::row::RowId;
use reifydb_catalog::table::Table;
use reifydb_core::BitVec;
use reifydb_core::DataType;
use reifydb_core::interface::Rx;
use reifydb_core::row::{EncodedRow, Layout};
use std::sync::Arc;

pub(crate) struct ScanFrameNode {
    table: Table,
    context: Arc<ExecutionContext>,
    layout: Option<FrameLayout>,
    row_layout: Layout,
    last_row_id: Option<RowId>,
    exhausted: bool,
}

impl ScanFrameNode {
    pub fn new(
        table: Table,
        context: Arc<ExecutionContext>,
        _rx: &mut impl Rx,
    ) -> crate::Result<Self> {
        let values = table.columns.iter().map(|c| c.data_type).collect::<Vec<_>>();
        let row_layout = Layout::new(&values);

        Ok(Self {
            table,
            context,
            layout: None,
            row_layout,
            last_row_id: None,
            exhausted: false,
        })
    }

    fn create_empty_frame(&self) -> Frame {
        let columns: Vec<FrameColumn> = self
            .table
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

        Frame::new_with_name(columns, self.table.name.clone())
    }
}

impl ExecutionPlan for ScanFrameNode {
    fn next(&mut self, rx: &mut dyn Rx) -> crate::Result<Option<Batch>> {
        if self.exhausted {
            return Ok(None);
        }

        let batch_size = self.context.batch_size;
        
        // Create range query starting from last_row_id
        let start_key = if let Some(last_id) = self.last_row_id {
            TableRowKey { table: self.table.id, row: last_id }
        } else {
            TableRowKey { table: self.table.id, row: RowId(0) }
        };

        let mut rows_collected = 0;
        let mut batch_rows = Vec::new();
        let mut last_seen_id = self.last_row_id;

        // Scan from the cursor position and collect up to batch_size rows
        for versioned in rx.scan_range(TableRowKey::full_scan(self.table.id))? {
            // Skip rows until we reach our cursor position
            if let Some(last_id) = self.last_row_id {
                if versioned.row_key().row <= last_id {
                    continue;
                }
            }

            batch_rows.push(versioned.row);
            last_seen_id = Some(versioned.row_key().row);
            rows_collected += 1;

            if rows_collected >= batch_size {
                break;
            }
        }

        if batch_rows.is_empty() {
            self.exhausted = true;
            return Ok(None);
        }

        self.last_row_id = last_seen_id;

        let mut frame = self.create_empty_frame();
        frame.append_rows(&self.row_layout, batch_rows.into_iter())?;

        if self.layout.is_none() {
            self.layout = Some(FrameLayout::from_frame(&frame));
        }

        let mask = BitVec::new(frame.row_count(), true);
        Ok(Some(Batch { frame, mask }))
    }

    fn layout(&self) -> Option<FrameLayout> {
        self.layout.clone()
    }
}
