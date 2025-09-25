// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, sync::Arc};

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	interface::{
		EncodableKey, MultiVersionQueryTransaction, RingBufferMetadata, RowKey, Transaction,
		resolved::ResolvedRingBuffer,
	},
	value::{
		column::{
			Column, ColumnData, Columns,
			layout::{ColumnLayout, ColumnsLayout},
		},
		row::EncodedRowLayout,
	},
};
use reifydb_type::{Fragment, ROW_NUMBER_COLUMN_NAME, RowNumber, Type};

use crate::{
	StandardTransaction,
	execute::{Batch, ExecutionContext, QueryNode},
};

pub struct RingBufferScan<'a, T: Transaction> {
	ring_buffer: ResolvedRingBuffer<'a>,
	metadata: Option<RingBufferMetadata>,
	layout: ColumnsLayout<'a>,
	row_layout: EncodedRowLayout,
	current_position: u64,
	rows_returned: u64,
	context: Option<Arc<ExecutionContext<'a>>>,
	initialized: bool,
	_phantom: PhantomData<T>,
}

impl<'a, T: Transaction> RingBufferScan<'a, T> {
	pub fn new(ring_buffer: ResolvedRingBuffer<'a>, context: Arc<ExecutionContext<'a>>) -> crate::Result<Self> {
		// Create row layout based on column types
		let types: Vec<Type> = ring_buffer.columns().iter().map(|c| c.constraint.get_type()).collect();
		let row_layout = EncodedRowLayout::new(&types);

		// Create columns layout
		let layout = ColumnsLayout {
			columns: ring_buffer
				.columns()
				.iter()
				.map(|col| ColumnLayout {
					namespace: None,
					source: None,
					name: Fragment::owned_internal(&col.name),
				})
				.collect(),
		};

		Ok(Self {
			ring_buffer,
			metadata: None,
			layout,
			row_layout,
			current_position: 0,
			rows_returned: 0,
			context: Some(context),
			initialized: false,
			_phantom: PhantomData,
		})
	}
}

impl<'a, T: Transaction> QueryNode<'a, T> for RingBufferScan<'a, T> {
	fn initialize(
		&mut self,
		txn: &mut StandardTransaction<'a, T>,
		_ctx: &ExecutionContext<'a>,
	) -> crate::Result<()> {
		if !self.initialized {
			// Get ring buffer metadata from the appropriate transaction type
			let metadata = match txn {
				crate::StandardTransaction::Command(cmd_txn) => {
					CatalogStore::find_ring_buffer_metadata(*cmd_txn, self.ring_buffer.def().id)?
				}
				crate::StandardTransaction::Query(query_txn) => {
					CatalogStore::find_ring_buffer_metadata(*query_txn, self.ring_buffer.def().id)?
				}
			};
			self.metadata = metadata;

			if let Some(ref metadata) = self.metadata {
				// Start scanning from head (oldest entry) if buffer has data
				self.current_position = if metadata.is_empty() {
					0
				} else {
					metadata.head
				};
			}

			self.initialized = true;
		}
		Ok(())
	}

	fn next(&mut self, txn: &mut StandardTransaction<'a, T>) -> crate::Result<Option<Batch<'a>>> {
		let ctx = self.context.as_ref().expect("RingBufferScan context not set");

		// Get metadata or return empty
		let metadata = match &self.metadata {
			Some(m) => m,
			None => return Ok(None),
		};

		// If we've returned all rows, we're done
		if self.rows_returned >= metadata.count {
			return Ok(None);
		}

		let batch_size = ctx.batch_size;

		// Collect rows for this batch
		let mut batch_rows = Vec::new();
		let mut row_numbers = Vec::new();
		let mut batch_count = 0;

		// Read up to batch_size rows
		while batch_count < batch_size && self.rows_returned < metadata.count {
			// Compute the actual row number to read
			let row_num = RowNumber(self.current_position);

			// Create the row key
			let key = RowKey {
				source: self.ring_buffer.def().id.into(),
				row: row_num,
			};

			// Get the row from storage
			if let Some(multi) = txn.get(&key.encode())? {
				let row_data = multi.row;
				batch_rows.push(row_data);
				row_numbers.push(row_num);
			}

			// Move to next position (circular)
			self.current_position = (self.current_position + 1) % metadata.capacity;
			self.rows_returned += 1;
			batch_count += 1;
		}

		if batch_rows.is_empty() {
			Ok(None)
		} else {
			let mut columns = Columns::from_ring_buffer(&self.ring_buffer);
			columns.append_rows(&self.row_layout, batch_rows.into_iter())?;

			if ctx.preserve_row_numbers {
				columns.0.push(Column {
					name: Fragment::owned_internal(ROW_NUMBER_COLUMN_NAME),
					data: ColumnData::row_number(row_numbers),
				});
			}

			Ok(Some(Batch {
				columns,
			}))
		}
	}

	fn layout(&self) -> Option<ColumnsLayout<'a>> {
		Some(self.layout.clone())
	}
}
