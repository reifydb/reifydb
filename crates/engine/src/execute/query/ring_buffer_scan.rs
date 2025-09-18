// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	interface::{EncodableKey, RingBufferDef, RingBufferMetadata, RowKey, Transaction, VersionedQueryTransaction},
	row::EncodedRowLayout,
	value::columnar::{
		Column, ColumnData, Columns,
		layout::{ColumnLayout, ColumnsLayout},
	},
};
use reifydb_type::{ROW_NUMBER_COLUMN_NAME, RowNumber, Type};

use crate::{
	StandardTransaction,
	execute::{Batch, ExecutionContext, QueryNode},
};

pub struct RingBufferScan<T: Transaction> {
	ring_buffer: RingBufferDef,
	metadata: Option<RingBufferMetadata>,
	layout: ColumnsLayout,
	row_layout: EncodedRowLayout,
	current_position: u64,
	rows_returned: u64,
	context: Option<Arc<ExecutionContext>>,
	initialized: bool,
	_phantom: std::marker::PhantomData<T>,
}

impl<T: Transaction> RingBufferScan<T> {
	pub fn new(ring_buffer: RingBufferDef, context: Arc<ExecutionContext>) -> crate::Result<Self> {
		// Create row layout based on column types
		let types: Vec<Type> = ring_buffer.columns.iter().map(|c| c.constraint.get_type()).collect();
		let row_layout = EncodedRowLayout::new(&types);

		// Create columns layout
		let layout = ColumnsLayout {
			columns: ring_buffer
				.columns
				.iter()
				.map(|col| ColumnLayout {
					namespace: None,
					source: None,
					name: col.name.clone(),
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
			_phantom: std::marker::PhantomData,
		})
	}

	fn from_ring_buffer_def(ring_buffer: &RingBufferDef) -> Columns {
		let columns: Vec<Column> = ring_buffer
			.columns
			.iter()
			.map(|col| {
				let name = col.name.clone();
				let data = match col.constraint.get_type() {
					Type::Boolean => ColumnData::bool(vec![]),
					Type::Float4 => ColumnData::float4(vec![]),
					Type::Float8 => ColumnData::float8(vec![]),
					Type::Int1 => ColumnData::int1(vec![]),
					Type::Int2 => ColumnData::int2(vec![]),
					Type::Int4 => ColumnData::int4(vec![]),
					Type::Int8 => ColumnData::int8(vec![]),
					Type::Int16 => ColumnData::int16(vec![]),
					Type::Utf8 => ColumnData::utf8(Vec::<String>::new()),
					Type::Uint1 => ColumnData::uint1(vec![]),
					Type::Uint2 => ColumnData::uint2(vec![]),
					Type::Uint4 => ColumnData::uint4(vec![]),
					Type::Uint8 => ColumnData::uint8(vec![]),
					Type::Uint16 => ColumnData::uint16(vec![]),
					Type::Date => ColumnData::date(vec![]),
					Type::DateTime => ColumnData::datetime(vec![]),
					Type::Time => ColumnData::time(vec![]),
					Type::Interval => ColumnData::interval(vec![]),
					Type::Uuid4 => ColumnData::uuid4(vec![]),
					Type::Uuid7 => ColumnData::uuid7(vec![]),
					Type::Blob => ColumnData::blob(vec![]),
					Type::Int => ColumnData::int(vec![]),
					Type::Uint => ColumnData::uint(vec![]),
					Type::Decimal => ColumnData::decimal(vec![]),
					Type::IdentityId => ColumnData::identity_id(vec![]),
					Type::RowNumber => ColumnData::row_number(vec![]),
					Type::Undefined => ColumnData::undefined(0),
				};
				Column::SourceQualified(reifydb_core::value::columnar::SourceQualified {
					source: ring_buffer.name.clone(),
					name,
					data,
				})
			})
			.collect();

		Columns(columns)
	}
}

impl<'a, T: Transaction> QueryNode<'a, T> for RingBufferScan<T> {
	fn initialize(&mut self, txn: &mut StandardTransaction<'a, T>, _ctx: &ExecutionContext) -> crate::Result<()> {
		if !self.initialized {
			// Get ring buffer metadata from the appropriate transaction type
			let metadata = match txn {
				crate::StandardTransaction::Command(cmd_txn) => {
					CatalogStore::find_ring_buffer_metadata(*cmd_txn, self.ring_buffer.id)?
				}
				crate::StandardTransaction::Query(query_txn) => {
					CatalogStore::find_ring_buffer_metadata(*query_txn, self.ring_buffer.id)?
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

	fn next(&mut self, txn: &mut StandardTransaction<'a, T>) -> crate::Result<Option<Batch>> {
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
				source: self.ring_buffer.id.into(),
				row: row_num,
			};

			// Get the row from storage
			if let Some(versioned) = txn.get(&key.encode())? {
				let row_data = versioned.row;
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
			// Build columns from rows
			let mut columns = Self::from_ring_buffer_def(&self.ring_buffer);
			columns.append_rows(&self.row_layout, batch_rows.into_iter())?;

			// Add row numbers if requested
			if ctx.preserve_row_numbers {
				let row_number_column =
					Column::SourceQualified(reifydb_core::value::columnar::SourceQualified {
						source: self.ring_buffer.name.clone(),
						name: ROW_NUMBER_COLUMN_NAME.to_string(),
						data: ColumnData::row_number(row_numbers),
					});
				columns.0.push(row_number_column);
			}

			Ok(Some(Batch {
				columns,
			}))
		}
	}

	fn layout(&self) -> Option<ColumnsLayout> {
		Some(self.layout.clone())
	}
}
