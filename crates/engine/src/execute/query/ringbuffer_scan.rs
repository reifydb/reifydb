// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_catalog::CatalogStore;
use reifydb_core::{
	interface::{
		DictionaryDef, EncodableKey, MultiVersionQueryTransaction, RingBufferMetadata, RowKey,
		resolved::ResolvedRingBuffer,
	},
	value::{
		column::{Column, ColumnData, Columns, headers::ColumnHeaders},
		encoded::EncodedValuesLayout,
	},
};
use reifydb_type::{DictionaryEntryId, Fragment, RowNumber, Type};
use tracing::instrument;

use crate::{
	StandardTransaction,
	execute::{Batch, ExecutionContext, QueryNode},
	transaction::operation::DictionaryOperations,
};

pub struct RingBufferScan {
	ringbuffer: ResolvedRingBuffer,
	metadata: Option<RingBufferMetadata>,
	headers: ColumnHeaders,
	row_layout: EncodedValuesLayout,
	/// Storage types for each column (dictionary ID types for dictionary columns)
	storage_types: Vec<Type>,
	/// Dictionary definitions for columns that need decoding (None for non-dictionary columns)
	dictionaries: Vec<Option<DictionaryDef>>,
	current_position: u64,
	rows_returned: u64,
	context: Option<Arc<ExecutionContext>>,
	initialized: bool,
}

impl RingBufferScan {
	pub async fn new<Rx: MultiVersionQueryTransaction + reifydb_core::interface::QueryTransaction>(
		ringbuffer: ResolvedRingBuffer,
		context: Arc<ExecutionContext>,
		rx: &mut Rx,
	) -> crate::Result<Self> {
		// Look up dictionaries and build storage types
		let mut storage_types = Vec::with_capacity(ringbuffer.columns().len());
		let mut dictionaries = Vec::with_capacity(ringbuffer.columns().len());

		for col in ringbuffer.columns() {
			if let Some(dict_id) = col.dictionary_id {
				if let Some(dict) = CatalogStore::find_dictionary(rx, dict_id).await? {
					storage_types.push(dict.id_type);
					dictionaries.push(Some(dict));
				} else {
					// Dictionary not found, fall back to constraint type
					storage_types.push(col.constraint.get_type());
					dictionaries.push(None);
				}
			} else {
				storage_types.push(col.constraint.get_type());
				dictionaries.push(None);
			}
		}

		let row_layout = EncodedValuesLayout::new(&storage_types);

		// Create columns headers
		let headers = ColumnHeaders {
			columns: ringbuffer.columns().iter().map(|col| Fragment::internal(&col.name)).collect(),
		};

		Ok(Self {
			ringbuffer,
			metadata: None,
			headers,
			row_layout,
			storage_types,
			dictionaries,
			current_position: 0,
			rows_returned: 0,
			context: Some(context),
			initialized: false,
		})
	}
}

#[async_trait]
impl QueryNode for RingBufferScan {
	#[instrument(name = "query::scan::ringbuffer::initialize", level = "trace", skip_all)]
	async fn initialize<'a>(
		&mut self,
		txn: &mut StandardTransaction<'a>,
		_ctx: &ExecutionContext,
	) -> crate::Result<()> {
		if !self.initialized {
			// Get ring buffer metadata from the appropriate transaction type
			let metadata = match txn {
				crate::StandardTransaction::Command(cmd_txn) => {
					CatalogStore::find_ringbuffer_metadata(*cmd_txn, self.ringbuffer.def().id)
						.await?
				}
				crate::StandardTransaction::Query(query_txn) => {
					CatalogStore::find_ringbuffer_metadata(*query_txn, self.ringbuffer.def().id)
						.await?
				}
			};
			self.metadata = metadata;

			if let Some(ref metadata) = self.metadata {
				// Start scanning from head (oldest row number)
				// For empty buffer, this value doesn't matter since no rows will be returned
				self.current_position = metadata.head;
			}

			self.initialized = true;
		}
		Ok(())
	}

	#[instrument(name = "query::scan::ringbuffer::next", level = "trace", skip_all)]
	async fn next<'a>(
		&mut self,
		txn: &mut StandardTransaction<'a>,
		_ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
		let stored_ctx = self.context.as_ref().expect("RingBufferScan context not set");

		// Get metadata or return empty
		let metadata = match &self.metadata {
			Some(m) => m,
			None => return Ok(None),
		};

		// If we've returned all rows, we're done
		if self.rows_returned >= metadata.count {
			return Ok(None);
		}

		let batch_size = stored_ctx.batch_size;

		// Collect rows for this batch
		let mut batch_rows = Vec::new();
		let mut row_numbers = Vec::new();
		let mut batch_count = 0;

		// Scan row numbers starting from head (monotonically increasing)
		// With monotonically increasing row numbers, we iterate from head to tail-1
		// Row numbers may have gaps after DELETE operations
		let max_row_num = metadata.tail; // tail is next row number to allocate
		while batch_count < batch_size
			&& self.rows_returned < metadata.count
			&& self.current_position < max_row_num
		{
			let row_num = RowNumber(self.current_position);

			// Create the encoded key
			let key = RowKey {
				source: self.ringbuffer.def().id.into(),
				row: row_num,
			};

			// Get the encoded from storage
			if let Some(multi) = txn.get(&key.encode()).await? {
				let row_data = multi.values;
				batch_rows.push(row_data);
				row_numbers.push(row_num);
				self.rows_returned += 1;
				batch_count += 1;
			}

			// Move to next row number (monotonically increasing)
			self.current_position += 1;
		}

		if batch_rows.is_empty() {
			Ok(None)
		} else {
			// Create columns with storage types (dictionary ID types for dictionary columns)
			let storage_columns: Vec<Column> = self
				.ringbuffer
				.columns()
				.iter()
				.enumerate()
				.map(|(idx, col)| Column {
					name: Fragment::internal(&col.name),
					data: ColumnData::with_capacity(self.storage_types[idx], 0),
				})
				.collect();

			let mut columns = Columns::with_row_numbers(storage_columns, Vec::new());
			columns.append_rows(&self.row_layout, batch_rows.into_iter(), row_numbers.clone())?;

			// Decode dictionary columns
			self.decode_dictionary_columns(&mut columns, txn)?;

			// Restore row numbers
			columns.row_numbers = reifydb_core::util::CowVec::new(row_numbers);

			Ok(Some(Batch {
				columns,
			}))
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}

impl<'a> RingBufferScan {
	/// Decode dictionary columns by replacing dictionary IDs with actual values
	async fn decode_dictionary_columns(
		&self,
		columns: &mut Columns,
		txn: &mut StandardTransaction<'a>,
	) -> crate::Result<()> {
		for (col_idx, dict_opt) in self.dictionaries.iter().enumerate() {
			if let Some(dictionary) = dict_opt {
				let col = &columns[col_idx];
				let row_count = col.data().len();

				// Create new column data with the original value type
				let mut new_data = ColumnData::with_capacity(dictionary.value_type, row_count);

				// Decode each value
				for row_idx in 0..row_count {
					let id_value = col.data().get_value(row_idx);
					if let Some(entry_id) = DictionaryEntryId::from_value(&id_value) {
						if let Some(decoded_value) =
							txn.get_from_dictionary(dictionary, entry_id).await?
						{
							new_data.push_value(decoded_value);
						} else {
							new_data.push_value(reifydb_type::Value::Undefined);
						}
					} else {
						new_data.push_value(reifydb_type::Value::Undefined);
					}
				}

				// Replace the column data
				let col_name = columns[col_idx].name().clone();
				columns.columns.make_mut()[col_idx] = Column {
					name: col_name,
					data: new_data,
				};
			}
		}
		Ok(())
	}
}
