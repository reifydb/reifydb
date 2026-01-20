// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::schema::Schema,
	interface::{
		catalog::{dictionary::DictionaryDef, ringbuffer::RingBufferMetadata},
		resolved::ResolvedRingBuffer,
	},
	key::row::RowKey,
	value::column::{Column, columns::Columns, data::ColumnData, headers::ColumnHeaders},
};
use reifydb_transaction::standard::{IntoStandardTransaction, StandardTransaction};
use reifydb_type::{
	fragment::Fragment,
	value::{dictionary::DictionaryEntryId, row_number::RowNumber, r#type::Type},
};
use tracing::instrument;

use crate::{
	execute::{Batch, ExecutionContext, QueryNode},
	transaction::operation::dictionary::DictionaryOperations,
};

pub struct RingBufferScan {
	ringbuffer: ResolvedRingBuffer,
	metadata: Option<RingBufferMetadata>,
	headers: ColumnHeaders,
	schema: Option<Schema>,
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
	pub fn new<Rx: IntoStandardTransaction>(
		ringbuffer: ResolvedRingBuffer,
		context: Arc<ExecutionContext>,
		rx: &mut Rx,
	) -> crate::Result<Self> {
		// Look up dictionaries and build storage types
		let mut storage_types = Vec::with_capacity(ringbuffer.columns().len());
		let mut dictionaries = Vec::with_capacity(ringbuffer.columns().len());

		for col in ringbuffer.columns() {
			if let Some(dict_id) = col.dictionary_id {
				if let Some(dict) = context.executor.catalog.find_dictionary(rx, dict_id)? {
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

		// Create columns headers
		let headers = ColumnHeaders {
			columns: ringbuffer.columns().iter().map(|col| Fragment::internal(&col.name)).collect(),
		};

		Ok(Self {
			ringbuffer,
			metadata: None,
			headers,
			schema: None,
			storage_types,
			dictionaries,
			current_position: 0,
			rows_returned: 0,
			context: Some(context),
			initialized: false,
		})
	}

	fn get_or_load_schema(
		&mut self,
		rx: &mut StandardTransaction,
		first_row: &reifydb_core::encoded::encoded::EncodedValues,
	) -> crate::Result<Schema> {
		if let Some(schema) = &self.schema {
			return Ok(schema.clone());
		}

		let fingerprint = first_row.fingerprint();

		let stored_ctx = self.context.as_ref().expect("RingBufferScan context not set");
		let schema = stored_ctx.executor.catalog.schema.get_or_load(fingerprint, rx)?.ok_or_else(|| {
			reifydb_type::error!(reifydb_core::error::diagnostic::internal::internal(format!(
				"Schema with fingerprint {:?} not found for ringbuffer {}",
				fingerprint,
				self.ringbuffer.def().name
			)))
		})?;

		self.schema = Some(schema.clone());

		Ok(schema)
	}
}

impl QueryNode for RingBufferScan {
	#[instrument(name = "query::scan::ringbuffer::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, txn: &mut StandardTransaction<'a>, ctx: &ExecutionContext) -> crate::Result<()> {
		if !self.initialized {
			// Get ring buffer metadata from the catalog
			let metadata = ctx.executor.catalog.find_ringbuffer_metadata(txn, self.ringbuffer.def().id)?;
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
	fn next<'a>(
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
			let key = RowKey::encoded(self.ringbuffer.def().id, row_num);

			// Get the encoded from storage
			if let Some(multi) = txn.get(&key)? {
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
			let schema = self.get_or_load_schema(txn, &batch_rows[0])?;
			columns.append_rows(&schema, batch_rows.into_iter(), row_numbers.clone())?;

			// Decode dictionary columns
			self.decode_dictionary_columns(&mut columns, txn)?;

			// Restore row numbers
			columns.row_numbers = reifydb_type::util::cowvec::CowVec::new(row_numbers);

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
	fn decode_dictionary_columns(
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
							txn.get_from_dictionary(dictionary, entry_id)?
						{
							new_data.push_value(decoded_value);
						} else {
							new_data.push_value(reifydb_type::value::Value::Undefined);
						}
					} else {
						new_data.push_value(reifydb_type::value::Value::Undefined);
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
