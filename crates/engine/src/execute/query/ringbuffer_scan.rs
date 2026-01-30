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
use reifydb_transaction::transaction::{AsTransaction, Transaction};
use reifydb_type::{
	fragment::Fragment,
	value::{row_number::RowNumber, r#type::Type},
};
use tracing::instrument;

use crate::execute::{Batch, ExecutionContext, QueryNode};

pub struct RingBufferScan {
	ringbuffer: ResolvedRingBuffer,
	metadata: Option<RingBufferMetadata>,
	headers: ColumnHeaders,
	schema: Option<Schema>,
	/// Storage types for each column (Type::DictionaryId for dictionary columns)
	storage_types: Vec<Type>,
	/// Dictionary definitions for columns that need decoding (None for non-dictionary columns)
	dictionaries: Vec<Option<DictionaryDef>>,
	current_position: u64,
	rows_returned: u64,
	context: Option<Arc<ExecutionContext>>,
	initialized: bool,
}

impl RingBufferScan {
	pub fn new<Rx: AsTransaction>(
		ringbuffer: ResolvedRingBuffer,
		context: Arc<ExecutionContext>,
		rx: &mut Rx,
	) -> crate::Result<Self> {
		// Build storage types and dictionaries
		let mut storage_types = Vec::with_capacity(ringbuffer.columns().len());
		let mut dictionaries = Vec::with_capacity(ringbuffer.columns().len());

		for col in ringbuffer.columns() {
			if let Some(dict_id) = col.dictionary_id {
				if let Some(dict) = context.executor.catalog.find_dictionary(rx, dict_id)? {
					storage_types.push(Type::DictionaryId);
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
		rx: &mut Transaction,
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
	fn initialize<'a>(&mut self, txn: &mut Transaction<'a>, ctx: &ExecutionContext) -> crate::Result<()> {
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
	fn next<'a>(&mut self, txn: &mut Transaction<'a>, _ctx: &mut ExecutionContext) -> crate::Result<Option<Batch>> {
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
			// Create columns with storage types (Type::DictionaryId for dictionary columns)
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

			// Restore row numbers
			columns.row_numbers = reifydb_type::util::cowvec::CowVec::new(row_numbers);

			super::decode_dictionary_columns(&mut columns, &self.dictionaries, txn)?;

			Ok(Some(Batch {
				columns,
			}))
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
