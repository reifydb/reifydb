// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::{row::EncodedRow, shape::RowShape},
	interface::{
		catalog::{dictionary::Dictionary, ringbuffer::PartitionedMetadata},
		resolved::ResolvedRingBuffer,
	},
	internal_error,
	key::row::RowKey,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	util::cowvec::CowVec,
	value::{Value, row_number::RowNumber, r#type::Type},
};
use tracing::instrument;

use super::super::decode_dictionary_columns;
use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub struct RingBufferScan {
	ringbuffer: ResolvedRingBuffer,
	/// All partitions for this ringbuffer (global = 1-element, partitioned = N-element)
	partitions: Vec<PartitionedMetadata>,
	current_partition_index: usize,
	headers: ColumnHeaders,
	shape: Option<RowShape>,
	/// Storage types for each column (Type::DictionaryId for dictionary columns)
	storage_types: Vec<Type>,
	/// Dictionary definitions for columns that need decoding (None for non-dictionary columns)
	dictionaries: Vec<Option<Dictionary>>,
	/// Column indices for partition_by columns (empty for global ringbuffers)
	partition_col_indices: Vec<usize>,
	current_position: u64,
	rows_returned_in_partition: u64,
	context: Option<Arc<QueryContext>>,
	initialized: bool,
	scan_limit: Option<usize>,
}

impl RingBufferScan {
	pub fn new(
		ringbuffer: ResolvedRingBuffer,
		context: Arc<QueryContext>,
		rx: &mut Transaction<'_>,
	) -> Result<Self> {
		// Build storage types and dictionaries
		let mut storage_types = Vec::with_capacity(ringbuffer.columns().len());
		let mut dictionaries = Vec::with_capacity(ringbuffer.columns().len());

		for col in ringbuffer.columns() {
			if let Some(dict_id) = col.dictionary_id {
				if let Some(dict) = context.services.catalog.find_dictionary(rx, dict_id)? {
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

		// Resolve partition column indices
		let partition_col_indices: Vec<usize> = ringbuffer
			.def()
			.partition_by
			.iter()
			.map(|pb_col| ringbuffer.columns().iter().position(|c| c.name == *pb_col).unwrap())
			.collect();

		// Create columns headers
		let headers = ColumnHeaders {
			columns: ringbuffer.columns().iter().map(|col| Fragment::internal(&col.name)).collect(),
		};

		Ok(Self {
			ringbuffer,
			partitions: Vec::new(),
			current_partition_index: 0,
			headers,
			shape: None,
			storage_types,
			dictionaries,
			partition_col_indices,
			current_position: 0,
			rows_returned_in_partition: 0,
			context: Some(context),
			initialized: false,
			scan_limit: None,
		})
	}

	fn get_or_load_shape(&mut self, rx: &mut Transaction, first_row: &EncodedRow) -> Result<RowShape> {
		if let Some(shape) = &self.shape {
			return Ok(shape.clone());
		}

		let fingerprint = first_row.fingerprint();

		let stored_ctx = self.context.as_ref().expect("RingBufferScan context not set");
		let shape = stored_ctx.services.catalog.get_or_load_row_shape(fingerprint, rx)?.ok_or_else(|| {
			internal_error!(
				"RowShape with fingerprint {:?} not found for ringbuffer {}",
				fingerprint,
				self.ringbuffer.def().name
			)
		})?;

		self.shape = Some(shape.clone());

		Ok(shape)
	}

	/// Advance to next non-empty partition if current is exhausted. Returns false if all done.
	fn advance_to_next_partition(&mut self) -> bool {
		loop {
			self.current_partition_index += 1;
			if self.current_partition_index >= self.partitions.len() {
				return false;
			}
			let partition = &self.partitions[self.current_partition_index].metadata;
			if !partition.is_empty() {
				self.current_position = partition.head;
				self.rows_returned_in_partition = 0;
				return true;
			}
		}
	}
}

impl QueryNode for RingBufferScan {
	#[instrument(name = "volcano::scan::ringbuffer::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, txn: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		if !self.initialized {
			// Load all partitions (global = 1-element vec, partitioned = N-element vec)
			self.partitions =
				ctx.services.catalog.list_ringbuffer_partitions(txn, self.ringbuffer.def())?;

			// Start scanning from the first non-empty partition's head
			if let Some(partition) = self.partitions.first() {
				self.current_position = partition.metadata.head;
			}

			self.initialized = true;
		}
		Ok(())
	}

	#[instrument(name = "volcano::scan::ringbuffer::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, txn: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		let stored_ctx = self.context.as_ref().expect("RingBufferScan context not set");

		// If no partitions, return empty shape
		if self.partitions.is_empty() {
			if self.current_partition_index == 0 {
				self.current_partition_index = 1; // prevent re-entry
				let columns: Vec<ColumnWithName> = self
					.ringbuffer
					.columns()
					.iter()
					.map(|col| ColumnWithName {
						name: Fragment::internal(&col.name),
						data: ColumnBuffer::none_typed(col.constraint.get_type(), 0),
					})
					.collect();
				return Ok(Some(Columns::new(columns)));
			}
			return Ok(None);
		}

		// Check if we're past all partitions
		if self.current_partition_index >= self.partitions.len() {
			return Ok(None);
		}

		let batch_size = match self.scan_limit {
			Some(limit) => limit.min(stored_ctx.batch_size as usize),
			None => stored_ctx.batch_size as usize,
		};

		// Collect rows for this batch, spanning partitions if needed
		let mut batch_rows = Vec::new();
		let mut row_numbers = Vec::new();

		loop {
			if self.current_partition_index >= self.partitions.len() {
				break;
			}

			// Copy partition fields to avoid holding a borrow on self
			let partition_empty = self.partitions[self.current_partition_index].metadata.is_empty();
			if partition_empty {
				if !self.advance_to_next_partition() {
					break;
				}
				continue;
			}

			let max_row_num = self.partitions[self.current_partition_index].metadata.tail;
			let partition_count = self.partitions[self.current_partition_index].metadata.count;
			let partition_values = self.partitions[self.current_partition_index].partition_values.clone();
			let partition_col_indices = self.partition_col_indices.clone();

			while batch_rows.len() < batch_size
				&& self.rows_returned_in_partition < partition_count
				&& self.current_position < max_row_num
			{
				let row_num = RowNumber(self.current_position);
				let key = RowKey::encoded(self.ringbuffer.def().id, row_num);

				if let Some(multi) = txn.get(&key)? {
					// For partitioned ringbuffers, check if this row belongs to the current
					// partition
					if !partition_col_indices.is_empty() {
						let shape = self.get_or_load_shape(txn, &multi.row)?;
						if !row_matches_partition(
							&shape,
							&multi.row,
							&partition_col_indices,
							&partition_values,
						) {
							self.current_position += 1;
							continue;
						}
					}
					batch_rows.push(multi.row);
					row_numbers.push(row_num);
					self.rows_returned_in_partition += 1;
				}

				self.current_position += 1;
			}

			// If we've exhausted this partition, move to next
			if (self.rows_returned_in_partition >= partition_count || self.current_position >= max_row_num)
				&& !self.advance_to_next_partition()
			{
				break;
			}

			// If we've filled a batch, stop
			if batch_rows.len() >= batch_size {
				break;
			}
		}

		if batch_rows.is_empty() {
			// If we never returned any rows at all, return empty shape
			if self.partitions.iter().all(|p| p.metadata.is_empty()) {
				let columns: Vec<ColumnWithName> = self
					.ringbuffer
					.columns()
					.iter()
					.map(|col| ColumnWithName {
						name: Fragment::internal(&col.name),
						data: ColumnBuffer::none_typed(col.constraint.get_type(), 0),
					})
					.collect();
				return Ok(Some(Columns::new(columns)));
			}
			Ok(None)
		} else {
			// Create columns with storage types (Type::DictionaryId for dictionary columns)
			let storage_columns: Vec<ColumnWithName> = self
				.ringbuffer
				.columns()
				.iter()
				.enumerate()
				.map(|(idx, col)| ColumnWithName {
					name: Fragment::internal(&col.name),
					data: ColumnBuffer::with_capacity(self.storage_types[idx].clone(), 0),
				})
				.collect();

			let mut columns =
				Columns::with_system_columns(storage_columns, Vec::new(), Vec::new(), Vec::new());
			let shape = self.get_or_load_shape(txn, &batch_rows[0])?;
			columns.append_rows(&shape, batch_rows.into_iter(), row_numbers.clone())?;

			// Restore row numbers
			columns.row_numbers = CowVec::new(row_numbers);

			decode_dictionary_columns(&mut columns, &self.dictionaries, txn)?;

			Ok(Some(columns))
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}

	fn set_scan_limit(&mut self, limit: usize) {
		self.scan_limit = Some(limit);
	}
}

fn row_matches_partition(
	shape: &RowShape,
	row: &EncodedRow,
	partition_col_indices: &[usize],
	expected_values: &[Value],
) -> bool {
	partition_col_indices.iter().zip(expected_values).all(|(&idx, expected)| shape.get_value(row, idx) == *expected)
}
