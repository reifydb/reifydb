// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::row::{bytes::EncodedBytes, ringbuffer::EncodedRingBufferRow, shape::RowShape};
use reifydb_core::{
	interface::{
		catalog::{dictionary::Dictionary, ringbuffer::PartitionedMetadata},
		resolved::ResolvedRingBuffer,
	},
	internal_error,
	key::{
		EncodableKey,
		partitioned_row::{PartitionedRowKey, RowLocator},
		row::RowKey,
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::{
	fragment::Fragment,
	value::{partition::Partition, row_number::RowNumber, system_columns::SystemColumns, value_type::ValueType},
};
use tracing::instrument;

use super::super::decode_dictionary_columns;
use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub struct RingBufferScan {
	ringbuffer: ResolvedRingBuffer,

	partitions: Vec<PartitionedMetadata>,
	current_partition_index: usize,
	headers: ColumnHeaders,
	shape: Option<RowShape>,

	storage_types: Vec<ValueType>,

	dictionaries: Vec<Option<Dictionary>>,

	partition_col_indices: Vec<usize>,
	current_partition_rows: Vec<(RowNumber, EncodedBytes)>,
	current_partition_cursor: usize,
	current_partition_loaded: bool,
	finished: bool,
	context: Option<Arc<QueryContext>>,
	initialized: bool,
}

impl RingBufferScan {
	pub fn new(
		ringbuffer: ResolvedRingBuffer,
		context: Arc<QueryContext>,
		rx: &mut Transaction<'_>,
	) -> Result<Self> {
		let mut storage_types = Vec::with_capacity(ringbuffer.columns().len());
		let mut dictionaries = Vec::with_capacity(ringbuffer.columns().len());

		for col in ringbuffer.columns() {
			if let Some(dict_id) = col.dictionary_id {
				if let Some(dict) = context.services.catalog.find_dictionary(rx, dict_id)? {
					storage_types.push(ValueType::DictionaryId);
					dictionaries.push(Some(dict));
				} else {
					storage_types.push(col.constraint.get_type());
					dictionaries.push(None);
				}
			} else {
				storage_types.push(col.constraint.get_type());
				dictionaries.push(None);
			}
		}

		let partition_col_indices: Vec<usize> = ringbuffer
			.def()
			.partition_by
			.iter()
			.map(|pb_col| ringbuffer.columns().iter().position(|c| c.name == *pb_col).unwrap())
			.collect();

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
			current_partition_rows: Vec::new(),
			current_partition_cursor: 0,
			current_partition_loaded: false,
			finished: false,
			context: Some(context),
			initialized: false,
		})
	}

	fn get_or_load_shape(&mut self, rx: &mut Transaction, first: &EncodedBytes) -> Result<RowShape> {
		if let Some(shape) = &self.shape {
			return Ok(shape.clone());
		}

		let fingerprint = EncodedRingBufferRow::view(first).fingerprint();

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

	#[instrument(level = "trace", skip_all, name = "volcano::scan::ringbuffer::drain")]
	fn drain_batch(
		&mut self,
		txn: &mut Transaction<'_>,
		batch_size: usize,
		partitioned: bool,
	) -> Result<(Vec<EncodedBytes>, Vec<RowNumber>, Vec<Partition>)> {
		let mut batch: Vec<EncodedBytes> = Vec::new();
		let mut row_numbers: Vec<RowNumber> = Vec::new();
		let mut partitions_sidecar: Vec<Partition> = Vec::new();

		while batch.len() < batch_size && self.current_partition_index < self.partitions.len() {
			if !self.current_partition_loaded {
				self.current_partition_rows =
					self.load_partition_rows(txn, self.current_partition_index)?;
				self.current_partition_cursor = 0;
				self.current_partition_loaded = true;
			}

			let hash = if partitioned {
				Some(Partition::of(&self.partitions[self.current_partition_index].partition_values))
			} else {
				None
			};

			while batch.len() < batch_size
				&& self.current_partition_cursor < self.current_partition_rows.len()
			{
				let (rn, row) = self.current_partition_rows[self.current_partition_cursor].clone();
				batch.push(row);
				row_numbers.push(rn);
				if let Some(h) = hash {
					partitions_sidecar.push(h);
				}
				self.current_partition_cursor += 1;
			}

			if self.current_partition_cursor >= self.current_partition_rows.len() {
				self.current_partition_index += 1;
				self.current_partition_loaded = false;
			}
		}

		Ok((batch, row_numbers, partitions_sidecar))
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::ringbuffer::column_alloc")]
	fn storage_columns(&self) -> Vec<ColumnWithName> {
		self.ringbuffer
			.columns()
			.iter()
			.enumerate()
			.map(|(idx, col)| ColumnWithName {
				name: Fragment::internal(&col.name),
				data: ColumnBuffer::with_capacity(self.storage_types[idx].clone(), 0),
			})
			.collect()
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::ringbuffer::empty_columns")]
	fn empty_columns(&self) -> Vec<ColumnWithName> {
		self.ringbuffer
			.columns()
			.iter()
			.map(|col| ColumnWithName {
				name: Fragment::internal(&col.name),
				data: ColumnBuffer::none_typed(col.constraint.get_type(), 0),
			})
			.collect()
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::ringbuffer::append_rows")]
	fn append_batch(
		&mut self,
		txn: &mut Transaction<'_>,
		columns: &mut Columns,
		bytes_vec: Vec<EncodedBytes>,
		row_numbers: Vec<RowNumber>,
	) -> Result<()> {
		let shape = self.get_or_load_shape(txn, &bytes_vec[0])?;
		columns.append_rows(&shape, bytes_vec.into_iter(), row_numbers.clone())?;
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::ringbuffer::load_partition")]
	fn load_partition_rows(
		&self,
		txn: &mut Transaction<'_>,
		partition_index: usize,
	) -> Result<Vec<(RowNumber, EncodedBytes)>> {
		let pm = &self.partitions[partition_index];
		let rb_id = self.ringbuffer.def().id;

		if self.partition_col_indices.is_empty() {
			let mut out = Vec::new();
			for rn_value in pm.metadata.head..pm.metadata.tail {
				let rn = RowNumber(rn_value);
				if let Some(multi) = txn.get(&RowKey::encoded(rb_id, rn))? {
					out.push((rn, multi.bytes));
				}
			}
			return Ok(out);
		}

		let hash = Partition::of(&pm.partition_values);
		let mut out = Vec::new();
		let mut last_key = None;
		loop {
			let batch: Vec<_> = txn
				.range(
					PartitionedRowKey::partition_scan_range(rb_id, hash, last_key.as_ref()),
					RangeScope::All,
					1024,
				)?
				.collect::<Result<Vec<_>>>()?;
			if batch.is_empty() {
				break;
			}
			let n = batch.len();
			for entry in batch {
				if let Some(RowLocator::Row(rn)) =
					PartitionedRowKey::decode(&entry.key).map(|pk| pk.locator)
				{
					out.push((rn, entry.bytes));
				}
				last_key = Some(entry.key);
			}
			if n < 1024 {
				break;
			}
		}
		out.sort_by_key(|(rn, _)| rn.0);
		Ok(out)
	}
}

impl QueryNode for RingBufferScan {
	#[instrument(name = "volcano::scan::ringbuffer::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, txn: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		if !self.initialized {
			self.partitions =
				ctx.services.catalog.list_ringbuffer_partitions(txn, self.ringbuffer.def())?;
			self.initialized = true;
		}
		Ok(())
	}

	#[instrument(name = "volcano::scan::ringbuffer::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, txn: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		if self.finished {
			return Ok(None);
		}

		let batch_size = self.context.as_ref().expect("RingBufferScan context not set").batch_size as usize;
		let partitioned = !self.partition_col_indices.is_empty();

		let (batch_rows, row_numbers, partitions_sidecar) = self.drain_batch(txn, batch_size, partitioned)?;

		if !batch_rows.is_empty() {
			let mut columns = Columns::with_system(self.storage_columns(), SystemColumns::default());
			self.append_batch(txn, &mut columns, batch_rows, row_numbers)?;
			if partitioned {
				columns.system.set_partitions(partitions_sidecar);
			}

			decode_dictionary_columns(&mut columns, &self.dictionaries, txn)?;

			return Ok(Some(columns));
		}

		self.finished = true;
		if self.partitions.is_empty() || self.partitions.iter().all(|p| p.metadata.is_empty()) {
			return Ok(Some(Columns::new(self.empty_columns())));
		}
		Ok(None)
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
