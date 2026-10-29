// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Reverse,
	collections::{BinaryHeap, VecDeque},
	sync::Arc,
};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_codec::row::{bytes::EncodedBytes, ringbuffer::EncodedRingBufferRow, shape::RowShape};
use reifydb_core::{
	common::TimeSource,
	interface::{
		catalog::{
			column::Column,
			dictionary::Dictionary,
			object::ObjectId,
			ringbuffer::{PartitionedMetadata, RingBufferMetadata},
			storage::StorageId,
			view::View,
		},
		resolved::{ResolvedRingBuffer, ResolvedView},
	},
	internal_error,
	key::{
		any::TaggedKey,
		partition::PartitionKey,
		row::{PartitionedRowKey, RowKey},
	},
	partition::{partition_col_indices, registry_values},
	value::{
		batch::{append_rows, batch},
		column::{builder::ColumnBuilder, headers::ColumnHeaders},
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{
	datetime::DateTime,
	partition::Partition,
	row_number::RowNumber,
	system_columns::{SystemColumn, with_system_column},
	value_type::ValueType,
};
use tracing::instrument;

use super::{
	super::decode_dictionary_columns, empty_scan, guard_view_read, partition_array, scan_headers,
	source_system_columns,
};
use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

const MERGE_WINDOW: usize = 64;

struct RingSource {
	storage: StorageId,
	name: String,
	columns: Vec<Column>,
	capacity: u64,
	registry: Option<ObjectId>,
	only: Option<Partition>,
	view: Option<ResolvedView>,
}

impl RingSource {
	fn key(&self, partition: Option<Partition>, row: RowNumber) -> TaggedKey {
		match partition {
			Some(partition) => PartitionedRowKey::new(self.storage, partition, row).into(),
			None => RowKey::new(self.storage, row).into(),
		}
	}

	fn read(
		&self,
		txn: &mut Transaction<'_>,
		partition: Option<Partition>,
		rows: &[RowNumber],
	) -> Result<Vec<(RowNumber, EncodedBytes)>> {
		if rows.is_empty() {
			return Ok(Vec::new());
		}
		let keys: Vec<TaggedKey> = rows.iter().map(|&row| self.key(partition, row)).collect();
		Ok(rows.iter()
			.zip(txn.get(&keys)?)
			.filter_map(|(&row, stored)| stored.map(|stored| (row, stored.bytes)))
			.collect())
	}
}

struct MergeHead {
	partition: Option<Partition>,
	pending: VecDeque<RowNumber>,
	buffer: VecDeque<(RowNumber, EncodedBytes)>,
}

type MergeOrder = (DateTime, Option<Partition>, u64);

struct OldestFirstMerge {
	heads: Vec<MergeHead>,
	queue: BinaryHeap<Reverse<(MergeOrder, usize)>>,
}

impl OldestFirstMerge {
	fn open(txn: &mut Transaction<'_>, source: &RingSource, partitions: &[PartitionedMetadata]) -> Result<Self> {
		let mut merge = Self {
			heads: partitions
				.iter()
				.map(|listed| MergeHead {
					partition: listed.partition,
					pending: listed
						.metadata
						.oldest_first(source.capacity)
						.map(|(_, row)| row)
						.collect(),
					buffer: VecDeque::new(),
				})
				.collect(),
			queue: BinaryHeap::new(),
		};
		for index in 0..merge.heads.len() {
			merge.enqueue(txn, source, index)?;
		}
		Ok(merge)
	}

	fn enqueue(&mut self, txn: &mut Transaction<'_>, source: &RingSource, index: usize) -> Result<()> {
		let head = &mut self.heads[index];
		while head.buffer.is_empty() && !head.pending.is_empty() {
			let take = head.pending.len().min(MERGE_WINDOW);
			let window: Vec<RowNumber> = head.pending.drain(..take).collect();
			head.buffer.extend(source.read(txn, head.partition, &window)?);
		}
		if let Some((_, bytes)) = head.buffer.front() {
			let row = EncodedRingBufferRow::view(bytes);
			self.queue.push(Reverse(((row.created_at(), head.partition, row.sequence()), index)));
		}
		Ok(())
	}

	fn next(
		&mut self,
		txn: &mut Transaction<'_>,
		source: &RingSource,
		batch_size: usize,
	) -> Result<Vec<(Option<Partition>, RowNumber, EncodedBytes)>> {
		let mut rows = Vec::new();
		while rows.len() < batch_size {
			let Some(Reverse((_, index))) = self.queue.pop() else {
				break;
			};
			let head = &mut self.heads[index];
			let Some((row, bytes)) = head.buffer.pop_front() else {
				return Err(
					internal_error!("a queued ring buffer merge head had no buffered row").into()
				);
			};
			rows.push((head.partition, row, bytes));
			self.enqueue(txn, source, index)?;
		}
		Ok(rows)
	}
}

pub struct RingBufferScan {
	source: RingSource,
	partitions: Vec<PartitionedMetadata>,
	current_partition_index: usize,
	headers: ColumnHeaders,
	shape: Option<RowShape>,
	storage_types: Vec<ValueType>,
	dictionaries: Vec<Option<(Dictionary, ValueType)>>,
	current_partition_rows: Vec<(RowNumber, EncodedBytes)>,
	current_partition_cursor: usize,
	current_partition_loaded: bool,
	finished: bool,
	produced: bool,
	context: Option<Arc<QueryContext>>,
	initialized: bool,
	system_columns: Vec<SystemColumn>,
	newest_first: bool,
	oldest_first: bool,
	merge: Option<OldestFirstMerge>,
	keep_dictionary_ids: bool,
}

impl RingBufferScan {
	pub fn new(
		ringbuffer: ResolvedRingBuffer,
		context: Arc<QueryContext>,
		rx: &mut Transaction<'_>,
	) -> Result<Self> {
		let def = ringbuffer.def();
		let partitioned = !partition_col_indices(ringbuffer.columns(), &def.partition_by).is_empty();
		let system_columns = source_system_columns(partitioned, def.time != TimeSource::None, false, true);
		let source = RingSource {
			storage: def.id.into(),
			name: def.name.clone(),
			columns: ringbuffer.columns().to_vec(),
			capacity: def.capacity,
			registry: Some(ObjectId::ringbuffer(def.id)),
			only: None,
			view: None,
		};
		Self::over(source, system_columns, false, context, rx)
	}

	pub fn for_view(
		view: ResolvedView,
		partition: Option<Partition>,
		context: Arc<QueryContext>,
		rx: &mut Transaction<'_>,
	) -> Result<Self> {
		let View::RingBuffer(def) = view.def() else {
			return Err(internal_error!(
				"view {} is not ring buffer backed and cannot be scanned as a ring",
				view.def().name()
			)
			.into());
		};
		let system_columns = source_system_columns(false, true, false, true);
		let source = RingSource {
			storage: view.def().storage_id(),
			name: def.name.clone(),
			columns: def.columns.clone(),
			capacity: def.capacity,
			registry: None,
			only: partition,
			view: Some(view.clone()),
		};
		Self::over(source, system_columns, true, context, rx)
	}

	fn over(
		source: RingSource,
		system_columns: Vec<SystemColumn>,
		newest_first: bool,
		context: Arc<QueryContext>,
		rx: &mut Transaction<'_>,
	) -> Result<Self> {
		let mut storage_types = Vec::with_capacity(source.columns.len());
		let mut dictionaries = Vec::with_capacity(source.columns.len());
		for col in &source.columns {
			match col.dictionary_id {
				Some(dict_id) => match context.services.catalog.find_dictionary(rx, dict_id)? {
					Some(dict) => {
						storage_types.push(ValueType::DictionaryId);
						dictionaries.push(Some((dict, col.constraint.get_type())));
					}
					None => {
						storage_types.push(col.constraint.get_type());
						dictionaries.push(None);
					}
				},
				None => {
					storage_types.push(col.constraint.get_type());
					dictionaries.push(None);
				}
			}
		}
		let headers = scan_headers(source.columns.iter().map(|col| col.name.as_str()), &system_columns);
		Ok(Self {
			source,
			partitions: Vec::new(),
			current_partition_index: 0,
			headers,
			shape: None,
			storage_types,
			dictionaries,
			current_partition_rows: Vec::new(),
			current_partition_cursor: 0,
			current_partition_loaded: false,
			finished: false,
			produced: false,
			context: Some(context),
			initialized: false,
			system_columns,
			newest_first,
			oldest_first: false,
			merge: None,
			keep_dictionary_ids: false,
		})
	}

	pub(crate) fn oldest_first(mut self) -> Self {
		self.newest_first = false;
		self.oldest_first = true;
		self
	}

	pub(crate) fn keep_dictionary_ids(mut self) -> Self {
		self.keep_dictionary_ids = true;
		self
	}

	fn get_or_load_shape(&mut self, rx: &mut Transaction, first: &EncodedBytes) -> Result<RowShape> {
		if let Some(shape) = &self.shape {
			return Ok(shape.clone());
		}
		let fingerprint = EncodedRingBufferRow::view(first).fingerprint();
		let stored_ctx = self.context.as_ref().expect("RingBufferScan context not set");
		let shape = stored_ctx.services.catalog.get_or_load_row_shape(fingerprint, rx)?.ok_or_else(|| {
			internal_error!(
				"RowShape with fingerprint {:?} not found for ring {}",
				fingerprint,
				self.source.name
			)
		})?;
		self.shape = Some(shape.clone());
		Ok(shape)
	}

	fn list_partitions(&self, txn: &mut Transaction<'_>, ctx: &QueryContext) -> Result<Vec<PartitionedMetadata>> {
		let mut listed: Vec<PartitionedMetadata> = ctx
			.services
			.catalog
			.list_ringbuffer_partitions(txn, self.source.storage)?
			.into_iter()
			.filter(|listed| !listed.metadata.is_empty())
			.filter(|listed| self.source.only.is_none() || listed.partition == self.source.only)
			.collect();
		if let Some(object) = self.source.registry
			&& listed.len() > 1
		{
			let keys: Vec<TaggedKey> = listed
				.iter()
				.map(|listed| PartitionKey::new(object, listed.partition.unwrap_or_default()).into())
				.collect();
			let values: Vec<_> = txn
				.get(&keys)?
				.into_iter()
				.map(|row| row.map(|row| registry_values(&row.bytes)).unwrap_or_default())
				.collect();
			let mut ordered: Vec<_> = values.into_iter().zip(listed).collect();
			ordered.sort_by(|left, right| left.0.cmp(&right.0));
			listed = ordered.into_iter().map(|(_, listed)| listed).collect();
		}
		Ok(listed)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::ringbuffer::load_partition")]
	fn load_partition_rows(
		&self,
		txn: &mut Transaction<'_>,
		index: usize,
	) -> Result<Vec<(RowNumber, EncodedBytes)>> {
		let listed = &self.partitions[index];
		let metadata: &RingBufferMetadata = &listed.metadata;
		let rows: Vec<RowNumber> = metadata.oldest_first(self.source.capacity).map(|(_, row)| row).collect();
		let mut loaded = self.source.read(txn, listed.partition, &rows)?;
		if self.newest_first {
			loaded.reverse();
		}
		Ok(loaded)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::ringbuffer::drain")]
	fn drain_batch(
		&mut self,
		txn: &mut Transaction<'_>,
		batch_size: usize,
	) -> Result<Vec<(Option<Partition>, RowNumber, EncodedBytes)>> {
		let mut drained = Vec::new();
		while drained.len() < batch_size && self.current_partition_index < self.partitions.len() {
			if !self.current_partition_loaded {
				self.current_partition_rows =
					self.load_partition_rows(txn, self.current_partition_index)?;
				self.current_partition_cursor = 0;
				self.current_partition_loaded = true;
			}
			let partition = self.partitions[self.current_partition_index].partition;
			while drained.len() < batch_size
				&& self.current_partition_cursor < self.current_partition_rows.len()
			{
				let (rn, row) = self.current_partition_rows[self.current_partition_cursor].clone();
				drained.push((partition, rn, row));
				self.current_partition_cursor += 1;
			}
			if self.current_partition_cursor >= self.current_partition_rows.len() {
				self.current_partition_index += 1;
				self.current_partition_loaded = false;
			}
		}
		Ok(drained)
	}

	fn storage_columns(&self) -> Vec<(FieldRef, ArrayRef)> {
		self.source
			.columns
			.iter()
			.enumerate()
			.map(|(idx, col)| {
				ColumnBuilder::with_capacity(self.storage_types[idx].clone(), 0).finish(&col.name)
			})
			.collect()
	}

	fn empty_columns(&self) -> Vec<(FieldRef, ArrayRef)> {
		self.source
			.columns
			.iter()
			.map(|col| ColumnBuilder::with_capacity(col.constraint.get_type(), 0).finish(&col.name))
			.collect()
	}
}

impl QueryNode for RingBufferScan {
	#[instrument(name = "volcano::scan::ringbuffer::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, txn: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		if let Some(view) = &self.source.view {
			guard_view_read(view, txn, &ctx.services)?;
		}
		if !self.initialized {
			self.partitions = self.list_partitions(txn, ctx)?;
			if self.oldest_first && self.partitions.len() > 1 {
				self.merge = Some(OldestFirstMerge::open(txn, &self.source, &self.partitions)?);
			}
			self.initialized = true;
		}
		Ok(())
	}

	#[instrument(name = "volcano::scan::ringbuffer::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, txn: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		if self.finished {
			return Ok(None);
		}
		let batch_size = self.context.as_ref().expect("RingBufferScan context not set").batch_size as usize;
		let drained = match self.merge.as_mut() {
			Some(merge) => merge.next(txn, &self.source, batch_size)?,
			None => self.drain_batch(txn, batch_size)?,
		};

		if drained.is_empty() {
			self.finished = true;
			if self.produced {
				return Ok(None);
			}
			return Ok(Some(empty_scan(self.empty_columns(), &self.system_columns)?));
		}
		self.produced = true;

		let mut partitions = Vec::with_capacity(drained.len());
		let mut row_numbers = Vec::with_capacity(drained.len());
		let mut rows = Vec::with_capacity(drained.len());
		for (partition, rn, row) in drained {
			partitions.push(partition.unwrap_or_default());
			row_numbers.push(rn);
			rows.push(row);
		}
		let shape = self.get_or_load_shape(txn, &rows[0])?;
		let mut columns = append_rows(batch(self.storage_columns())?, &shape, rows, row_numbers)?;
		if self.system_columns.contains(&SystemColumn::Partitions) {
			columns = with_system_column(columns, SystemColumn::Partitions, partition_array(&partitions))?;
		}
		if self.keep_dictionary_ids {
			return Ok(Some(columns));
		}
		Ok(Some(decode_dictionary_columns(columns, &self.dictionaries, txn)?))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
