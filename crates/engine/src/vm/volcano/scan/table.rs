// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Bound, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::FieldRef;
use reifydb_codec::row::{bytes::EncodedBytes, shape::RowShape, table::EncodedTableRow};
use reifydb_core::{
	common::TimeSource,
	error::diagnostic,
	interface::{
		catalog::{dictionary::Dictionary, storage::StorageId},
		resolved::ResolvedTable,
		store::MultiVersionRow,
	},
	key::{
		any::TaggedKey,
		row::{PartitionedRowKey, RowKeyRange, StoragePartitionedRowKey, StorageRowKey},
	},
	value::{
		batch::{append_rows, batch},
		column::{builder::ColumnBuilder, headers::ColumnHeaders},
	},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::{
	error, reifydb_assertions,
	value::{
		partition::Partition,
		row_number::RowNumber,
		system_columns::{SystemColumn, stamp_system_columns},
		value_type::ValueType,
	},
};
use tracing::instrument;

use super::{
	super::decode_dictionary_columns,
	empty_scan,
	merge::{MergeLayout, PartitionMerge},
	partition_array, scan_headers, source_system_columns, storage_partitioned_row, storage_row,
};
use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub struct TableScanNode {
	table: ResolvedTable,
	context: Option<Arc<QueryContext>>,
	headers: ColumnHeaders,

	storage_types: Vec<ValueType>,

	dictionaries: Vec<Option<(Dictionary, ValueType)>>,

	shape: Option<RowShape>,
	resume: Resume,
	exhausted: bool,

	partition: Option<Partition>,

	system_columns: Vec<SystemColumn>,

	oldest_first: bool,

	merge: Option<PartitionMerge>,

	storage_batch: Option<RecordBatch>,
}

impl TableScanNode {
	pub fn new(
		table: ResolvedTable,
		partition: Option<Partition>,
		context: Arc<QueryContext>,
		rx: &mut Transaction<'_>,
	) -> Result<Self> {
		let mut storage_types = Vec::with_capacity(table.columns().len());
		let mut dictionaries = Vec::with_capacity(table.columns().len());

		for col in table.columns() {
			if let Some(dict_id) = col.dictionary_id {
				if let Some(dict) = context.services.catalog.find_dictionary(rx, dict_id)? {
					storage_types.push(ValueType::DictionaryId);
					dictionaries.push(Some((dict, col.constraint.get_type())));
				} else {
					storage_types.push(col.constraint.get_type());
					dictionaries.push(None);
				}
			} else {
				storage_types.push(col.constraint.get_type());
				dictionaries.push(None);
			}
		}

		let system_columns = source_system_columns(
			!table.def().partition_by.is_empty(),
			table.def().time != TimeSource::None,
			true,
		);
		let headers = scan_headers(table.columns().iter().map(|col| col.name.as_str()), &system_columns);

		let resume = if table.def().partition_by.is_empty() {
			Resume::Row(None)
		} else {
			Resume::Partitioned(None)
		};

		Ok(Self {
			table,
			context: Some(context),
			headers,
			storage_types,
			dictionaries,
			shape: None,
			resume,
			exhausted: false,
			partition,
			system_columns,
			oldest_first: false,
			merge: None,
			storage_batch: None,
		})
	}

	pub(crate) fn oldest_first(mut self) -> Self {
		self.oldest_first = true;
		self
	}

	fn get_or_load_shape<'a>(&mut self, rx: &mut Transaction<'a>, first: &EncodedBytes) -> Result<RowShape> {
		if let Some(shape) = &self.shape {
			return Ok(shape.clone());
		}

		let fingerprint = EncodedTableRow::view(first).fingerprint();

		let stored_ctx = self.context.as_ref().expect("TableScanNode context not set");
		let shape = stored_ctx.services.catalog.get_or_load_row_shape(fingerprint, rx)?.ok_or_else(|| {
			error!(diagnostic::internal::internal(format!(
				"RowShape with fingerprint {:?} not found for table {}",
				fingerprint,
				self.table.def().name
			)))
		})?;

		self.shape = Some(shape.clone());

		Ok(shape)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::table::drain_partitioned")]
	fn drain_batch_partitioned(
		stream: &mut dyn Iterator<Item = Result<MultiVersionRow<StoragePartitionedRowKey>>>,
		batch_size: u64,
	) -> Result<(ScannedBatch, Option<StoragePartitionedRowKey>)> {
		let mut batch = ScannedBatch::default();
		let mut last = None;

		for _ in 0..batch_size {
			match stream.next() {
				Some(Ok(multi)) => {
					batch.rows.push(multi.bytes);
					batch.row_numbers.push(multi.key.row());
					batch.partitions.push(multi.key.partition());
					batch.commit_versions.push(multi.version.0);
					last = Some(multi.key);
				}
				Some(Err(e)) => return Err(e),
				None => {
					batch.exhausted = true;
					break;
				}
			}
		}

		Ok((batch, last))
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::table::drain_row")]
	fn drain_batch_row(
		stream: &mut dyn Iterator<Item = Result<MultiVersionRow<StorageRowKey>>>,
		batch_size: u64,
	) -> Result<(ScannedBatch, Option<StorageRowKey>)> {
		let mut batch = ScannedBatch::default();
		let mut last = None;

		for _ in 0..batch_size {
			match stream.next() {
				Some(Ok(multi)) => {
					batch.rows.push(multi.bytes);
					batch.row_numbers.push(multi.key.row());
					batch.commit_versions.push(multi.version.0);
					last = Some(multi.key);
				}
				Some(Err(e)) => return Err(e),
				None => {
					batch.exhausted = true;
					break;
				}
			}
		}

		Ok((batch, last))
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::table::column_alloc")]
	fn storage_columns(&self) -> Vec<(FieldRef, ArrayRef)> {
		self.table
			.columns()
			.iter()
			.enumerate()
			.map(|(idx, col)| {
				ColumnBuilder::with_capacity(self.storage_types[idx].clone(), 0).finish(&col.name)
			})
			.collect()
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::table::empty_columns")]
	fn empty_columns(&self) -> Vec<(FieldRef, ArrayRef)> {
		self.table
			.columns()
			.iter()
			.map(|col| ColumnBuilder::with_capacity(col.constraint.get_type(), 0).finish(&col.name))
			.collect()
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::table::append_rows")]
	fn append_batch<'a>(
		&mut self,
		rx: &mut Transaction<'a>,
		columns: RecordBatch,
		bytes_vec: Vec<EncodedBytes>,
		row_numbers: Vec<RowNumber>,
	) -> Result<RecordBatch> {
		let shape = self.get_or_load_shape(rx, &bytes_vec[0])?;
		append_rows(columns, &shape, bytes_vec, row_numbers)
	}
}

#[derive(Clone, Copy)]
enum Resume {
	Row(Option<StorageRowKey>),
	Partitioned(Option<StoragePartitionedRowKey>),
}

#[derive(Default)]
struct ScannedBatch {
	rows: Vec<EncodedBytes>,
	row_numbers: Vec<RowNumber>,
	partitions: Vec<Partition>,
	commit_versions: Vec<u64>,
	exhausted: bool,
}

fn partitioned_bounds(
	partition: Option<Partition>,
	last: Option<StoragePartitionedRowKey>,
) -> (Bound<StoragePartitionedRowKey>, Bound<StoragePartitionedRowKey>) {
	let start = match (last, partition) {
		(Some(key), _) => Bound::Excluded(key),
		(None, Some(partition)) => {
			Bound::Included(StoragePartitionedRowKey::new(partition, RowNumber(u64::MAX)))
		}
		(None, None) => Bound::Unbounded,
	};
	let end = match partition {
		Some(partition) => Bound::Included(StoragePartitionedRowKey::new(partition, RowNumber(u64::MIN))),
		None => Bound::Unbounded,
	};
	(start, end)
}

impl QueryNode for TableScanNode {
	#[instrument(level = "trace", skip_all, name = "volcano::scan::table::initialize")]
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::table::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		reifydb_assertions! {
			assert!(self.context.is_some(), "TableScanNode::next() called before initialize()");
		}
		let stored_ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = stored_ctx.batch_size;

		let scope = RangeScope::All;

		let storage: StorageId = self.table.def().id.into();

		let probe_storage = std::time::Instant::now();
		let (scanned, next_resume, resumed) = match self.resume {
			Resume::Partitioned(last) if self.oldest_first => {
				let partition = self.partition;
				let merge = self.merge.get_or_insert_with(|| {
					PartitionMerge::new(
						MergeLayout::Row,
						storage,
						partition.map(|partition| {
							PartitionedRowKey::partition_range(storage, partition)
						}),
					)
				});
				let rows = merge.next(rx, batch_size)?;
				let (scanned, new_last) = Self::drain_batch_partitioned(
					&mut rows.into_iter().map(storage_partitioned_row),
					batch_size,
				)?;
				(scanned, Resume::Partitioned(new_last), last.is_some())
			}
			Resume::Partitioned(last) => {
				let (start, end) = partitioned_bounds(self.partition, last);
				let (scanned, new_last) = {
					let mut stream = rx.range_partitioned_row(
						storage,
						start,
						end,
						scope,
						batch_size as usize,
					)?;
					Self::drain_batch_partitioned(&mut stream, batch_size)?
				};
				(scanned, Resume::Partitioned(new_last), last.is_some())
			}
			Resume::Row(last) if self.oldest_first => {
				let last_key = last.map(|key| TaggedKey::Row(key.with_storage(storage)));
				let range = RowKeyRange::scan_range_rev(storage, last_key.as_ref());
				let (scanned, new_last) = {
					let mut stream = rx
						.range_rev(range, scope, batch_size as usize)?
						.map(|row| row.and_then(storage_row));
					Self::drain_batch_row(&mut stream, batch_size)?
				};
				(scanned, Resume::Row(new_last), last.is_some())
			}
			Resume::Row(last) => {
				let start = match last {
					Some(key) => Bound::Excluded(key),
					None => Bound::Unbounded,
				};
				let (scanned, new_last) = {
					let mut stream = rx.range_row(
						storage,
						start,
						Bound::Unbounded,
						scope,
						batch_size as usize,
					)?;
					Self::drain_batch_row(&mut stream, batch_size)?
				};
				(scanned, Resume::Row(new_last), last.is_some())
			}
		};
		crate::probe::add(&crate::probe::SCAN_STORAGE_NS, probe_storage);
		crate::probe::bump(&crate::probe::SCAN_CALLS, 1);
		crate::probe::bump(&crate::probe::SCAN_ROWS, scanned.rows.len() as u64);

		if scanned.exhausted {
			self.exhausted = true;
		}

		if scanned.rows.is_empty() {
			self.exhausted = true;
			if !resumed {
				return Ok(Some(empty_scan(self.empty_columns(), &self.system_columns)?));
			}
			return Ok(None);
		}

		self.resume = next_resume;

		let probe_decode = std::time::Instant::now();
		let columns = match &self.storage_batch {
			Some(storage) => storage.clone(),
			None => {
				let storage = batch(self.storage_columns())?;
				self.storage_batch = Some(storage.clone());
				storage
			}
		};
		let columns = self.append_batch(rx, columns, scanned.rows, scanned.row_numbers)?;
		crate::probe::add(&crate::probe::SCAN_DECODE_NS, probe_decode);

		let probe_stamp = std::time::Instant::now();
		let mut stamps: Vec<(SystemColumn, ArrayRef)> = Vec::new();
		if !scanned.partitions.is_empty() {
			stamps.push((SystemColumn::Partitions, partition_array(&scanned.partitions)));
		}
		stamps.push((SystemColumn::CommitVersion, Arc::new(UInt64Array::from(scanned.commit_versions))));
		let columns = stamp_system_columns(columns, stamps)?;

		let decoded = decode_dictionary_columns(columns, &self.dictionaries, rx)?;
		crate::probe::add(&crate::probe::SCAN_STAMP_NS, probe_stamp);
		Ok(Some(decoded))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
