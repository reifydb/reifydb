// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Bound, sync::Arc};

use reifydb_codec::row::{bytes::EncodedBytes, shape::RowShape, table::EncodedTableRow};
use reifydb_core::{
	common::CommitVersion,
	error::diagnostic,
	interface::{
		catalog::{dictionary::Dictionary, storage::StorageId},
		resolved::ResolvedTable,
		store::MultiVersionRow,
	},
	key::row::{StoragePartitionedRowKey, StorageRowKey},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::{
	error,
	fragment::Fragment,
	reifydb_assertions,
	value::{partition::Partition, row_number::RowNumber, system_columns::SystemColumns, value_type::ValueType},
};
use tracing::instrument;

use super::super::decode_dictionary_columns;
use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub struct TableScanNode {
	table: ResolvedTable,
	context: Option<Arc<QueryContext>>,
	headers: ColumnHeaders,

	storage_types: Vec<ValueType>,

	dictionaries: Vec<Option<Dictionary>>,

	shape: Option<RowShape>,
	resume: Resume,
	exhausted: bool,

	partition: Option<Partition>,

	min_commit_version: Option<CommitVersion>,
}

impl TableScanNode {
	pub fn with_min_commit_version(mut self, min_commit_version: Option<CommitVersion>) -> Self {
		self.min_commit_version = min_commit_version;
		self
	}

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

		let headers = ColumnHeaders {
			columns: table.columns().iter().map(|col| Fragment::internal(&col.name)).collect(),
		};

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
			min_commit_version: None,
		})
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
	fn storage_columns(&self) -> Vec<ColumnWithName> {
		self.table
			.columns()
			.iter()
			.enumerate()
			.map(|(idx, col)| ColumnWithName {
				name: Fragment::internal(&col.name),
				data: ColumnBuffer::with_capacity(self.storage_types[idx].clone(), 0),
			})
			.collect()
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::table::empty_columns")]
	fn empty_columns(&self) -> Vec<ColumnWithName> {
		self.table
			.columns()
			.iter()
			.map(|col| ColumnWithName {
				name: Fragment::internal(&col.name),
				data: ColumnBuffer::with_capacity(col.constraint.get_type(), 0),
			})
			.collect()
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::table::append_rows")]
	fn append_batch<'a>(
		&mut self,
		rx: &mut Transaction<'a>,
		columns: &mut Columns,
		bytes_vec: Vec<EncodedBytes>,
		row_numbers: Vec<RowNumber>,
	) -> Result<()> {
		let shape = self.get_or_load_shape(rx, &bytes_vec[0])?;
		columns.append_rows(&shape, bytes_vec.into_iter(), row_numbers)?;
		Ok(())
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
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		reifydb_assertions! {
			assert!(self.context.is_some(), "TableScanNode::next() called before initialize()");
		}
		let stored_ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = stored_ctx.batch_size;

		let scope = match self.min_commit_version {
			Some(v) => RangeScope::After(v),
			None => RangeScope::All,
		};

		let storage: StorageId = self.table.def().id.into();

		let (scanned, next_resume, resumed) = match self.resume {
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

		if scanned.exhausted {
			self.exhausted = true;
		}

		if scanned.rows.is_empty() {
			self.exhausted = true;
			if !resumed {
				return Ok(Some(Columns::new(self.empty_columns())));
			}
			return Ok(None);
		}

		self.resume = next_resume;

		let mut columns = Columns::with_system(self.storage_columns(), SystemColumns::default());
		self.append_batch(rx, &mut columns, scanned.rows, scanned.row_numbers)?;

		if !scanned.partitions.is_empty() {
			columns.system.set_partitions(scanned.partitions);
		}

		decode_dictionary_columns(&mut columns, &self.dictionaries, rx)?;

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
