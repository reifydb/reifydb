// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, shape::RowShape, table::EncodedTableRow},
};
use reifydb_core::{
	common::CommitVersion,
	error::diagnostic,
	interface::{catalog::dictionary::Dictionary, resolved::ResolvedTable, store::MultiVersionRow},
	key::{
		row::{PartitionedRowKey, RowKey, RowKeyRange},
		typed::key::Key,
	},
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
	last_key: Option<EncodedKey>,
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

		Ok(Self {
			table,
			context: Some(context),
			headers,
			storage_types,
			dictionaries,
			shape: None,
			last_key: None,
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

	#[instrument(level = "trace", skip_all, name = "volcano::scan::table::range_open")]
	fn open_range<'rx, 'tx>(
		rx: &'rx mut Transaction<'tx>,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: u64,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + 'rx>> {
		rx.range(range, scope, batch_size as usize)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::table::drain")]
	fn drain_batch(
		stream: &mut dyn Iterator<Item = Result<MultiVersionRow>>,
		batch_size: u64,
		partitioned: bool,
	) -> Result<ScannedBatch> {
		let mut batch = ScannedBatch::default();

		for _ in 0..batch_size {
			match stream.next() {
				Some(Ok(multi)) => {
					let decoded = if partitioned {
						PartitionedRowKey::decode(&multi.key)
							.map(|k| (k.row, Some(k.partition)))
					} else {
						RowKey::decode(&multi.key).map(|k| (k.row, None))
					};
					if let Some((rn, partition)) = decoded {
						batch.rows.push(multi.bytes);
						batch.row_numbers.push(rn);
						if let Some(p) = partition {
							batch.partitions.push(p);
						}
						batch.last_key = Some(multi.key);
					}
				}
				Some(Err(e)) => return Err(e),
				None => {
					batch.exhausted = true;
					break;
				}
			}
		}

		Ok(batch)
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
				data: ColumnBuffer::none_typed(col.constraint.get_type(), 0),
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

#[derive(Default)]
struct ScannedBatch {
	rows: Vec<EncodedBytes>,
	row_numbers: Vec<RowNumber>,
	partitions: Vec<Partition>,
	last_key: Option<EncodedKey>,
	exhausted: bool,
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

		let partitioned = !self.table.def().partition_by.is_empty();
		let range = if partitioned {
			match self.partition {
				Some(partition) => PartitionedRowKey::partition_scan_range(
					self.table.def().id,
					partition,
					self.last_key.as_ref(),
				),
				None => PartitionedRowKey::scan_range(self.table.def().id, self.last_key.as_ref()),
			}
		} else {
			RowKeyRange::scan_range(self.table.def().id.into(), self.last_key.as_ref())
		};

		let scope = match self.min_commit_version {
			Some(v) => RangeScope::After(v),
			None => RangeScope::All,
		};

		let batch = {
			let mut stream = Self::open_range(rx, range, scope, batch_size)?;
			Self::drain_batch(&mut stream, batch_size, partitioned)?
		};

		if batch.exhausted {
			self.exhausted = true;
		}

		if batch.rows.is_empty() {
			self.exhausted = true;
			if self.last_key.is_none() {
				return Ok(Some(Columns::new(self.empty_columns())));
			}
			return Ok(None);
		}

		self.last_key = batch.last_key;

		let mut columns = Columns::with_system(self.storage_columns(), SystemColumns::default());
		self.append_batch(rx, &mut columns, batch.rows, batch.row_numbers)?;

		if partitioned {
			columns.system.set_partitions(batch.partitions);
		}

		decode_dictionary_columns(&mut columns, &self.dictionaries, rx)?;

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
