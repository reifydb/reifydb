// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{
		bytes::{EncodedBytes, read_fingerprint},
		shape::RowShape,
	},
};
use reifydb_core::{
	interface::{
		catalog::{dictionary::Dictionary, view::ViewStorageKind},
		resolved::ResolvedView,
		store::MultiVersionRow,
	},
	internal_error,
	key::{
		row::{PartitionedRowKey, RowKey, RowKeyRange},
		series::{PartitionedSeriesRowKey, PartitionedSeriesRowKeyRange, SeriesRowKey, SeriesRowKeyRange},
		typed::key::Key,
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::{
	fragment::Fragment,
	reifydb_assertions,
	value::{partition::Partition, row_number::RowNumber, system_columns::SystemColumns, value_type::ValueType},
};
use tracing::instrument;

use super::{super::decode_dictionary_columns, guard_view_read};
use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

type DrainedBatch = (Vec<EncodedBytes>, Vec<RowNumber>, Option<EncodedKey>, bool);

pub(crate) struct ViewScanNode {
	view: ResolvedView,
	context: Option<Arc<QueryContext>>,
	headers: ColumnHeaders,
	storage_types: Vec<ValueType>,
	dictionaries: Vec<Option<Dictionary>>,
	shape: Option<RowShape>,
	last_key: Option<EncodedKey>,
	exhausted: bool,
	sorted: bool,
	partitioned: bool,
	series: bool,
	partition: Option<Partition>,
}

impl ViewScanNode {
	pub fn new(
		view: ResolvedView,
		partition: Option<Partition>,
		context: Arc<QueryContext>,
		rx: &mut Transaction<'_>,
	) -> Result<Self> {
		let mut storage_types = Vec::with_capacity(view.columns().len());
		let mut dictionaries = Vec::with_capacity(view.columns().len());

		for col in view.columns() {
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
			columns: view.columns().iter().map(|col| Fragment::internal(&col.name)).collect(),
		};
		let series = view.def().storage_kind() == ViewStorageKind::Series;
		let sorted = !view.def().sort().is_empty();
		let partitioned = !view.def().partition_by().is_empty();

		Ok(Self {
			view,
			context: Some(context),
			headers,
			storage_types,
			dictionaries,
			shape: None,
			last_key: None,
			exhausted: false,
			sorted,
			partitioned,
			series,
			partition,
		})
	}

	fn get_or_load_shape<'a>(&mut self, rx: &mut Transaction<'a>, first: &EncodedBytes) -> Result<RowShape> {
		if let Some(shape) = &self.shape {
			return Ok(shape.clone());
		}

		let fingerprint = read_fingerprint(first);

		let stored_ctx = self.context.as_ref().expect("ViewScanNode context not set");
		let shape = stored_ctx.services.catalog.get_or_load_row_shape(fingerprint, rx)?.ok_or_else(|| {
			internal_error!(
				"RowShape with fingerprint {:?} not found for view {}",
				fingerprint,
				self.view.def().name()
			)
		})?;

		self.shape = Some(shape.clone());

		Ok(shape)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::view::range_open")]
	fn open_range<'rx, 'tx>(
		rx: &'rx mut Transaction<'tx>,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + 'rx>> {
		rx.range(range, RangeScope::All, batch_size as usize)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::view::drain")]
	fn drain_batch(
		&self,
		stream: &mut dyn Iterator<Item = Result<MultiVersionRow>>,
		batch_size: u64,
	) -> Result<DrainedBatch> {
		let mut batch = Vec::new();
		let mut row_numbers = Vec::new();
		let mut new_last_key = None;
		let mut drained = false;

		for _ in 0..batch_size {
			match stream.next() {
				Some(Ok(multi)) => {
					let row = if self.series {
						if self.partitioned {
							match PartitionedSeriesRowKey::decode(&multi.key) {
								Some(key) => RowNumber(key.sequence),
								None => continue,
							}
						} else {
							match SeriesRowKey::decode(&multi.key) {
								Some(key) => RowNumber(key.sequence),
								None => continue,
							}
						}
					} else if self.sorted {
						let bytes = multi.key.as_slice();
						RowNumber(u64::from_be_bytes(
							bytes[bytes.len() - 8..].try_into().unwrap(),
						))
					} else if self.partitioned {
						match PartitionedRowKey::decode(&multi.key) {
							Some(key) => key.row,
							None => continue,
						}
					} else if let Some(key) = RowKey::decode(&multi.key) {
						key.row
					} else {
						continue;
					};
					batch.push(multi.bytes);
					row_numbers.push(row);
					new_last_key = Some(multi.key);
				}
				Some(Err(e)) => return Err(e),
				None => {
					drained = true;
					break;
				}
			}
		}

		Ok((batch, row_numbers, new_last_key, drained))
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::view::column_alloc")]
	fn storage_columns(&self) -> Vec<ColumnWithName> {
		self.view
			.columns()
			.iter()
			.enumerate()
			.map(|(idx, col)| ColumnWithName {
				name: Fragment::internal(&col.name),
				data: ColumnBuffer::with_capacity(self.storage_types[idx].clone(), 0),
			})
			.collect()
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::view::append_rows")]
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

impl QueryNode for ViewScanNode {
	#[instrument(name = "volcano::scan::view::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		guard_view_read(&self.view, rx, &ctx.services)
	}

	#[instrument(name = "volcano::scan::view::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		reifydb_assertions! {
			assert!(self.context.is_some(), "ViewScanNode::next() called before initialize()");
		}
		let stored_ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = stored_ctx.batch_size;
		let storage = self.view.def().storage_id();
		let range = match (self.series, self.partitioned, self.partition) {
			(true, true, Some(partition)) => PartitionedSeriesRowKeyRange::partition_scan_range(
				storage,
				partition,
				self.last_key.as_ref(),
			),
			(true, true, None) => {
				PartitionedSeriesRowKeyRange::full_scan_range(storage, self.last_key.as_ref())
			}
			(true, false, _) => {
				SeriesRowKeyRange::scan_range(storage, None, None, None, self.last_key.as_ref())
			}
			(false, true, Some(partition)) => {
				PartitionedRowKey::partition_scan_range(storage, partition, self.last_key.as_ref())
			}
			(false, true, None) => PartitionedRowKey::scan_range(storage, self.last_key.as_ref()),
			(false, false, _) => RowKeyRange::scan_range(storage, self.last_key.as_ref()),
		};

		let (batch, row_numbers, new_last_key, drained) = {
			let mut stream = Self::open_range(rx, range, batch_size)?;
			self.drain_batch(&mut stream, batch_size)?
		};

		if drained {
			self.exhausted = true;
		}

		if batch.is_empty() {
			self.exhausted = true;
			if self.last_key.is_none() {
				return Ok(Some(Columns::from_catalog_columns(self.view.columns())));
			}
			return Ok(None);
		}

		self.last_key = new_last_key;

		let mut columns = Columns::with_system(self.storage_columns(), SystemColumns::default());
		self.append_batch(rx, &mut columns, batch, row_numbers)?;

		decode_dictionary_columns(&mut columns, &self.dictionaries, rx)?;

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
