// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Bound, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_codec::row::{
	bytes::{EncodedBytes, read_fingerprint},
	shape::RowShape,
};
use reifydb_core::{
	interface::{
		catalog::{dictionary::Dictionary, view::ViewStorageKind},
		resolved::ResolvedView,
		store::MultiVersionRow,
	},
	internal_error,
	key::{
		any::TaggedKey,
		bound::TaggedKeyBoundRange,
		row::{
			PartitionedRowKey, PartitionedSortedViewRowKey, RowKeyRange, SortedViewRowKey,
			StoragePartitionedRowKey,
		},
		series::{PartitionedSeriesRowKeyRange, SeriesRowKeyRange},
	},
	value::{
		batch::{append_rows, batch, empty_for},
		column::{builder::ColumnBuilder, headers::ColumnHeaders},
	},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::{
	reifydb_assertions,
	value::{partition::Partition, row_number::RowNumber, system_columns::SystemColumn, value_type::ValueType},
};
use tracing::instrument;

use super::{
	super::{decode_dictionary_columns, user_pairs},
	empty_scan, guard_view_read,
	merge::{MergeLayout, PartitionMerge},
	scan_headers, source_system_columns, storage_partitioned_row,
};
use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

type DrainedBatch = (Vec<EncodedBytes>, Vec<RowNumber>, Option<TaggedKey>, bool);

type DrainedPartitionedBatch = (Vec<EncodedBytes>, Vec<RowNumber>, Option<StoragePartitionedRowKey>, bool);

enum Resume {
	Key(Option<TaggedKey>),
	Partitioned(Option<StoragePartitionedRowKey>),
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

pub(crate) struct ViewScanNode {
	view: ResolvedView,
	context: Option<Arc<QueryContext>>,
	headers: ColumnHeaders,
	storage_types: Vec<ValueType>,
	dictionaries: Vec<Option<Dictionary>>,
	shape: Option<RowShape>,
	resume: Resume,
	exhausted: bool,
	sorted: bool,
	partitioned: bool,
	series: bool,
	partition: Option<Partition>,
	system_columns: Vec<SystemColumn>,
	oldest_first: bool,
	merge: Option<PartitionMerge>,
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

		let system_columns = source_system_columns(false, true, false);
		let headers = scan_headers(view.columns().iter().map(|col| col.name.as_str()), &system_columns);
		let series = view.def().storage_kind() == ViewStorageKind::Series;
		let sorted = !view.def().sort().is_empty() && view.def().storage_kind() == ViewStorageKind::Table;
		let partitioned = !view.def().partition_by().is_empty();

		let resume = if partitioned && !series && !sorted {
			Resume::Partitioned(None)
		} else {
			Resume::Key(None)
		};

		Ok(Self {
			view,
			context: Some(context),
			headers,
			storage_types,
			dictionaries,
			shape: None,
			resume,
			exhausted: false,
			sorted,
			partitioned,
			series,
			partition,
			system_columns,
			oldest_first: false,
			merge: None,
		})
	}

	pub(crate) fn oldest_first(mut self) -> Self {
		self.oldest_first = !self.sorted;
		self
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
		range: TaggedKeyBoundRange,
		batch_size: u64,
		oldest_first: bool,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + 'rx>> {
		if oldest_first {
			return rx.range_rev(range, RangeScope::All, batch_size as usize);
		}
		rx.range(range, RangeScope::All, batch_size as usize)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::view::drain")]
	fn drain_batch(
		&self,
		stream: &mut dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>>,
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
							match &multi.key {
								TaggedKey::PartitionedSeriesRow(key) => {
									RowNumber(key.sequence)
								}
								_ => continue,
							}
						} else {
							match &multi.key {
								TaggedKey::SeriesRow(key) => RowNumber(key.sequence),
								_ => continue,
							}
						}
					} else if self.sorted {
						let row = if self.partitioned {
							match &multi.key {
								TaggedKey::PartitionedSortedViewRow(key) => {
									Some(key.row.0)
								}
								_ => None,
							}
						} else {
							match &multi.key {
								TaggedKey::SortedViewRow(key) => Some(key.row.0),
								_ => None,
							}
						};
						match row {
							Some(row) => row,
							None => continue,
						}
					} else if let TaggedKey::Row(key) = &multi.key {
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

	#[instrument(level = "trace", skip_all, name = "volcano::scan::view::drain_partitioned")]
	fn drain_batch_partitioned(
		stream: &mut dyn Iterator<Item = Result<MultiVersionRow<StoragePartitionedRowKey>>>,
		batch_size: u64,
	) -> Result<DrainedPartitionedBatch> {
		let mut batch = Vec::new();
		let mut row_numbers = Vec::new();
		let mut new_last_key = None;
		let mut drained = false;

		for _ in 0..batch_size {
			match stream.next() {
				Some(Ok(multi)) => {
					batch.push(multi.bytes);
					row_numbers.push(multi.key.row());
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
	fn storage_columns(&self) -> Vec<(FieldRef, ArrayRef)> {
		self.view
			.columns()
			.iter()
			.enumerate()
			.map(|(idx, col)| {
				ColumnBuilder::with_capacity(self.storage_types[idx].clone(), 0).finish(&col.name)
			})
			.collect()
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::view::append_rows")]
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

impl QueryNode for ViewScanNode {
	#[instrument(name = "volcano::scan::view::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		guard_view_read(&self.view, rx, &ctx.services)
	}

	#[instrument(name = "volcano::scan::view::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		reifydb_assertions! {
			assert!(self.context.is_some(), "ViewScanNode::next() called before initialize()");
		}
		let stored_ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = stored_ctx.batch_size;
		let storage = self.view.def().storage_id();

		let (batch_rows, row_numbers, next_resume, resumed, drained) = match &self.resume {
			Resume::Partitioned(last) => {
				let last = *last;
				let (batch, row_numbers, new_last_key, drained) = if self.oldest_first {
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
					Self::drain_batch_partitioned(
						&mut rows.into_iter().map(storage_partitioned_row),
						batch_size,
					)?
				} else {
					let (start, end) = partitioned_bounds(self.partition, last);
					let mut stream = rx.range_partitioned_row(
						storage,
						start,
						end,
						RangeScope::All,
						batch_size as usize,
					)?;
					Self::drain_batch_partitioned(&mut stream, batch_size)?
				};
				(batch, row_numbers, Resume::Partitioned(new_last_key), last.is_some(), drained)
			}
			Resume::Key(last) if self.oldest_first && self.series && self.partitioned => {
				let partition = self.partition;
				let merge = self.merge.get_or_insert_with(|| {
					PartitionMerge::new(
						MergeLayout::Series,
						storage,
						partition.map(|partition| {
							PartitionedSeriesRowKeyRange::partition_range(
								storage, partition,
							)
						}),
					)
				});
				let rows = merge.next(rx, batch_size)?;
				let resumed = last.is_some();
				let (batch, row_numbers, new_last_key, drained) =
					self.drain_batch(&mut rows.into_iter().map(Ok), batch_size)?;
				(batch, row_numbers, Resume::Key(new_last_key), resumed, drained)
			}
			Resume::Key(last) => {
				let after = if self.oldest_first {
					None
				} else {
					last.as_ref()
				};
				let range = match (self.series, self.partitioned, self.partition) {
					(true, true, Some(partition)) => {
						PartitionedSeriesRowKeyRange::partition_scan_range(
							storage, partition, after,
						)
					}
					(true, true, None) => {
						PartitionedSeriesRowKeyRange::full_scan_range(storage, after)
					}
					(true, false, _) => {
						SeriesRowKeyRange::scan_range(storage, false, None, None, None, after)
					}
					(false, true, Some(partition)) if self.sorted => {
						PartitionedSortedViewRowKey::partition_scan_range(
							storage, partition, after,
						)
					}
					(false, true, None) if self.sorted => {
						PartitionedSortedViewRowKey::scan_range(storage, after)
					}
					(false, false, _) if self.sorted => {
						SortedViewRowKey::scan_range(storage, after)
					}
					(false, false, _) => RowKeyRange::scan_range(storage, after),
					(false, true, _) => unreachable!(
						"unsorted partitioned view rows resume through Resume::Partitioned"
					),
				};
				let range = if self.oldest_first {
					range.resume_before(last.as_ref())
				} else {
					range
				};

				let resumed = last.is_some();
				let (batch, row_numbers, new_last_key, drained) = {
					let mut stream = Self::open_range(rx, range, batch_size, self.oldest_first)?;
					self.drain_batch(&mut stream, batch_size)?
				};
				(batch, row_numbers, Resume::Key(new_last_key), resumed, drained)
			}
		};

		if drained {
			self.exhausted = true;
		}

		if batch_rows.is_empty() {
			self.exhausted = true;
			if !resumed {
				let user = user_pairs(&empty_for(self.view.columns())?);
				return Ok(Some(empty_scan(user, &self.system_columns)?));
			}
			return Ok(None);
		}

		self.resume = next_resume;

		let columns = batch(self.storage_columns())?;
		let columns = self.append_batch(rx, columns, batch_rows, row_numbers)?;

		Ok(Some(decode_dictionary_columns(columns, &self.dictionaries, rx)?))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
