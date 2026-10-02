// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{FieldRef, SchemaRef};
use reifydb_codec::row::{series::EncodedSeriesRow, shape::RowShape};
use reifydb_core::{
	common::TimeSource,
	interface::{catalog::storage::StorageId, resolved::ResolvedSeries, store::MultiVersionRow},
	key::{
		any::TaggedKey,
		bound::TaggedKeyBoundRange,
		series::{PartitionedSeriesRowKeyRange, SeriesRowKeyRange},
	},
	value::{
		batch::{batch, batch_with},
		column::{
			builder::ColumnBuilder,
			factory::{datetime, none_typed, uint1},
			headers::ColumnHeaders,
		},
	},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::{
	reifydb_assertions,
	value::{
		Value,
		datetime::DateTime,
		dictionary::DictionaryEntryId,
		partition::Partition,
		system_columns::{SystemColumn, system_field},
		value_type::ValueType,
	},
};
use tracing::instrument;

use super::{
	empty_scan,
	merge::{MergeLayout, PartitionMerge},
	partition_array, scan_headers, source_system_columns,
};
use crate::{
	Result,
	transaction::operation::dictionary::DictionaryOperations,
	vm::{
		instruction::dml::shape::get_or_create_series_shape,
		volcano::query::{QueryContext, QueryNode},
	},
};

pub struct SeriesScanNode {
	series: ResolvedSeries,
	key_range_start: Option<u64>,
	key_range_end: Option<u64>,
	variant_tag: Option<u8>,
	partition: Option<Partition>,
	context: Option<Arc<QueryContext>>,
	headers: ColumnHeaders,
	last_key: Option<TaggedKey>,
	exhausted: bool,
	system_columns: Vec<SystemColumn>,
	oldest_first: bool,
	merge: Option<PartitionMerge>,
	schema: Option<SchemaRef>,
}

impl SeriesScanNode {
	pub fn new(
		series: ResolvedSeries,
		key_range_start: Option<u64>,
		key_range_end: Option<u64>,
		variant_tag: Option<u8>,
		partition: Option<Partition>,
		context: Arc<QueryContext>,
	) -> Result<Self> {
		let mut columns = vec![series.def().key.column()];
		if series.def().tag.is_some() {
			columns.push("tag");
		}
		for col in series.columns() {
			columns.push(col.name.as_str());
		}
		let system_columns = source_system_columns(
			!series.def().partition_by.is_empty(),
			series.def().time != TimeSource::None,
			false,
		);
		let headers = scan_headers(columns.into_iter(), &system_columns);

		Ok(Self {
			series,
			key_range_start,
			key_range_end,
			variant_tag,
			partition,
			context: Some(context),
			headers,
			last_key: None,
			exhausted: false,
			system_columns,
			oldest_first: false,
			merge: None,
			schema: None,
		})
	}

	pub(crate) fn oldest_first(mut self) -> Self {
		self.oldest_first = true;
		self
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::series::range_open")]
	fn open_range<'rx, 'tx>(
		rx: &'rx mut Transaction<'tx>,
		range: TaggedKeyBoundRange,
		scope: RangeScope,
		batch_size: u64,
		oldest_first: bool,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + 'rx>> {
		if oldest_first {
			return rx.range_rev(range, scope, batch_size as usize);
		}
		rx.range(range, scope, batch_size as usize)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::series::drain")]
	fn drain_batch(
		stream: &mut dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>>,
		batch_size: u64,
		partitioned: bool,
		has_tag: bool,
		data_column_count: usize,
		read_shape: &RowShape,
	) -> Result<SeriesBatch> {
		let mut batch = SeriesBatch::default();
		let mut count = 0;

		for entry in stream {
			let entry = entry?;

			let decoded: Option<(u64, u64, Option<u8>, Option<Partition>)> = if partitioned {
				match &entry.key {
					TaggedKey::PartitionedSeriesRow(pk) => {
						Some((pk.key, pk.sequence, pk.variant_tag, Some(pk.partition)))
					}
					_ => None,
				}
			} else {
				match &entry.key {
					TaggedKey::SeriesRow(k) => Some((k.key, k.sequence, k.variant_tag, None)),
					_ => None,
				}
			};

			if let Some((key_val, sequence, variant_tag, partition)) = decoded {
				batch.key_values.push(key_val);
				batch.sequences.push(sequence);
				if let Some(p) = partition {
					batch.partitions.push(p);
				}
				let row = EncodedSeriesRow::view(&entry.bytes);
				batch.created_at_values.push(row.created_at());
				if let Some(time) = row.time() {
					batch.time_values.push(time);
				}
				batch.updated_at_values.push(row.updated_at());
				if has_tag {
					batch.tags.push(variant_tag.unwrap_or(0));
				}

				let mut values = Vec::with_capacity(data_column_count);
				for i in 0..data_column_count {
					values.push(read_shape.get_value(&entry.bytes, i + 1));
				}
				batch.data_rows.push(values);

				batch.last_key = Some(entry.key.clone());
				count += 1;
				if count >= batch_size as usize {
					break;
				}
			}
		}

		Ok(batch)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::series::empty_columns")]
	fn empty_columns(&self, has_tag: bool) -> Vec<(FieldRef, ArrayRef)> {
		let series = self.series.def();
		let key_type = series
			.columns
			.iter()
			.find(|c| c.name == series.key.column())
			.map(|c| c.constraint.get_type())
			.unwrap_or(ValueType::Int8);

		let mut result_columns = Vec::new();
		result_columns.push(none_typed(series.key.column(), key_type, 0));
		if has_tag {
			result_columns.push(none_typed("tag", ValueType::Uint1, 0));
		}
		for col_def in series.data_columns() {
			result_columns.push(none_typed(&col_def.name, col_def.constraint.get_type(), 0));
		}
		result_columns
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::series::assemble")]
	fn assemble<'a>(
		&mut self,
		rx: &mut Transaction<'a>,
		stored_ctx: &QueryContext,
		scanned: SeriesBatch,
		has_tag: bool,
		partitioned: bool,
	) -> Result<Option<RecordBatch>> {
		let series = self.series.def();
		let mut result_columns = Vec::new();

		result_columns.push(series.key_column_data(scanned.key_values));

		if has_tag {
			result_columns.push(uint1("tag", scanned.tags));
		}

		for (col_idx, col_def) in series.data_columns().enumerate() {
			let col_type = col_def.constraint.get_type();
			let mut col_values: Vec<Value> = scanned
				.data_rows
				.iter()
				.map(|row| row.get(col_idx).cloned().unwrap_or(Value::none()))
				.collect();

			if let Some(dict_id) = col_def.dictionary_id
				&& let Some(dictionary) = stored_ctx.services.catalog.find_dictionary(rx, dict_id)?
			{
				for value in col_values.iter_mut() {
					if let Some(entry_id) = DictionaryEntryId::from_value(value) {
						*value = rx
							.get_from_dictionary(&dictionary, entry_id)?
							.unwrap_or_else(Value::none);
					}
				}
			}

			result_columns.push(build_data_column(&col_def.name, &col_values, col_type)?);
		}

		let mut stamps: Vec<(SystemColumn, ArrayRef)> =
			vec![(SystemColumn::RowNumbers, Arc::new(UInt64Array::from(scanned.sequences)))];
		if partitioned {
			stamps.push((SystemColumn::Partitions, partition_array(&scanned.partitions)));
		}
		stamps.push((
			SystemColumn::CreatedAt,
			datetime(SystemColumn::CreatedAt.name(), scanned.created_at_values).1,
		));
		stamps.push((
			SystemColumn::UpdatedAt,
			datetime(SystemColumn::UpdatedAt.name(), scanned.updated_at_values).1,
		));
		if !scanned.time_values.is_empty() {
			stamps.push((SystemColumn::Time, datetime(SystemColumn::Time.name(), scanned.time_values).1));
		}
		let row_count = result_columns[0].1.len();
		result_columns.extend(stamps
			.into_iter()
			.map(|(column, array)| (system_field(column, array.logical_null_count() > 0), array)));
		let out = match &self.schema {
			Some(schema) => batch_with(schema, result_columns, row_count)?,
			None => batch(result_columns)?,
		};
		self.schema = Some(out.schema());
		Ok(Some(out))
	}
}

#[derive(Default)]
struct SeriesBatch {
	key_values: Vec<u64>,
	tags: Vec<u8>,
	sequences: Vec<u64>,
	partitions: Vec<Partition>,
	created_at_values: Vec<DateTime>,
	time_values: Vec<DateTime>,
	updated_at_values: Vec<DateTime>,
	data_rows: Vec<Vec<Value>>,
	last_key: Option<TaggedKey>,
}

impl QueryNode for SeriesScanNode {
	#[instrument(name = "volcano::scan::series::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		Ok(())
	}

	#[instrument(name = "volcano::scan::series::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		reifydb_assertions! {
			assert!(self.context.is_some(), "SeriesScanNode::next() called before initialize()");
		}
		let stored_ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = stored_ctx.batch_size;
		let series = self.series.def();
		let has_tag = series.tag.is_some();

		let partitioned = !series.partition_by.is_empty();
		let storage = StorageId::series(series.id);
		let after = if self.oldest_first {
			None
		} else {
			self.last_key.as_ref()
		};
		let range = if partitioned {
			match self.partition {
				Some(partition) => PartitionedSeriesRowKeyRange::scan_range(
					storage,
					partition,
					has_tag,
					self.variant_tag,
					self.key_range_start,
					self.key_range_end,
					after,
				),
				None => PartitionedSeriesRowKeyRange::full_scan_range(storage, after),
			}
		} else {
			SeriesRowKeyRange::scan_range(
				storage,
				has_tag,
				self.variant_tag,
				self.key_range_start,
				self.key_range_end,
				after,
			)
		};
		let range = if self.oldest_first {
			range.resume_before(self.last_key.as_ref())
		} else {
			range
		};

		let read_shape = get_or_create_series_shape(&stored_ctx.services.catalog, self.series.def(), rx)?;
		let stored_ctx = stored_ctx.clone();

		let scope = RangeScope::All;

		let data_column_count = series.data_columns().count();
		let batch = if self.oldest_first && partitioned {
			let fixed = self.partition.map(|_| range);
			let merge = self
				.merge
				.get_or_insert_with(|| PartitionMerge::new(MergeLayout::Series, storage, fixed));
			let rows = merge.next(rx, batch_size)?;
			Self::drain_batch(
				&mut rows.into_iter().map(Ok),
				batch_size,
				partitioned,
				has_tag,
				data_column_count,
				&read_shape,
			)?
		} else {
			let mut stream = Self::open_range(rx, range, scope, batch_size, self.oldest_first)?;
			Self::drain_batch(
				&mut stream,
				batch_size,
				partitioned,
				has_tag,
				data_column_count,
				&read_shape,
			)?
		};

		if batch.key_values.is_empty() {
			self.exhausted = true;
			if self.last_key.is_none() {
				return Ok(Some(empty_scan(self.empty_columns(has_tag), &self.system_columns)?));
			}
			return Ok(None);
		}

		self.last_key = batch.last_key.clone();

		self.assemble(rx, &stored_ctx, batch, has_tag, partitioned)
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}

pub(crate) fn build_data_column(name: &str, values: &[Value], col_type: ValueType) -> Result<(FieldRef, ArrayRef)> {
	let mut data = ColumnBuilder::with_capacity(col_type, values.len());
	for value in values {
		data.push_value(value.clone());
	}
	Ok(data.finish(name))
}
