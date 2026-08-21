// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{series::EncodedSeriesRow, shape::RowShape},
};
use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::storage::StorageId, resolved::ResolvedSeries, store::MultiVersionRow},
	key::{
		EncodableKey,
		partitioned_row::{PartitionedRowKey, RowLocator},
		series_row::{SeriesRowKey, SeriesRowKeyRange},
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::{
	fragment::Fragment,
	reifydb_assertions,
	value::{
		Value, datetime::DateTime, dictionary::DictionaryEntryId, partition::Partition, row_number::RowNumber,
		system_columns::SystemColumns, value_type::ValueType,
	},
};
use tracing::instrument;

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
	last_key: Option<EncodedKey>,
	exhausted: bool,

	min_commit_version: Option<CommitVersion>,
}

impl SeriesScanNode {
	pub fn with_min_commit_version(mut self, min_commit_version: Option<CommitVersion>) -> Self {
		self.min_commit_version = min_commit_version;
		self
	}

	pub fn new(
		series: ResolvedSeries,
		key_range_start: Option<u64>,
		key_range_end: Option<u64>,
		variant_tag: Option<u8>,
		partition: Option<Partition>,
		context: Arc<QueryContext>,
	) -> Result<Self> {
		let mut columns = vec![Fragment::internal(series.def().key.column())];
		if series.def().tag.is_some() {
			columns.push(Fragment::internal("tag"));
		}
		for col in series.columns() {
			columns.push(Fragment::internal(&col.name));
		}
		let headers = ColumnHeaders {
			columns,
		};

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
			min_commit_version: None,
		})
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::series::range_open")]
	fn open_range<'rx, 'tx>(
		rx: &'rx mut Transaction<'tx>,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: u64,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + 'rx>> {
		rx.range(range, scope, batch_size as usize)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::series::drain")]
	fn drain_batch(
		stream: &mut dyn Iterator<Item = Result<MultiVersionRow>>,
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
				match PartitionedRowKey::decode(&entry.key) {
					Some(pk) => match pk.locator {
						RowLocator::Series {
							variant_tag,
							key,
							sequence,
						} => Some((key, sequence, variant_tag, Some(pk.partition))),
						_ => None,
					},
					None => None,
				}
			} else {
				SeriesRowKey::decode(&entry.key).map(|k| (k.key, k.sequence, k.variant_tag, None))
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

				batch.last_key = Some(entry.key);
				count += 1;
				if count >= batch_size as usize {
					break;
				}
			}
		}

		Ok(batch)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::series::empty_columns")]
	fn empty_columns(&self, has_tag: bool) -> Vec<ColumnWithName> {
		let series = self.series.def();
		let key_type = series
			.columns
			.iter()
			.find(|c| c.name == series.key.column())
			.map(|c| c.constraint.get_type())
			.unwrap_or(ValueType::Int8);

		let mut result_columns = Vec::new();
		result_columns.push(ColumnWithName {
			name: Fragment::internal(series.key.column()),
			data: ColumnBuffer::none_typed(key_type, 0),
		});
		if has_tag {
			result_columns.push(ColumnWithName {
				name: Fragment::internal("tag"),
				data: ColumnBuffer::none_typed(ValueType::Uint1, 0),
			});
		}
		for col_def in series.data_columns() {
			result_columns.push(ColumnWithName {
				name: Fragment::internal(&col_def.name),
				data: ColumnBuffer::none_typed(col_def.constraint.get_type(), 0),
			});
		}
		result_columns
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::series::assemble")]
	fn assemble<'a>(
		&self,
		rx: &mut Transaction<'a>,
		stored_ctx: &QueryContext,
		batch: SeriesBatch,
		has_tag: bool,
		partitioned: bool,
	) -> Result<Option<Columns>> {
		let series = self.series.def();
		let mut result_columns = Vec::new();

		result_columns.push(ColumnWithName::new(
			Fragment::internal(series.key.column()),
			series.key_column_data(batch.key_values),
		));

		if has_tag {
			result_columns
				.push(ColumnWithName::new(Fragment::internal("tag"), ColumnBuffer::uint1(batch.tags)));
		}

		for (col_idx, col_def) in series.data_columns().enumerate() {
			let col_type = col_def.constraint.get_type();
			let mut col_values: Vec<Value> = batch
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

		let row_numbers: Vec<RowNumber> = batch.sequences.into_iter().map(RowNumber::from).collect();
		let mut result = Columns::with_system(
			result_columns,
			SystemColumns::new(
				row_numbers,
				Vec::new(),
				batch.created_at_values,
				batch.updated_at_values,
				batch.time_values,
			),
		);
		if partitioned {
			result.system.set_partitions(batch.partitions);
		}
		Ok(Some(result))
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
	last_key: Option<EncodedKey>,
}

impl QueryNode for SeriesScanNode {
	#[instrument(name = "volcano::scan::series::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		Ok(())
	}

	#[instrument(name = "volcano::scan::series::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
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
		let range = if partitioned {
			match self.partition {
				Some(partition) => PartitionedRowKey::partition_scan_range(
					series.id,
					partition,
					self.last_key.as_ref(),
				),
				None => PartitionedRowKey::scan_range(series.id, self.last_key.as_ref()),
			}
		} else {
			SeriesRowKeyRange::scan_range(
				StorageId::series(series.id),
				self.variant_tag,
				self.key_range_start,
				self.key_range_end,
				self.last_key.as_ref(),
			)
		};

		let read_shape = get_or_create_series_shape(&stored_ctx.services.catalog, self.series.def(), rx)?;
		let stored_ctx = stored_ctx.clone();

		let scope = match self.min_commit_version {
			Some(v) => RangeScope::After(v),
			None => RangeScope::All,
		};

		let data_column_count = series.data_columns().count();
		let batch = {
			let mut stream = Self::open_range(rx, range, scope, batch_size)?;
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
				return Ok(Some(Columns::new(self.empty_columns(has_tag))));
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

pub(crate) fn build_data_column(name: &str, values: &[Value], col_type: ValueType) -> Result<ColumnWithName> {
	let data = match col_type {
		ValueType::Boolean => {
			let vals: Vec<bool> = values
				.iter()
				.map(|v| match v {
					Value::Boolean(b) => *b,
					_ => false,
				})
				.collect();
			ColumnBuffer::bool(vals)
		}
		ValueType::Int1 => {
			let vals: Vec<i8> = values
				.iter()
				.map(|v| match v {
					Value::Int1(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::int1(vals)
		}
		ValueType::Int2 => {
			let vals: Vec<i16> = values
				.iter()
				.map(|v| match v {
					Value::Int2(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::int2(vals)
		}
		ValueType::Int4 => {
			let vals: Vec<i32> = values
				.iter()
				.map(|v| match v {
					Value::Int4(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::int4(vals)
		}
		ValueType::Int8 => {
			let vals: Vec<i64> = values
				.iter()
				.map(|v| match v {
					Value::Int8(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::int8(vals)
		}
		ValueType::Uint1 => {
			let vals: Vec<u8> = values
				.iter()
				.map(|v| match v {
					Value::Uint1(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint1(vals)
		}
		ValueType::Uint2 => {
			let vals: Vec<u16> = values
				.iter()
				.map(|v| match v {
					Value::Uint2(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint2(vals)
		}
		ValueType::Uint4 => {
			let vals: Vec<u32> = values
				.iter()
				.map(|v| match v {
					Value::Uint4(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint4(vals)
		}
		ValueType::Uint8 => {
			let vals: Vec<u64> = values
				.iter()
				.map(|v| match v {
					Value::Uint8(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint8(vals)
		}
		ValueType::Float4 => {
			let vals: Vec<f32> = values
				.iter()
				.map(|v| match v {
					Value::Float4(n) => n.value(),
					_ => 0.0,
				})
				.collect();
			ColumnBuffer::float4(vals)
		}
		ValueType::Float8 => {
			let vals: Vec<f64> = values
				.iter()
				.map(|v| match v {
					Value::Float8(n) => n.value(),
					_ => 0.0,
				})
				.collect();
			ColumnBuffer::float8(vals)
		}
		ValueType::Utf8 => {
			let vals: Vec<String> = values
				.iter()
				.map(|v| match v {
					Value::Utf8(s) => s.clone(),
					_ => String::new(),
				})
				.collect();
			ColumnBuffer::utf8(vals)
		}
		_ => {
			let vals: Vec<String> = values.iter().map(|v| format!("{:?}", v)).collect();
			ColumnBuffer::utf8(vals)
		}
	};

	Ok(ColumnWithName {
		name: Fragment::internal(name),
		data,
	})
}
