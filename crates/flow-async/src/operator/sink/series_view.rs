// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use arrow_array::RecordBatch;
use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{
		bytes::EncodedBytes,
		shape::{RowFamily, RowShape},
	},
};
use reifydb_core::{
	interface::{
		catalog::{flow::OperatorId, series::SeriesKey, storage::StorageId, view::View},
		change::{Change, Diff},
		flow::OperatorCapability,
		resolved::ResolvedView,
	},
	key::series::{PartitionedSeriesRowKey, SeriesRowKey},
	partition::partition_col_indices,
	row::row_shape_from_columns,
};
use reifydb_flow::{
	error::FlowSinkError,
	operator::sink::{
		SourceRowEncoder, coerce_columns,
		partition::{ensure_partition_unchanged, partition_of},
		shape_field_columns,
	},
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	error::Error,
	reifydb_assertions,
	value::{
		Value,
		column_view::ColumnView,
		partition::Partition,
		system_columns::{SystemColumn, column_view, require_row_numbers},
	},
};
use tracing::instrument;

use super::{
	DurableSink, emit_view_change,
	partition::resolve_partition_flow,
	view::{dictionary_encode_view_columns, dictionary_lookup_view_columns},
};
use crate::transaction::{FlowTransaction, deferred::DeferredTransaction};

pub struct SinkSeriesViewOperator {
	operator: OperatorId,
	view: ResolvedView,
	shape: RowShape,
	storage: StorageId,
	key: SeriesKey,
	partition_indices: Vec<usize>,
	verified_partitions: HashMap<Partition, Vec<Value>>,
	runtime_context: RuntimeContext,
}

impl SinkSeriesViewOperator {
	pub fn new(
		operator: OperatorId,
		view: ResolvedView,
		key: SeriesKey,
		partition_by: Vec<String>,
		runtime_context: RuntimeContext,
	) -> Self {
		let partition_indices = partition_col_indices(view.def().columns(), &partition_by);
		let storage = view.def().storage_id();
		let shape = row_shape_from_columns(RowFamily::Series, view.def().columns());
		Self {
			operator,
			view,
			shape,
			storage,
			key,
			partition_indices,
			verified_partitions: HashMap::new(),
			runtime_context,
		}
	}

	#[inline]
	fn is_partitioned(&self) -> bool {
		!self.partition_indices.is_empty()
	}

	fn series_key_view<'a>(&self, columns: &'a RecordBatch) -> Result<Option<ColumnView<'a>>> {
		let key_column = self.key.column();
		if key_column.is_empty() {
			return column_view(columns, SystemColumn::Time.name());
		}
		reifydb_assertions! {
			assert!(
				columns.schema_ref().fields().iter().any(|field| field.name() == key_column),
				"the series key column '{key_column}' must reach the sink for every row of \
				 view '{}'; without it every row collapses onto a single key and overwrites \
				 its predecessor",
				self.view.def().name()
			);
		}
		column_view(columns, key_column)
	}

	#[inline]
	fn series_key_at(&self, key_view: Option<&ColumnView>, row_idx: usize) -> Result<u64> {
		let key_column = self.key.column();

		let key = match key_view {
			None => None,
			Some(time) if key_column.is_empty() => match time.get_value(row_idx) {
				Value::DateTime(time) => self.key.key_to_u64(Value::DateTime(time)),
				_ => None,
			},
			Some(view) => self.key.key_to_u64(view.get_value(row_idx)),
		};

		key.ok_or_else(|| {
			Error::from(FlowSinkError::MissingSeriesKey {
				view: self.view.def().name().to_string(),
				column: if key_column.is_empty() {
					SystemColumn::Time.name().to_string()
				} else {
					key_column.to_string()
				},
				row_idx,
			})
		})
	}
}

impl DurableSink for SinkSeriesViewOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&mut self, txn: &mut DeferredTransaction, change: Change) -> Result<Change> {
		let resolved = self.view.clone();
		let view = resolved.def();
		let shape = self.shape.clone();
		let object_id = self.storage;

		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => self.apply_series_view_insert(txn, view, &shape, object_id, post)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_series_view_update(txn, view, &shape, object_id, pre, post)?,
				Diff::Remove {
					pre,
					..
				} => self.apply_series_view_remove(txn, view, object_id, pre)?,
			}
		}

		Ok(Change::from_flow(self.operator, change.version, Vec::new(), change.changed_at))
	}
}

impl SinkSeriesViewOperator {
	#[inline]
	#[instrument(name = "flow::operator::sink::series::insert", level = "trace", skip_all, fields(rows = post.num_rows()))]
	fn apply_series_view_insert(
		&mut self,
		txn: &mut DeferredTransaction,
		view: &View,
		shape: &RowShape,
		object_id: StorageId,
		post: &RecordBatch,
	) -> Result<()> {
		let coerced = coerce_columns(post, view.columns(), &self.runtime_context)?;
		let dict_encoded = dictionary_encode_view_columns(txn, view, &coerced)?;
		let source = dict_encoded.as_ref().unwrap_or(&coerced);
		let row_count = source.num_rows();
		let field_columns = shape_field_columns(source, shape);
		let encoder = SourceRowEncoder::new(source, shape, &field_columns)?;
		let mut keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut encoded_bytes_list: Vec<EncodedBytes> = Vec::with_capacity(row_count);
		let (row_numbers, key_view) = if row_count == 0 {
			(&[][..], None)
		} else {
			(require_row_numbers(source)?, self.series_key_view(&coerced)?)
		};
		let encoded = encoder.encode_all()?;
		for ((row_idx, &row_number), encoded) in row_numbers.iter().enumerate().take(row_count).zip(encoded) {
			let series_key = self.series_key_at(key_view.as_ref(), row_idx)?;
			let key = if self.is_partitioned() {
				let (partition, values) = partition_of(view, &self.partition_indices, source, row_idx)?;
				resolve_partition_flow(
					txn,
					object_id.into(),
					partition,
					&values,
					&mut self.verified_partitions,
				)?;
				PartitionedSeriesRowKey::encoded(object_id, partition, None, series_key, row_number.0)
			} else {
				SeriesRowKey {
					storage: object_id,
					variant_tag: None,
					key: series_key,
					sequence: row_number.0,
				}
				.encode()
			};
			keys.push(key);
			encoded_bytes_list.push(encoded);
		}
		for (key, encoded) in keys.iter().zip(encoded_bytes_list.iter()) {
			txn.set(key, encoded.clone())?;
		}
		emit_view_change(txn, view, Diff::insert(coerced));
		Ok(())
	}

	#[inline]
	#[instrument(name = "flow::operator::sink::series::update", level = "trace", skip_all, fields(rows = post.num_rows()))]
	fn apply_series_view_update(
		&mut self,
		txn: &mut DeferredTransaction,
		view: &View,
		shape: &RowShape,
		object_id: StorageId,
		pre: &RecordBatch,
		post: &RecordBatch,
	) -> Result<()> {
		let coerced_pre = coerce_columns(pre, view.columns(), &self.runtime_context)?;
		let coerced_post = coerce_columns(post, view.columns(), &self.runtime_context)?;
		let dict_pre = dictionary_encode_view_columns(txn, view, &coerced_pre)?;
		let dict_post = dictionary_encode_view_columns(txn, view, &coerced_post)?;
		let source_pre = dict_pre.as_ref().unwrap_or(&coerced_pre);
		let source_post = dict_post.as_ref().unwrap_or(&coerced_post);
		let row_count = source_post.num_rows();
		let field_columns = shape_field_columns(source_post, shape);
		let encoder = SourceRowEncoder::new(source_post, shape, &field_columns)?;
		let mut pre_keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut post_keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut post_encoded_bytes_vec: Vec<EncodedBytes> = Vec::with_capacity(row_count);
		let (pre_row_numbers, post_row_numbers, pre_key_view, post_key_view) = if row_count == 0 {
			(&[][..], &[][..], None, None)
		} else {
			(
				require_row_numbers(source_pre)?,
				require_row_numbers(source_post)?,
				self.series_key_view(&coerced_pre)?,
				self.series_key_view(&coerced_post)?,
			)
		};
		let encoded = encoder.encode_all()?;
		for (row_idx, post_encoded) in (0..row_count).zip(encoded) {
			let pre_row_number = pre_row_numbers[row_idx];
			let post_row_number = post_row_numbers[row_idx];

			let pre_series_key = self.series_key_at(pre_key_view.as_ref(), row_idx)?;
			let post_series_key = self.series_key_at(post_key_view.as_ref(), row_idx)?;

			let (pre_key, post_key) = if self.is_partitioned() {
				let (pre_partition, _pre_values) =
					partition_of(view, &self.partition_indices, source_pre, row_idx)?;
				let (post_partition, post_values) =
					partition_of(view, &self.partition_indices, source_post, row_idx)?;
				ensure_partition_unchanged(object_id.into(), pre_partition, post_partition)?;
				resolve_partition_flow(
					txn,
					object_id.into(),
					post_partition,
					&post_values,
					&mut self.verified_partitions,
				)?;
				(
					PartitionedSeriesRowKey::encoded(
						object_id,
						pre_partition,
						None,
						pre_series_key,
						pre_row_number.0,
					),
					PartitionedSeriesRowKey::encoded(
						object_id,
						post_partition,
						None,
						post_series_key,
						post_row_number.0,
					),
				)
			} else {
				(
					SeriesRowKey {
						storage: object_id,
						variant_tag: None,
						key: pre_series_key,
						sequence: pre_row_number.0,
					}
					.encode(),
					SeriesRowKey {
						storage: object_id,
						variant_tag: None,
						key: post_series_key,
						sequence: post_row_number.0,
					}
					.encode(),
				)
			};
			pre_keys.push(pre_key);
			post_keys.push(post_key);
			post_encoded_bytes_vec.push(post_encoded);
		}
		for ((pre_key, post_key), post_encoded) in
			pre_keys.iter().zip(post_keys.iter()).zip(post_encoded_bytes_vec.iter())
		{
			txn.remove(pre_key)?;
			txn.set(post_key, post_encoded.clone())?;
		}
		emit_view_change(txn, view, Diff::update(coerced_pre, coerced_post));
		Ok(())
	}

	#[inline]
	#[instrument(name = "flow::operator::sink::series::remove", level = "trace", skip_all, fields(rows = pre.num_rows()))]
	fn apply_series_view_remove(
		&self,
		txn: &mut DeferredTransaction,
		view: &View,
		object_id: StorageId,
		pre: &RecordBatch,
	) -> Result<()> {
		let coerced = coerce_columns(pre, view.columns(), &self.runtime_context)?;
		let dict_encoded = dictionary_lookup_view_columns(txn, view, &coerced)?;
		let source = dict_encoded.as_ref().unwrap_or(&coerced);
		let row_count = coerced.num_rows();
		let mut keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let (row_numbers, key_view) = if row_count == 0 {
			(&[][..], None)
		} else {
			(require_row_numbers(&coerced)?, self.series_key_view(&coerced)?)
		};
		for (row_idx, &row_number) in row_numbers.iter().enumerate().take(row_count) {
			let series_key = self.series_key_at(key_view.as_ref(), row_idx)?;
			let key = if self.is_partitioned() {
				let (partition, _values) =
					partition_of(view, &self.partition_indices, source, row_idx)?;
				PartitionedSeriesRowKey::encoded(object_id, partition, None, series_key, row_number.0)
			} else {
				SeriesRowKey {
					storage: object_id,
					variant_tag: None,
					key: series_key,
					sequence: row_number.0,
				}
				.encode()
			};
			keys.push(key);
		}
		for key in keys.iter() {
			txn.remove(key)?;
		}
		emit_view_change(txn, view, Diff::remove(coerced));
		Ok(())
	}
}
