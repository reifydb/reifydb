// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

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
	key::{EncodableKey, partitioned_series_row::PartitionedSeriesRowKey, series_row::SeriesRowKey},
	partition::partition_col_indices,
	row::row_shape_from_columns,
	value::column::columns::Columns,
};
use reifydb_value::{
	Result,
	error::Error,
	reifydb_assertions,
	value::{Value, partition::Partition},
};
use tracing::instrument;

use super::{
	DurableSink, coerce_columns, emit_view_change, encode_row_at_index,
	partition::{ensure_partition_unchanged, partition_of, resolve_partition_flow},
	shape_field_columns,
	view::dictionary_encode_view_columns,
};
use crate::{
	error::FlowSinkError,
	transaction::{FlowTransaction, deferred::DeferredTransaction},
};

pub struct SinkSeriesViewOperator {
	operator: OperatorId,
	view: ResolvedView,
	storage: StorageId,
	key: SeriesKey,
	partition_indices: Vec<usize>,
	verified_partitions: HashMap<Partition, Vec<Value>>,
}

impl SinkSeriesViewOperator {
	pub fn new(operator: OperatorId, view: ResolvedView, key: SeriesKey, partition_by: Vec<String>) -> Self {
		let partition_indices = partition_col_indices(view.def().columns(), &partition_by);
		let storage = view.def().storage_id();
		Self {
			operator,
			view,
			storage,
			key,
			partition_indices,
			verified_partitions: HashMap::new(),
		}
	}

	#[inline]
	fn is_partitioned(&self) -> bool {
		!self.partition_indices.is_empty()
	}

	#[inline]
	fn series_key_at(&self, columns: &Columns, row_idx: usize) -> Result<u64> {
		let key_column = self.key.column();
		let value = columns
			.iter()
			.find(|col| col.name().text() == key_column)
			.map(|col| col.data().get_value(row_idx));

		reifydb_assertions! {
			assert!(
				value.is_some(),
				"the series key column '{key_column}' must reach the sink for every row of view \
				 '{}'; without it every row collapses onto a single key and overwrites its \
				 predecessor",
				self.view.def().name()
			);
		}

		value.and_then(|value| self.key.key_to_u64(value)).ok_or_else(|| {
			Error::from(FlowSinkError::MissingSeriesKey {
				view: self.view.def().name().to_string(),
				column: key_column.to_string(),
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
		let view = self.view.def().clone();
		let shape = row_shape_from_columns(RowFamily::Series, view.columns());
		let object_id = self.storage;

		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => self.apply_series_view_insert(txn, &view, &shape, object_id, post)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_series_view_update(txn, &view, &shape, object_id, pre, post)?,
				Diff::Remove {
					pre,
					..
				} => self.apply_series_view_remove(txn, &view, object_id, pre)?,
			}
		}

		Ok(Change::from_flow(self.operator, change.version, Vec::new(), change.changed_at))
	}
}

impl SinkSeriesViewOperator {
	#[inline]
	#[instrument(name = "flow::operator::sink::series::insert", level = "trace", skip_all, fields(rows = post.row_count()))]
	fn apply_series_view_insert(
		&mut self,
		txn: &mut DeferredTransaction,
		view: &View,
		shape: &RowShape,
		object_id: StorageId,
		post: &Columns,
	) -> Result<()> {
		let coerced = coerce_columns(post, view.columns())?;
		let dict_encoded = dictionary_encode_view_columns(txn, view, &coerced)?;
		let source = dict_encoded.as_ref().unwrap_or(&coerced);
		let row_count = source.row_count();
		let field_columns = shape_field_columns(source, shape);
		let mut keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut encoded_bytes_list: Vec<EncodedBytes> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let row_number = source.row_numbers()[row_idx];
			let (_, encoded) = encode_row_at_index(source, row_idx, shape, row_number, &field_columns)?;
			let series_key = self.series_key_at(&coerced, row_idx)?;
			let key = if self.is_partitioned() {
				let (partition, values) = partition_of(&self.partition_indices, &coerced, row_idx);
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
	#[instrument(name = "flow::operator::sink::series::update", level = "trace", skip_all, fields(rows = post.row_count()))]
	fn apply_series_view_update(
		&mut self,
		txn: &mut DeferredTransaction,
		view: &View,
		shape: &RowShape,
		object_id: StorageId,
		pre: &Columns,
		post: &Columns,
	) -> Result<()> {
		let coerced_pre = coerce_columns(pre, view.columns())?;
		let coerced_post = coerce_columns(post, view.columns())?;
		let dict_pre = dictionary_encode_view_columns(txn, view, &coerced_pre)?;
		let dict_post = dictionary_encode_view_columns(txn, view, &coerced_post)?;
		let source_pre = dict_pre.as_ref().unwrap_or(&coerced_pre);
		let source_post = dict_post.as_ref().unwrap_or(&coerced_post);
		let row_count = source_post.row_count();
		let field_columns = shape_field_columns(source_post, shape);
		let mut pre_keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut post_keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut post_encoded_bytes_vec: Vec<EncodedBytes> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let pre_row_number = source_pre.row_numbers()[row_idx];
			let post_row_number = source_post.row_numbers()[row_idx];
			let (_, post_encoded) =
				encode_row_at_index(source_post, row_idx, shape, post_row_number, &field_columns)?;

			let pre_series_key = self.series_key_at(&coerced_pre, row_idx)?;
			let post_series_key = self.series_key_at(&coerced_post, row_idx)?;

			let (pre_key, post_key) = if self.is_partitioned() {
				let (pre_partition, _pre_values) =
					partition_of(&self.partition_indices, &coerced_pre, row_idx);
				let (post_partition, post_values) =
					partition_of(&self.partition_indices, &coerced_post, row_idx);
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
	#[instrument(name = "flow::operator::sink::series::remove", level = "trace", skip_all, fields(rows = pre.row_count()))]
	fn apply_series_view_remove(
		&self,
		txn: &mut DeferredTransaction,
		view: &View,
		object_id: StorageId,
		pre: &Columns,
	) -> Result<()> {
		let coerced = coerce_columns(pre, view.columns())?;
		let row_count = coerced.row_count();
		let mut keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let row_number = coerced.row_numbers()[row_idx];
			let series_key = self.series_key_at(&coerced, row_idx)?;
			let key = if self.is_partitioned() {
				let (partition, _values) = partition_of(&self.partition_indices, &coerced, row_idx);
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
