// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::UnsafeCell, collections::HashMap};

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{
		bytes::EncodedBytes,
		shape::{RowFamily, RowShape},
	},
};
use reifydb_core::{
	interface::{
		catalog::{
			flow::OperatorId, id::SeriesId, object::ObjectId, series::SeriesKey, storage::StorageId,
			view::View,
		},
		change::{Change, ChangeOrigin, Diff},
		flow::OperatorCapability,
		resolved::ResolvedView,
	},
	key::{
		partitioned_row::{PartitionedRowKey, RowLocator},
		row::RowKey,
	},
	partition::partition_col_indices,
	row::row_shape_from_columns,
	value::column::columns::Columns,
};
use reifydb_value::{
	Result,
	value::{Value, partition::Partition},
};
use smallvec::smallvec;
use tracing::instrument;

use super::{
	coerce_columns, encode_row_at_index,
	partition::{ensure_partition_unchanged, partition_of, resolve_partition_flow},
	shape_field_columns,
	view::dictionary_encode_view_columns,
};
use crate::{
	operator::Operator,
	transaction::{FlowTransaction, deferred::DeferredTransaction},
};

pub struct SinkSeriesViewOperator {
	operator: OperatorId,
	view: ResolvedView,
	series_id: SeriesId,
	#[allow(dead_code)]
	key: SeriesKey,
	partition_indices: Vec<usize>,
	verified_partitions: UnsafeCell<HashMap<Partition, Vec<Value>>>,
}

impl SinkSeriesViewOperator {
	pub fn new(
		operator: OperatorId,
		view: ResolvedView,
		series_id: SeriesId,
		key: SeriesKey,
		partition_by: Vec<String>,
	) -> Self {
		let partition_indices = partition_col_indices(view.def().columns(), &partition_by);
		Self {
			operator,
			view,
			series_id,
			key,
			partition_indices,
			verified_partitions: UnsafeCell::new(HashMap::new()),
		}
	}

	#[inline]
	fn is_partitioned(&self) -> bool {
		!self.partition_indices.is_empty()
	}

	#[allow(clippy::mut_from_ref)]
	fn verified_partitions(&self) -> &mut HashMap<Partition, Vec<Value>> {
		// SAFETY: an operator is reachable from one thread at a time and each apply helper takes this
		// borrow once, calling nothing that reaches the cell again, so no other borrow is live.
		unsafe { &mut *self.verified_partitions.get() }
	}
}

impl Operator<DeferredTransaction> for SinkSeriesViewOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&mut self, txn: &mut DeferredTransaction, change: Change) -> Result<Change> {
		let view = self.view.def().clone();
		let shape = row_shape_from_columns(RowFamily::Series, view.columns());
		let object_id = StorageId::series(self.series_id);

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
		&self,
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
		let verified = self.verified_partitions();
		for row_idx in 0..row_count {
			let row_number = source.row_numbers()[row_idx];
			let (_, encoded) = encode_row_at_index(source, row_idx, shape, row_number, &field_columns)?;
			let key = if self.is_partitioned() {
				let (partition, values) = partition_of(&self.partition_indices, &coerced, row_idx);
				resolve_partition_flow(txn, object_id.into(), partition, &values, verified)?;
				PartitionedRowKey::encoded(
					object_id,
					partition,
					RowLocator::Series {
						variant_tag: None,
						key: 0,
						sequence: row_number.0,
					},
				)
			} else {
				RowKey::encoded(object_id, row_number)
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
		&self,
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
		let verified = self.verified_partitions();
		for row_idx in 0..row_count {
			let pre_row_number = source_pre.row_numbers()[row_idx];
			let post_row_number = source_post.row_numbers()[row_idx];
			let (_, post_encoded) =
				encode_row_at_index(source_post, row_idx, shape, post_row_number, &field_columns)?;

			let (pre_key, post_key) = if self.is_partitioned() {
				let (pre_partition, _pre_values) =
					partition_of(&self.partition_indices, &coerced_pre, row_idx);
				let (post_partition, post_values) =
					partition_of(&self.partition_indices, &coerced_post, row_idx);
				ensure_partition_unchanged(object_id.into(), pre_partition, post_partition)?;
				resolve_partition_flow(txn, object_id.into(), post_partition, &post_values, verified)?;
				(
					PartitionedRowKey::encoded(
						object_id,
						pre_partition,
						RowLocator::Series {
							variant_tag: None,
							key: 0,
							sequence: pre_row_number.0,
						},
					),
					PartitionedRowKey::encoded(
						object_id,
						post_partition,
						RowLocator::Series {
							variant_tag: None,
							key: 0,
							sequence: post_row_number.0,
						},
					),
				)
			} else {
				(
					RowKey::encoded(object_id, pre_row_number),
					RowKey::encoded(object_id, post_row_number),
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
			let key = if self.is_partitioned() {
				let (partition, _values) = partition_of(&self.partition_indices, &coerced, row_idx);
				PartitionedRowKey::encoded(
					object_id,
					partition,
					RowLocator::Series {
						variant_tag: None,
						key: 0,
						sequence: row_number.0,
					},
				)
			} else {
				RowKey::encoded(object_id, row_number)
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

#[inline]
fn emit_view_change(txn: &mut DeferredTransaction, view: &View, diff: Diff) {
	let version = txn.version();
	let changed_at = txn.clock().now();
	txn.track_flow_change(Change {
		origin: ChangeOrigin::Object(ObjectId::view(view.id())),
		version,
		diffs: smallvec![diff],
		changed_at,
	});
}
