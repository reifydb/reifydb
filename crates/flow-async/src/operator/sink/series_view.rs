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
	partition::partition_col_indices,
	row::row_shape_from_columns,
};
use reifydb_flow::{
	error::FlowSinkError,
	operator::{
		sink::{
			SourceRowEncoder, coerce_columns,
			partition::{partition_batch, resolve_partitions_flow},
			shape_field_columns, stored_created_at,
			table::{
				dictionary_encode_view_columns, dictionary_lookup_partition_columns,
				dictionary_lookup_view_columns,
			},
		},
		source::decode_change_keeping,
	},
	txn::Emit,
	writer::series::{FlowSeriesWriter, SeriesRowAddress},
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
		system_columns::{SystemColumn, column_view, created_at, require_row_numbers},
		value_type::ValueType,
	},
};
use tracing::instrument;

use super::DurableSink;
use crate::transaction::deferred::DeferredTransaction;

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

	#[inline]
	fn row_address(&self, partition: Option<Partition>, key: u64, sequence: u64) -> SeriesRowAddress {
		SeriesRowAddress {
			storage: self.storage,
			partition,
			variant_tag: None,
			key,
			sequence,
		}
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

	fn series_keys(&self, columns: &RecordBatch) -> Result<Vec<Option<u64>>> {
		Ok(match self.series_key_view(columns)? {
			None => vec![None; columns.num_rows()],
			Some(time) if self.key.column().is_empty() => match time.base_type() {
				ValueType::DateTime => self.key.keys_to_u64(&time),
				_ => vec![None; time.len()],
			},
			Some(view) => self.key.keys_to_u64(&view),
		})
	}

	#[inline]
	fn series_key_at(&self, keys: &[Option<u64>], row_idx: usize) -> Result<u64> {
		let key_column = self.key.column();
		keys[row_idx].ok_or_else(|| {
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

	fn checked_series_keys(&self, keys: &[Option<u64>]) -> Result<Vec<u64>> {
		(0..keys.len()).map(|row_idx| self.series_key_at(keys, row_idx)).collect()
	}

	fn resolved_partitions(
		&mut self,
		txn: &mut DeferredTransaction,
		view: &View,
		object_id: StorageId,
		source: &RecordBatch,
	) -> Result<Option<Vec<Partition>>> {
		if !self.is_partitioned() {
			return Ok(None);
		}
		let partitions = partition_batch(view, &self.partition_indices, source)?;
		resolve_partitions_flow(txn, object_id.into(), &partitions.distinct, &mut self.verified_partitions)?;
		Ok(Some(partitions.rows))
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
		let change = decode_change_keeping(txn, change, self.view.def().columns())?;
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
				} => self.apply_series_view_remove(txn, view, pre)?,
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
		if post.num_rows() == 0 {
			return Ok(());
		}
		let coerced = coerce_columns(post, view.columns(), &self.runtime_context)?;
		let dict_encoded = dictionary_encode_view_columns(txn, view, &coerced)?;
		let source = dict_encoded.as_ref().unwrap_or(&coerced);
		let row_count = source.num_rows();
		let field_columns = shape_field_columns(source, shape);
		let encoder = SourceRowEncoder::new(source, shape, &field_columns)?;
		let mut keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut encoded_bytes_list: Vec<EncodedBytes> = Vec::with_capacity(row_count);
		let row_numbers = require_row_numbers(source)?;
		let series_keys = self.series_keys(&coerced)?;
		let encoded = encoder.encode_all()?;
		let series_keys = self.checked_series_keys(&series_keys)?;
		let partitions = self.resolved_partitions(txn, view, object_id, source)?;
		for ((row_idx, &row_number), encoded) in row_numbers.iter().enumerate().take(row_count).zip(encoded) {
			let key = self
				.row_address(
					partitions.as_ref().map(|rows| rows[row_idx]),
					series_keys[row_idx],
					row_number.0,
				)
				.encoded();
			keys.push(key);
			encoded_bytes_list.push(encoded);
		}
		FlowSeriesWriter::new(txn).append(keys, encoded_bytes_list)?;
		txn.emit(view.id(), Diff::insert(coerced))?;
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
		if pre.num_rows() == 0 && post.num_rows() == 0 {
			return Ok(());
		}
		let coerced_pre = coerce_columns(pre, view.columns(), &self.runtime_context)?;
		let coerced_post = coerce_columns(post, view.columns(), &self.runtime_context)?;
		let dict_pre = if self.is_partitioned() {
			dictionary_lookup_partition_columns(txn, view, &self.partition_indices, &coerced_pre)?
		} else {
			None
		};
		let dict_post = dictionary_encode_view_columns(txn, view, &coerced_post)?;
		let source_pre = dict_pre.as_ref().unwrap_or(&coerced_pre);
		let source_post = dict_post.as_ref().unwrap_or(&coerced_post);
		let row_count = source_post.num_rows();
		let mut pre_keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut post_keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let pre_row_numbers = require_row_numbers(source_pre)?;
		let post_row_numbers = require_row_numbers(source_post)?;
		let pre_series_keys = self.series_keys(&coerced_pre)?;
		let post_series_keys = self.series_keys(&coerced_post)?;
		let pre_series_keys = self.checked_series_keys(&pre_series_keys)?;
		let post_series_keys = self.checked_series_keys(&post_series_keys)?;
		let partitions = if self.is_partitioned() {
			let pre = partition_batch(view, &self.partition_indices, source_pre)?;
			let post = partition_batch(view, &self.partition_indices, source_post)?;
			resolve_partitions_flow(txn, object_id.into(), &post.distinct, &mut self.verified_partitions)?;
			Some((pre.rows, post.rows))
		} else {
			None
		};
		for row_idx in 0..row_count {
			let pre_row_number = pre_row_numbers[row_idx];
			let post_row_number = post_row_numbers[row_idx];

			let pre_series_key = pre_series_keys[row_idx];
			let post_series_key = post_series_keys[row_idx];

			let (pre_partition, post_partition) = match &partitions {
				Some((pre, post)) => (Some(pre[row_idx]), Some(post[row_idx])),
				None => (None, None),
			};
			let pre_key = self.row_address(pre_partition, pre_series_key, pre_row_number.0).encoded();
			let post_key = self.row_address(post_partition, post_series_key, post_row_number.0).encoded();
			pre_keys.push(pre_key);
			post_keys.push(post_key);
		}
		let reads: Vec<(EncodedKey, Option<EncodedKey>)> = pre_keys
			.iter()
			.zip(&post_keys)
			.map(|(pre_key, post_key)| (post_key.clone(), (pre_key != post_key).then(|| pre_key.clone())))
			.collect();
		let mut created = created_at(source_post)?.to_vec();
		for (slot, stored) in created.iter_mut().zip(stored_created_at(txn, &reads)?) {
			if let Some(stamp) = stored {
				*slot = stamp;
			}
		}
		let field_columns = shape_field_columns(source_post, shape);
		let encoded = SourceRowEncoder::new(source_post, shape, &field_columns)?
			.with_created_at(&created)
			.encode_all()?;
		FlowSeriesWriter::new(txn).update(pre_keys, post_keys, encoded)?;
		txn.emit(view.id(), Diff::update(coerced_pre, coerced_post))?;
		Ok(())
	}

	#[inline]
	#[instrument(name = "flow::operator::sink::series::remove", level = "trace", skip_all, fields(rows = pre.num_rows()))]
	fn apply_series_view_remove(
		&self,
		txn: &mut DeferredTransaction,
		view: &View,
		pre: &RecordBatch,
	) -> Result<()> {
		if pre.num_rows() == 0 {
			return Ok(());
		}
		let coerced = coerce_columns(pre, view.columns(), &self.runtime_context)?;
		let dict_encoded = dictionary_lookup_view_columns(txn, view, &coerced)?;
		let source = dict_encoded.as_ref().unwrap_or(&coerced);
		let row_count = coerced.num_rows();
		let mut keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let row_numbers = require_row_numbers(&coerced)?;
		let series_keys = self.series_keys(&coerced)?;
		let partitions = if self.is_partitioned() {
			Some(partition_batch(view, &self.partition_indices, source)?.rows)
		} else {
			None
		};
		for (row_idx, &row_number) in row_numbers.iter().enumerate().take(row_count) {
			let series_key = self.series_key_at(&series_keys, row_idx)?;
			let key = self
				.row_address(partitions.as_ref().map(|rows| rows[row_idx]), series_key, row_number.0)
				.encoded();
			keys.push(key);
		}
		FlowSeriesWriter::new(txn).delete(keys)?;
		txn.emit(view.id(), Diff::remove(coerced))?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow_array::{ArrayRef, UInt64Array};
	use reifydb_core::{
		common::{ChangeVersion, CommitVersion},
		interface::{
			catalog::{
				column::{Column, ColumnIndex},
				id::{ColumnId, NamespaceId, ViewId},
				namespace::Namespace,
				view::{SeriesView, ViewKind},
			},
			resolved::ResolvedNamespace,
		},
		value::{batch::batch, column::factory::int4_optional},
	};
	use reifydb_flow::txn::FlowGet;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{
		fragment::Fragment,
		value::{
			constraint::TypeConstraint, container::temporal_array::datetime_array, datetime::DateTime,
			system_columns::with_system_column,
		},
	};

	use super::*;
	use crate::transaction::{deferred::StorageCounts, mock::FlowTxn};

	fn sink() -> SinkSeriesViewOperator {
		let key = SeriesKey::Integer {
			column: "k".to_string(),
		};
		let def = View::Series(SeriesView {
			id: ViewId(1),
			namespace: NamespaceId(1),
			name: "s".to_string(),
			kind: ViewKind::Deferred,
			columns: vec![Column {
				id: ColumnId(1),
				name: "k".to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Int4),
				properties: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			}],
			primary_key: None,
			partition_by: vec![],
			key: key.clone(),
			tag: None,
			sort: vec![],
		});
		let namespace = Namespace::Local {
			id: NamespaceId(1),
			name: "n".to_string(),
			local_name: "n".to_string(),
			parent_id: NamespaceId::ROOT,
		};
		let resolved = ResolvedView::new(
			Fragment::internal("s"),
			ResolvedNamespace::new(Fragment::internal("n"), namespace),
			def,
		);
		SinkSeriesViewOperator::new(
			OperatorId(1),
			resolved,
			key,
			vec![],
			RuntimeContext::with_clock(Clock::Mock(MockClock::from_millis(0))),
		)
	}

	fn one_row(key: i32) -> RecordBatch {
		let user = batch(vec![int4_optional("k", [Some(key)])]).unwrap();
		let stamp = || -> ArrayRef { Arc::new(datetime_array([DateTime::from_nanos(1_000)])) };
		let system: [(SystemColumn, ArrayRef); 4] = [
			(SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![1u64]))),
			(SystemColumn::CreatedAt, stamp()),
			(SystemColumn::UpdatedAt, stamp()),
			(SystemColumn::Time, stamp()),
		];
		system.into_iter()
			.fold(user, |columns, (column, array)| with_system_column(columns, column, array).unwrap())
	}

	fn change(diff: Diff) -> Change {
		Change::from_flow(
			OperatorId(1),
			ChangeVersion::from(CommitVersion(1)),
			vec![diff],
			DateTime::from_nanos(0),
		)
	}

	#[test]
	fn a_series_view_write_makes_only_the_reads_it_has_always_made() {
		// Each step gets a fresh transaction, otherwise pending writes would answer an added read and hide it.
		let engine = TestEngine::new();
		let mut sink = sink();
		for (diff, expected) in [
			(Diff::insert(one_row(1)), StorageCounts::default()),
			(
				Diff::update(one_row(1), one_row(2)),
				StorageCounts {
					get_many_calls: 1,
					..StorageCounts::default()
				},
			),
			(Diff::remove(one_row(2)), StorageCounts::default()),
		] {
			let mut txn = engine.flow_txn().clock_millis(0).deferred();
			DurableSink::apply(&mut sink, &mut txn, change(diff)).unwrap();
			assert_eq!(txn.storage_counts, expected);
		}
	}

	#[test]
	fn a_series_view_update_that_moves_the_key_removes_the_old_row_and_keeps_the_new_one() {
		// The writer owns this order; leaving the old key behind would show the row twice.
		let engine = TestEngine::new();
		let mut sink = sink();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		DurableSink::apply(&mut sink, &mut txn, change(Diff::update(one_row(1), one_row(2)))).unwrap();
		let key = |key: u64| sink.row_address(None, key, 1).encoded();
		assert_eq!(txn.get(&[key(1)]).unwrap(), vec![None]);
		assert!(txn.get(&[key(2)]).unwrap()[0].is_some());
	}
}
