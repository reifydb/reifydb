// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow, shape::RowShape},
	interface::{
		catalog::{flow::FlowNodeId, id::SeriesId, series::SeriesKey, shape::ShapeId, view::View},
		change::{Change, ChangeOrigin, Diff},
		resolved::ResolvedView,
	},
	key::row::RowKey,
	value::column::columns::Columns,
};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};
use smallvec::smallvec;

use super::{coerce_columns, encode_row_at_index, shape_field_columns, view::dictionary_encode_view_columns};
use crate::{Operator, operator::OperatorCell, transaction::FlowTransaction};

pub struct SinkSeriesViewOperator {
	#[allow(dead_code)]
	parent: OperatorCell,
	node: FlowNodeId,
	view: ResolvedView,
	series_id: SeriesId,
	#[allow(dead_code)]
	key: SeriesKey,
}

impl SinkSeriesViewOperator {
	pub fn new(
		parent: OperatorCell,
		node: FlowNodeId,
		view: ResolvedView,
		series_id: SeriesId,
		key: SeriesKey,
	) -> Self {
		Self {
			parent,
			node,
			view,
			series_id,
			key,
		}
	}
}

impl Operator for SinkSeriesViewOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let view = self.view.def().clone();
		let shape: RowShape = view.columns().into();
		let object_id = ShapeId::series(self.series_id);

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

		Ok(Change::from_flow(self.node, change.version, Vec::new(), change.changed_at))
	}
}

impl SinkSeriesViewOperator {
	#[inline]
	fn apply_series_view_insert(
		&self,
		txn: &mut FlowTransaction,
		view: &View,
		shape: &RowShape,
		object_id: ShapeId,
		post: &Columns,
	) -> Result<()> {
		let coerced = coerce_columns(post, view.columns())?;
		let dict_encoded = dictionary_encode_view_columns(txn, view, &coerced)?;
		let source = dict_encoded.as_ref().unwrap_or(&coerced);
		let row_count = source.row_count();
		let field_columns = shape_field_columns(source, shape);
		let mut ids: Vec<RowNumber> = Vec::with_capacity(row_count);
		let mut encoded_rows: Vec<EncodedRow> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let row_number = source.row_numbers[row_idx];
			let (_, encoded) = encode_row_at_index(source, row_idx, shape, row_number, &field_columns)?;
			ids.push(row_number);
			encoded_rows.push(encoded);
		}
		for (row_number, encoded) in ids.iter().zip(encoded_rows.iter()) {
			let key = RowKey::encoded(object_id, *row_number);
			txn.set(&key, encoded.clone())?;
		}
		emit_view_change(txn, view, Diff::insert(coerced));
		Ok(())
	}

	#[inline]
	fn apply_series_view_update(
		&self,
		txn: &mut FlowTransaction,
		view: &View,
		shape: &RowShape,
		object_id: ShapeId,
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
		let mut post_encoded_rows: Vec<EncodedRow> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let pre_row_number = source_pre.row_numbers[row_idx];
			let post_row_number = source_post.row_numbers[row_idx];
			let (_, post_encoded) =
				encode_row_at_index(source_post, row_idx, shape, post_row_number, &field_columns)?;

			pre_keys.push(RowKey::encoded(object_id, pre_row_number));
			post_keys.push(RowKey::encoded(object_id, post_row_number));
			post_encoded_rows.push(post_encoded);
		}
		for ((pre_key, post_key), post_encoded) in
			pre_keys.iter().zip(post_keys.iter()).zip(post_encoded_rows.iter())
		{
			txn.remove(pre_key)?;
			txn.set(post_key, post_encoded.clone())?;
		}
		emit_view_change(txn, view, Diff::update(coerced_pre, coerced_post));
		Ok(())
	}

	#[inline]
	fn apply_series_view_remove(
		&self,
		txn: &mut FlowTransaction,
		view: &View,
		object_id: ShapeId,
		pre: &Columns,
	) -> Result<()> {
		let coerced = coerce_columns(pre, view.columns())?;
		let row_count = coerced.row_count();
		let mut ids: Vec<RowNumber> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let row_number = coerced.row_numbers[row_idx];
			ids.push(row_number);
		}
		for row_number in ids.iter() {
			let key = RowKey::encoded(object_id, *row_number);
			txn.remove(&key)?;
		}
		emit_view_change(txn, view, Diff::remove(coerced));
		Ok(())
	}
}

#[inline]
fn emit_view_change(txn: &mut FlowTransaction, view: &View, diff: Diff) {
	let version = txn.version();
	let changed_at = DateTime::from_nanos(txn.clock().now_nanos());
	txn.track_flow_change(Change {
		origin: ChangeOrigin::Shape(ShapeId::view(view.id())),
		version,
		diffs: smallvec![diff],
		changed_at,
	});
}
