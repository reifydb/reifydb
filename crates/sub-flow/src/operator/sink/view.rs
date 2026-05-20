// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_abi::operator::capabilities::CAPABILITY_ALL_STANDARD;
use reifydb_core::{
	encoded::{
		key::EncodedKey,
		row::{EncodedRow, SHAPE_HEADER_SIZE},
		shape::RowShape,
	},
	interface::{
		catalog::{flow::FlowNodeId, id::TableId, shape::ShapeId, view::View},
		change::{Change, ChangeOrigin, Diff},
		resolved::ResolvedView,
	},
	key::kind::KeyKind,
	util::encoding::keycode::{catalog::serialize_shape_id, encode_u8, encode_u64_varint},
	value::column::columns::Columns,
};
use reifydb_transaction::interceptor::{WithInterceptors, view_row::ViewRowInterceptor};
use reifydb_type::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};
use smallvec::smallvec;

use super::{coerce_columns, encode_row_at_index};
use crate::{Operator, operator::Operators, transaction::FlowTransaction};

pub struct SinkTableViewOperator {
	#[allow(dead_code)]
	parent: Arc<Operators>,
	node: FlowNodeId,
	view: ResolvedView,
	underlying: TableId,

	key_prefix: Vec<u8>,
}

impl SinkTableViewOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, view: ResolvedView, underlying: TableId) -> Self {
		let mut key_prefix: Vec<u8> = Vec::with_capacity(10);
		key_prefix.push(encode_u8(KeyKind::Row as u8));
		serialize_shape_id(&ShapeId::table(underlying), &mut key_prefix);
		Self {
			parent,
			node,
			view,
			underlying,
			key_prefix,
		}
	}

	#[inline]
	fn row_key(&self, row: RowNumber) -> EncodedKey {
		let mut buf = Vec::with_capacity(self.key_prefix.len() + 9);
		buf.extend_from_slice(&self.key_prefix);
		encode_u64_varint(row.0, &mut buf);
		EncodedKey::new(buf)
	}
}

impl Operator for SinkTableViewOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> u32 {
		CAPABILITY_ALL_STANDARD
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let view = self.view.def().clone();
		let shape: RowShape = view.columns().into();

		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => self.apply_table_view_insert(txn, &view, &shape, post)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_table_view_update(txn, &view, &shape, pre, post)?,
				Diff::Remove {
					pre,
					..
				} => self.apply_table_view_remove(txn, &view, &shape, pre)?,
			}
		}

		Ok(Change::from_flow(self.node, change.version, Vec::new(), change.changed_at))
	}
}

impl SinkTableViewOperator {
	#[inline]
	fn apply_table_view_insert(
		&self,
		txn: &mut FlowTransaction,
		view: &View,
		shape: &RowShape,
		post: &Arc<Columns>,
	) -> Result<()> {
		let coerced = coerce_columns(post, view.columns())?;
		let row_count = coerced.row_count();
		let mut ids: Vec<RowNumber> = Vec::with_capacity(row_count);
		let mut encoded_rows: Vec<EncodedRow> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let row_number = coerced.row_numbers[row_idx];
			let (_, encoded) = encode_row_at_index(&coerced, row_idx, shape, row_number)?;
			ids.push(row_number);
			encoded_rows.push(encoded);
		}
		let has_pre = !txn.view_row_pre_insert_interceptors().is_empty();
		let has_post = !txn.view_row_post_insert_interceptors().is_empty();
		if has_pre {
			ViewRowInterceptor::pre_insert(txn, view, &ids, &mut encoded_rows)?;
		}
		let mut keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		for row_number in ids.iter() {
			keys.push(self.row_key(*row_number));
		}
		txn.set_batch(&keys, &encoded_rows)?;
		if has_post {
			ViewRowInterceptor::post_insert(txn, view, &ids, &encoded_rows)?;
		}
		emit_view_change(txn, view, Diff::insert(coerced));
		Ok(())
	}

	#[inline]
	fn apply_table_view_update(
		&self,
		txn: &mut FlowTransaction,
		view: &View,
		shape: &RowShape,
		pre: &Arc<Columns>,
		post: &Arc<Columns>,
	) -> Result<()> {
		let coerced_pre = coerce_columns(pre, view.columns())?;
		let coerced_post = coerce_columns(post, view.columns())?;
		let row_count = coerced_post.row_count();
		let mut post_ids: Vec<RowNumber> = Vec::with_capacity(row_count);
		let mut pre_keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut post_keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut pre_encoded_rows: Vec<EncodedRow> = Vec::with_capacity(row_count);
		let mut post_encoded_rows: Vec<EncodedRow> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let pre_row_number = coerced_pre.row_numbers[row_idx];
			let post_row_number = coerced_post.row_numbers[row_idx];
			let (_, pre_encoded) = encode_row_at_index(&coerced_pre, row_idx, shape, pre_row_number)?;
			let (_, mut post_encoded) =
				encode_row_at_index(&coerced_post, row_idx, shape, post_row_number)?;

			let pre_key = self.row_key(pre_row_number);
			let post_key = self.row_key(post_row_number);

			let prior_created = match txn.get(&post_key)? {
				Some(prior) if prior.len() >= SHAPE_HEADER_SIZE => {
					let c = prior.created_at_nanos();
					if c != 0 {
						Some(c)
					} else {
						None
					}
				}
				_ => None,
			};
			if prior_created.is_none() && pre_row_number != post_row_number {
				match txn.get(&pre_key)? {
					Some(prior) if prior.len() >= SHAPE_HEADER_SIZE => {
						let c = prior.created_at_nanos();
						if c != 0 && post_encoded.len() >= SHAPE_HEADER_SIZE {
							let updated = post_encoded.updated_at_nanos();
							post_encoded.set_timestamps(c, updated);
						}
					}
					_ => {}
				}
			} else if let Some(c) = prior_created
				&& post_encoded.len() >= SHAPE_HEADER_SIZE
			{
				let updated = post_encoded.updated_at_nanos();
				post_encoded.set_timestamps(c, updated);
			}

			post_ids.push(post_row_number);
			pre_keys.push(pre_key);
			post_keys.push(post_key);
			pre_encoded_rows.push(pre_encoded);
			post_encoded_rows.push(post_encoded);
		}

		let has_pre = !txn.view_row_pre_update_interceptors().is_empty();
		let has_post = !txn.view_row_post_update_interceptors().is_empty();
		if has_pre {
			ViewRowInterceptor::pre_update(txn, view, &post_ids, &mut post_encoded_rows)?;
		}
		txn.remove_batch(&pre_keys)?;
		txn.set_batch(&post_keys, &post_encoded_rows)?;
		if has_post {
			ViewRowInterceptor::post_update(txn, view, &post_ids, &post_encoded_rows, &pre_encoded_rows)?;
		}

		emit_view_change(txn, view, Diff::update(coerced_pre, coerced_post));
		Ok(())
	}

	#[inline]
	fn apply_table_view_remove(
		&self,
		txn: &mut FlowTransaction,
		view: &View,
		shape: &RowShape,
		pre: &Arc<Columns>,
	) -> Result<()> {
		let coerced = coerce_columns(pre, view.columns())?;
		let row_count = coerced.row_count();
		let mut ids: Vec<RowNumber> = Vec::with_capacity(row_count);
		let mut encoded_rows: Vec<EncodedRow> = Vec::with_capacity(row_count);
		let mut keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let row_number = coerced.row_numbers[row_idx];
			let (_, encoded) = encode_row_at_index(&coerced, row_idx, shape, row_number)?;
			keys.push(self.row_key(row_number));
			ids.push(row_number);
			encoded_rows.push(encoded);
		}
		let has_pre = !txn.view_row_pre_delete_interceptors().is_empty();
		let has_post = !txn.view_row_post_delete_interceptors().is_empty();
		if has_pre {
			ViewRowInterceptor::pre_delete(txn, view, &ids)?;
		}
		txn.remove_batch(&keys)?;
		if has_post {
			ViewRowInterceptor::post_delete(txn, view, &ids, &encoded_rows)?;
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
