// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::shape::RowShape,
	interface::{
		catalog::{flow::FlowNodeId, id::TableId, shape::ShapeId},
		change::{Change, ChangeOrigin, Diff},
		resolved::ResolvedView,
	},
	key::row::RowKey,
	value::column::columns::Columns,
};
use reifydb_transaction::interceptor::view_row::ViewRowInterceptor;
use reifydb_type::{Result, value::row_number::RowNumber};

use super::{coerce_columns, encode_row_at_index};
use crate::{Operator, operator::Operators, transaction::FlowTransaction};

pub struct SinkTableViewOperator {
	#[allow(dead_code)]
	parent: Arc<Operators>,
	node: FlowNodeId,
	view: ResolvedView,
	underlying: TableId,
}

impl SinkTableViewOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, view: ResolvedView, underlying: TableId) -> Self {
		Self {
			parent,
			node,
			view,
			underlying,
		}
	}
}

impl Operator for SinkTableViewOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let view = self.view.def().clone();
		let shape: RowShape = view.columns().into();
		let object_id = ShapeId::table(self.underlying);

		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
				} => {
					let coerced = coerce_columns(post, view.columns())?;
					let row_count = coerced.row_count();
					for row_idx in 0..row_count {
						let row_number = coerced.row_numbers[row_idx];

						let (_, encoded) =
							encode_row_at_index(&coerced, row_idx, &shape, row_number);

						let encoded = ViewRowInterceptor::pre_insert(
							txn, &view, row_number, encoded,
						)?;
						let key = RowKey::encoded(object_id, row_number);
						txn.set(&key, encoded.clone())?;
						ViewRowInterceptor::post_insert(txn, &view, row_number, &encoded)?;
					}
					let version = txn.version();
					txn.track_flow_change(Change {
						origin: ChangeOrigin::Shape(ShapeId::view(view.id())),
						version,
						diffs: vec![Diff::Insert {
							post: coerced,
						}],
					});
				}
				Diff::Update {
					pre,
					post,
				} => {
					let coerced_pre = coerce_columns(pre, view.columns())?;
					let coerced_post = coerce_columns(post, view.columns())?;
					let row_count = coerced_post.row_count();
					for row_idx in 0..row_count {
						let pre_row_number = coerced_pre.row_numbers[row_idx];
						let post_row_number = coerced_post.row_numbers[row_idx];
						let (_, pre_encoded) = encode_row_at_index(
							&coerced_pre,
							row_idx,
							&shape,
							pre_row_number,
						);
						let (_, post_encoded) = encode_row_at_index(
							&coerced_post,
							row_idx,
							&shape,
							post_row_number,
						);

						let post_encoded = ViewRowInterceptor::pre_update(
							txn,
							&view,
							post_row_number,
							post_encoded,
						)?;
						let pre_key = RowKey::encoded(object_id, pre_row_number);
						let post_key = RowKey::encoded(object_id, post_row_number);
						txn.remove(&pre_key)?;
						txn.set(&post_key, post_encoded.clone())?;
						ViewRowInterceptor::post_update(
							txn,
							&view,
							post_row_number,
							&post_encoded,
							&pre_encoded,
						)?;
					}
					let version = txn.version();
					txn.track_flow_change(Change {
						origin: ChangeOrigin::Shape(ShapeId::view(view.id())),
						version,
						diffs: vec![Diff::Update {
							pre: coerced_pre,
							post: coerced_post,
						}],
					});
				}
				Diff::Remove {
					pre,
				} => {
					let coerced = coerce_columns(pre, view.columns())?;
					let row_count = coerced.row_count();
					for row_idx in 0..row_count {
						let row_number = coerced.row_numbers[row_idx];
						let (_, encoded) =
							encode_row_at_index(&coerced, row_idx, &shape, row_number);

						ViewRowInterceptor::pre_delete(txn, &view, row_number)?;
						let key = RowKey::encoded(object_id, row_number);
						txn.remove(&key)?;
						ViewRowInterceptor::post_delete(txn, &view, row_number, &encoded)?;
					}
					let version = txn.version();
					txn.track_flow_change(Change {
						origin: ChangeOrigin::Shape(ShapeId::view(view.id())),
						version,
						diffs: vec![Diff::Remove {
							pre: coerced,
						}],
					});
				}
			}
		}

		Ok(Change::from_flow(self.node, change.version, Vec::new()))
	}

	fn pull(&self, _txn: &mut FlowTransaction, _rows: &[RowNumber]) -> Result<Columns> {
		unreachable!()
	}
}
