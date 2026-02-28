// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::schema::Schema,
	interface::{
		catalog::{flow::FlowNodeId, primitive::PrimitiveId},
		change::{Change, ChangeOrigin, Diff},
		resolved::ResolvedView,
	},
	key::row::RowKey,
	value::column::columns::Columns,
};
use reifydb_transaction::interceptor::view::ViewInterceptor;
use reifydb_type::{Result, value::row_number::RowNumber};

use super::{coerce_columns, encode_row_at_index};
use crate::{Operator, operator::Operators, transaction::FlowTransaction};

pub struct SinkViewOperator {
	#[allow(dead_code)]
	parent: Arc<Operators>,
	node: FlowNodeId,
	view: ResolvedView,
}

impl SinkViewOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, view: ResolvedView) -> Self {
		Self {
			parent,
			node,
			view,
		}
	}
}

impl Operator for SinkViewOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let view_def = self.view.def().clone();
		let schema: Schema = (&view_def.columns).into();

		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
				} => {
					// Coerce columns to match view schema types (already decoded at source)
					let coerced = coerce_columns(post, &view_def.columns)?;
					let row_count = coerced.row_count();
					for row_idx in 0..row_count {
						let row_number = coerced.row_numbers[row_idx];
						let (_, encoded) =
							encode_row_at_index(&coerced, row_idx, &schema, row_number);

						ViewInterceptor::pre_insert(txn, &view_def, row_number, &encoded)?;
						let key = RowKey::encoded(PrimitiveId::view(view_def.id), row_number);
						txn.set(&key, encoded.clone())?;
						ViewInterceptor::post_insert(txn, &view_def, row_number, &encoded)?;
					}
					// Emit view change for downstream transactional flows
					let version = txn.version();
					txn.push_view_change(Change {
						origin: ChangeOrigin::Primitive(PrimitiveId::view(view_def.id)),
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
					// Coerce columns to match view schema types (already decoded at source)
					let coerced_pre = coerce_columns(pre, &view_def.columns)?;
					let coerced_post = coerce_columns(post, &view_def.columns)?;
					let row_count = coerced_post.row_count();
					for row_idx in 0..row_count {
						let pre_row_number = coerced_pre.row_numbers[row_idx];
						let post_row_number = coerced_post.row_numbers[row_idx];
						let (_, pre_encoded) = encode_row_at_index(
							&coerced_pre,
							row_idx,
							&schema,
							pre_row_number,
						);
						let (_, post_encoded) = encode_row_at_index(
							&coerced_post,
							row_idx,
							&schema,
							post_row_number,
						);

						ViewInterceptor::pre_update(
							txn,
							&view_def,
							post_row_number,
							&post_encoded,
						)?;
						let old_key =
							RowKey::encoded(PrimitiveId::view(view_def.id), pre_row_number);
						let new_key = RowKey::encoded(
							PrimitiveId::view(view_def.id),
							post_row_number,
						);
						txn.remove(&old_key)?;
						txn.set(&new_key, post_encoded.clone())?;
						ViewInterceptor::post_update(
							txn,
							&view_def,
							post_row_number,
							&post_encoded,
							&pre_encoded,
						)?;
					}
					// Emit view change for downstream transactional flows
					let version = txn.version();
					txn.push_view_change(Change {
						origin: ChangeOrigin::Primitive(PrimitiveId::view(view_def.id)),
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
					// Coerce columns to match view schema types (already decoded at source)
					let coerced = coerce_columns(pre, &view_def.columns)?;
					let row_count = coerced.row_count();
					for row_idx in 0..row_count {
						let row_number = coerced.row_numbers[row_idx];
						let (_, encoded) =
							encode_row_at_index(&coerced, row_idx, &schema, row_number);

						ViewInterceptor::pre_delete(txn, &view_def, row_number)?;
						let key = RowKey::encoded(PrimitiveId::view(view_def.id), row_number);
						txn.remove(&key)?;
						ViewInterceptor::post_delete(txn, &view_def, row_number, &encoded)?;
					}
					// Emit view change for downstream transactional flows
					let version = txn.version();
					txn.push_view_change(Change {
						origin: ChangeOrigin::Primitive(PrimitiveId::view(view_def.id)),
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
