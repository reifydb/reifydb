// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::rc::Rc;

use reifydb_core::{
	encoded::schema::Schema,
	interface::{
		catalog::{flow::FlowNodeId, primitive::PrimitiveId},
		resolved::ResolvedView,
	},
	key::row::RowKey,
	value::column::columns::Columns,
};
use reifydb_engine::evaluate::column::StandardColumnEvaluator;
use reifydb_sdk::flow::{FlowChange, FlowDiff};
use reifydb_type::value::row_number::RowNumber;

use super::{coerce_columns, encode_row_at_index};
use crate::{Operator, operator::Operators, transaction::FlowTransaction};

pub struct SinkViewOperator {
	#[allow(dead_code)]
	parent: Rc<Operators>,
	node: FlowNodeId,
	view: ResolvedView,
}

impl SinkViewOperator {
	pub fn new(parent: Rc<Operators>, node: FlowNodeId, view: ResolvedView) -> Self {
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

	fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardColumnEvaluator,
	) -> reifydb_type::Result<FlowChange> {
		// Write rows to the view storage
		let view_def = self.view.def().clone();
		let schema: Schema = (&view_def.columns).into();

		for diff in change.diffs.iter() {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					// Coerce columns to match view schema types
					let coerced = coerce_columns(post, &view_def.columns)?;
					let row_count = coerced.row_count();
					for row_idx in 0..row_count {
						let (row_number, encoded) =
							encode_row_at_index(&coerced, row_idx, &schema);

						let key = RowKey::encoded(PrimitiveId::view(view_def.id), row_number);
						txn.set(&key, encoded)?;
					}
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					// Coerce columns to match view schema types
					let coerced_pre = coerce_columns(pre, &view_def.columns)?;
					let coerced_post = coerce_columns(post, &view_def.columns)?;
					let row_count = coerced_post.row_count();
					for row_idx in 0..row_count {
						let (pre_row_number, _) =
							encode_row_at_index(&coerced_pre, row_idx, &schema);
						let (post_row_number, post_encoded) =
							encode_row_at_index(&coerced_post, row_idx, &schema);

						let old_key =
							RowKey::encoded(PrimitiveId::view(view_def.id), pre_row_number);
						let new_key = RowKey::encoded(
							PrimitiveId::view(view_def.id),
							post_row_number,
						);
						txn.remove(&old_key)?;
						txn.set(&new_key, post_encoded)?;
					}
				}
				FlowDiff::Remove {
					pre,
				} => {
					// Coerce columns to match view schema types - only need row numbers for remove
					let coerced = coerce_columns(pre, &view_def.columns)?;
					let row_count = coerced.row_count();
					for row_idx in 0..row_count {
						let row_number = coerced.row_numbers[row_idx];

						let key = RowKey::encoded(PrimitiveId::view(view_def.id), row_number);
						txn.remove(&key)?;
					}
				}
			}
		}

		Ok(FlowChange::internal(self.node, change.version, Vec::new()))
	}

	fn pull(&self, _txn: &mut FlowTransaction, _rows: &[RowNumber]) -> reifydb_type::Result<Columns> {
		unreachable!()
	}
}
