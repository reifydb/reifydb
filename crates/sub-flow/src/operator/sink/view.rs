// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	interface::{FlowNodeId, PrimitiveId, ResolvedView},
	key::RowKey,
	value::{column::Columns, encoded::EncodedValuesNamedLayout},
};
use reifydb_engine::StandardColumnEvaluator;
use reifydb_sdk::{FlowChange, FlowDiff};
use reifydb_type::RowNumber;

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

#[async_trait]
impl Operator for SinkViewOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	async fn apply(
		&self,
		txn: &mut FlowTransaction<'_>,
		change: FlowChange,
		_evaluator: &StandardColumnEvaluator,
	) -> crate::Result<FlowChange> {
		// Write rows to the view storage
		let view_def = self.view.def().clone();
		let layout: EncodedValuesNamedLayout = (&view_def).into();

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
							encode_row_at_index(&coerced, row_idx, &layout);

						let key = RowKey::encoded(PrimitiveId::view(view_def.id), row_number);
						txn.set(&key, encoded).await?;
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
							encode_row_at_index(&coerced_pre, row_idx, &layout);
						let (post_row_number, post_encoded) =
							encode_row_at_index(&coerced_post, row_idx, &layout);

						let old_key =
							RowKey::encoded(PrimitiveId::view(view_def.id), pre_row_number);
						let new_key = RowKey::encoded(
							PrimitiveId::view(view_def.id),
							post_row_number,
						);
						txn.remove(&old_key).await?;
						txn.set(&new_key, post_encoded).await?;
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
						txn.remove(&key).await?;
					}
				}
			}
		}

		Ok(FlowChange::internal(self.node, change.version, Vec::new()))
	}

	async fn pull(&self, _txn: &mut FlowTransaction<'_>, _rows: &[RowNumber]) -> crate::Result<Columns> {
		unreachable!()
	}
}
