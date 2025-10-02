// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	EncodableKey, FlowNodeId, MultiVersionCommandTransaction, ResolvedView, RowKey, SourceId, Transaction,
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};

use crate::{
	Operator,
	flow::{FlowChange, FlowDiff},
};

pub struct SinkViewOperator {
	node: FlowNodeId,
	view: ResolvedView<'static>,
}

impl SinkViewOperator {
	pub fn new(node: FlowNodeId, view: ResolvedView<'static>) -> Self {
		Self {
			node,
			view,
		}
	}
}

impl<T: Transaction> Operator<T> for SinkViewOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// Transform rows to match the view's schema before writing
		let target_columns = self.view.columns();

		for diff in change.diffs.iter() {
			match diff {
				FlowDiff::Insert {
					post,
					..
				} => {
					let transformed_row = evaluator.coerce(post, target_columns)?;

					let row_id = post.number;
					let row = transformed_row.encoded;

					let key = RowKey {
						source: SourceId::view(self.view.def().id),
						row: row_id,
					}
					.encode();

					txn.set(&key, row)?;
				}
				FlowDiff::Update {
					pre,
					post,
					..
				} => {
					// Transform the encoded to match the view schema
					let transformed_row = evaluator.coerce(post, target_columns)?;

					let key = RowKey {
						source: SourceId::view(self.view.def().id),
						row: post.number,
					}
					.encode();

					txn.remove(&RowKey {
						source: SourceId::view(self.view.def().id),
						row: pre.number,
					}
					.encode())?;

					txn.set(&key, transformed_row.encoded)?;
				}
				FlowDiff::Remove {
					pre,
					..
				} => {
					let key = RowKey {
						source: SourceId::view(self.view.def().id),
						row: pre.number,
					}
					.encode();

					txn.remove(&key)?;
				}
			}
		}

		// Sink is a terminal node - don't propagate changes further
		Ok(FlowChange::internal(self.node, change.version, Vec::new()))
	}
}
