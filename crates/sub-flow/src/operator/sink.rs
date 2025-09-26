// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::{FlowChange, FlowDiff},
	interface::{
		EncodableKey, FlowNodeId, MultiVersionCommandTransaction, ResolvedView, RowKey, SourceId, Transaction,
	},
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};

use crate::Operator;

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

		for (i, diff) in change.diffs.iter().enumerate() {
			match diff {
				FlowDiff::Insert {
					post: row_data,
					..
				} => {
					let transformed_row = match evaluator.coerce(row_data, target_columns) {
						Ok(r) => r,
						Err(err) => {
							print!("{:#?}", err);
							return Err(err);
						}
					};

					// let transformed_row = evaluator.coerce(row_data, target_columns)?;

					let row_id = transformed_row.number;
					let row = transformed_row.encoded;

					let key = RowKey {
						source: SourceId::view(self.view.def().id),
						row: row_id,
					}
					.encode();

					txn.set(&key, row)?;
				}
				FlowDiff::Update {
					pre: _,
					post: row_data,
					..
				} => {
					// Transform the row to match the view schema
					let transformed_row = evaluator.coerce(row_data, target_columns)?;

					let row_id = transformed_row.number;
					let new_row = transformed_row.encoded;

					let key = RowKey {
						source: SourceId::view(self.view.def().id),
						row: row_id,
					}
					.encode();

					txn.set(&key, new_row)?;
				}
				FlowDiff::Remove {
					pre: row_data,
					..
				} => {
					let row_id = row_data.number;

					let key = RowKey {
						source: SourceId::view(self.view.def().id),
						row: row_id,
					}
					.encode();

					txn.remove(&key)?;
				}
			}
		}

		// Sink is a terminal node - don't propagate changes further
		Ok(FlowChange::internal(self.node, Vec::new()))
	}
}
