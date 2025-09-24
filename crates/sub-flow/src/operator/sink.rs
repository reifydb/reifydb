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
		println!(
			"SINK[{:?}]: Applying change with {} diffs from {:?}",
			self.node,
			change.diffs.len(),
			change.origin
		);

		// Transform rows to match the view's schema before writing
		let target_columns = self.view.columns();
		println!("SINK[{:?}]: Target view has {} columns", self.node, target_columns.len());

		for (i, diff) in change.diffs.iter().enumerate() {
			match diff {
				FlowDiff::Insert {
					post: row_data,
					..
				} => {
					println!(
						"SINK[{:?}]: Processing INSERT[{}] with {} input columns",
						self.node,
						i,
						row_data.layout.fields.len()
					);
					// Transform the row to match the view schema
					let transformed_row = evaluator.coerce(row_data, target_columns)?;
					println!(
						"SINK[{:?}]: Coerced to {} output columns",
						self.node,
						transformed_row.layout.fields.len()
					);

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

		println!("SINK[{:?}]: Completed processing, sink is terminal", self.node);
		// Sink is a terminal node - don't propagate changes further
		Ok(FlowChange::internal(self.node, Vec::new()))
	}
}
