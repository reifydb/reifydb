// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::{FlowChange, FlowDiff},
	interface::{
		EncodableKey, MultiVersionCommandTransaction, ResolvedView, RowKey, SourceId, Transaction, ViewId,
	},
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};

use crate::Operator;

pub struct SinkViewOperator {
	view: ResolvedView<'static>,
}

impl SinkViewOperator {
	pub fn new(view: ResolvedView<'static>) -> Self {
		Self {
			view,
		}
	}
}

impl<T: Transaction> Operator<T> for SinkViewOperator {
	fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// For now, we just directly write the row to the view
		// TODO: This assumes source and view layouts are compatible - Maybe needs to be transformed

		for diff in &change.diffs {
			match diff {
				FlowDiff::Insert {
					post: row_data,
					..
				} => {
					let row_id = row_data.number;
					let row = row_data.encoded.clone();

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
					let row_id = row_data.number;
					let new_row = row_data.encoded.clone();

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

		Ok(change)
	}
}
