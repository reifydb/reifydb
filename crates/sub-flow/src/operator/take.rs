// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CowVec, EncodedKey,
	flow::{FlowChange, FlowDiff},
	interface::{FlowNodeId, Transaction},
	row::EncodedRow,
};
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};
use reifydb_type::RowNumber;
use serde::{Deserialize, Serialize};

use crate::{
	Result,
	operator::{Operator, stateful::StatefulOperator},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TakeState {
	current_count: usize,
	row_ids: Vec<RowNumber>,
}

pub struct TakeOperator {
	node: FlowNodeId,
	limit: usize,
}

impl TakeOperator {
	pub fn new(node: FlowNodeId, limit: usize) -> Self {
		Self {
			node,
			limit,
		}
	}

	fn load_state<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
	) -> Result<TakeState> {
		let empty_key = EncodedKey::new(Vec::new());
		let state_row = self.get(txn, &empty_key)?;

		if state_row.as_ref().is_empty() {
			Ok(TakeState {
				current_count: 0,
				row_ids: Vec::new(),
			})
		} else {
			serde_json::from_slice(state_row.as_ref()).map_err(
				|e| {
					reifydb_type::Error(
						reifydb_type::internal_error!(
							"Failed to deserialize TakeState: {}",
							e
						),
					)
				},
			)
		}
	}

	fn save_state<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		state: &TakeState,
	) -> Result<()> {
		let empty_key = EncodedKey::new(Vec::new());
		let serialized = serde_json::to_vec(state).map_err(|e| {
			reifydb_type::Error(reifydb_type::internal_error!(
				"Failed to serialize TakeState: {}",
				e
			))
		})?;

		self.set(txn, &empty_key, EncodedRow(CowVec::new(serialized)))?;
		Ok(())
	}
}

impl<T: Transaction> Operator<T> for TakeOperator {
	fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		change: &FlowChange,
		evaluator: &StandardEvaluator,
	) -> Result<FlowChange> {
		// Load current state
		let mut state = self.load_state(txn)?;
		let mut output_diffs = Vec::new();

		for diff in &change.diffs {
			match diff {
				FlowDiff::Insert {
					source,
					row_ids,
					after,
				} => {
					// For DESC order (default), we need to
					// keep the highest row IDs
					let mut all_rows: Vec<_> =
						state.row_ids.clone();
					all_rows.extend_from_slice(row_ids);

					// Sort in descending order (highest IDs
					// first)
					all_rows.sort_by(|a, b| b.0.cmp(&a.0));

					// Take only the limit
					let new_top_rows: Vec<_> = all_rows
						.into_iter()
						.take(self.limit)
						.collect();

					// Find what changed
					let mut rows_to_add = Vec::new();
					let mut rows_to_remove = Vec::new();

					for &row_id in &new_top_rows {
						if !state
							.row_ids
							.contains(&row_id)
						{
							rows_to_add
								.push(row_id);
						}
					}

					for &row_id in &state.row_ids {
						if !new_top_rows
							.contains(&row_id)
						{
							rows_to_remove
								.push(row_id);
						}
					}

					// Emit changes
					if !rows_to_remove.is_empty() {
						// These rows are no longer in
						// top N
						output_diffs
							.push(FlowDiff::Remove {
							source: *source,
							row_ids: rows_to_remove,
							before: after.clone(), /* Simplified - should track actual data */
						});
					}

					if !rows_to_add.is_empty() {
						// These are new top N rows
						output_diffs
							.push(FlowDiff::Insert {
							source: *source,
							row_ids: rows_to_add,
							after: after.clone(),
						});
					}

					// Update state
					state.row_ids = new_top_rows;
					state.current_count =
						state.row_ids.len();
				}
				FlowDiff::Remove {
					source,
					row_ids,
					before,
				} => {
					// Remove these rows from our state
					let mut new_state_rows = Vec::new();
					for &row_id in &state.row_ids {
						if !row_ids.contains(&row_id) {
							new_state_rows
								.push(row_id);
						}
					}

					// Pass through the removal if it was in
					// our top N
					let mut rows_to_remove = Vec::new();
					for &row_id in row_ids {
						if state.row_ids
							.contains(&row_id)
						{
							rows_to_remove
								.push(row_id);
						}
					}

					if !rows_to_remove.is_empty() {
						output_diffs
							.push(FlowDiff::Remove {
							source: *source,
							row_ids: rows_to_remove,
							before: before.clone(),
						});
					}

					// Update state
					state.row_ids = new_state_rows;
					state.current_count =
						state.row_ids.len();

					// Note: In a full implementation, we'd
					// need to check if there are
					// additional rows to bring into the top
					// N to replace removed ones
				}
				FlowDiff::Update {
					source,
					row_ids,
					before,
					after,
				} => {
					// Only pass through updates for rows in
					// our top N
					let mut rows_to_update = Vec::new();
					for &row_id in row_ids {
						if state.row_ids
							.contains(&row_id)
						{
							rows_to_update
								.push(row_id);
						}
					}

					if !rows_to_update.is_empty() {
						output_diffs
							.push(FlowDiff::Update {
							source: *source,
							row_ids: rows_to_update,
							before: before.clone(),
							after: after.clone(),
						});
					}
				}
			}
		}

		// Save updated state
		self.save_state(txn, &state)?;

		Ok(FlowChange {
			diffs: output_diffs,
			metadata: change.metadata.clone(),
		})
	}
}

impl<T: Transaction> StatefulOperator<T> for TakeOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}
}
