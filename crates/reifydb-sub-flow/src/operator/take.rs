// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CowVec, RowNumber,
	flow::{FlowChange, FlowDiff},
	interface::{CommandTransaction, Evaluator},
	row::EncodedKey,
};
use serde::{Deserialize, Serialize};

use crate::{
	Result,
	operator::{Operator, OperatorContext},
};

// Key for storing take state
#[derive(Debug, Clone)]
struct FlowTakeStateKey {
	flow_id: u64,
	node_id: u64,
}

impl FlowTakeStateKey {
	const KEY_PREFIX: u8 = 0xF3;

	fn new(flow_id: u64, node_id: u64) -> Self {
		Self {
			flow_id,
			node_id,
		}
	}

	fn encode(&self) -> EncodedKey {
		let mut key = Vec::new();
		key.push(Self::KEY_PREFIX);
		key.extend(&self.flow_id.to_be_bytes());
		key.extend(&self.node_id.to_be_bytes());
		EncodedKey(CowVec::new(key))
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TakeState {
	current_count: usize,
	row_ids: Vec<RowNumber>,
}

pub struct TakeOperator {
	flow_id: u64,
	node_id: u64,
	limit: usize,
}

impl TakeOperator {
	pub fn new(flow_id: u64, node_id: u64, limit: usize) -> Self {
		Self {
			flow_id,
			node_id,
			limit,
		}
	}

	fn load_state<T: CommandTransaction>(
		&self,
		txn: &mut T,
	) -> Result<TakeState> {
		let key = FlowTakeStateKey::new(self.flow_id, self.node_id);

		match txn.get(&key.encode())? {
			Some(versioned) => {
				let bytes = versioned.row.as_ref();
				bincode::deserialize(bytes).map_err(|e| {
					reifydb_core::Error(
						reifydb_core::internal_error!(
							"Failed to deserialize TakeState: {}",
							e
						),
					)
				})
			}
			None => Ok(TakeState {
				current_count: 0,
				row_ids: Vec::new(),
			}),
		}
	}

	fn save_state<T: CommandTransaction>(
		&self,
		txn: &mut T,
		state: &TakeState,
	) -> Result<()> {
		let key = FlowTakeStateKey::new(self.flow_id, self.node_id);
		let serialized = bincode::serialize(state).map_err(|e| {
			reifydb_core::Error(reifydb_core::internal_error!(
				"Failed to serialize TakeState: {}",
				e
			))
		})?;

		txn.set(
			&key.encode(),
			reifydb_core::row::EncodedRow(
				reifydb_core::util::CowVec::new(serialized),
			),
		)?;
		Ok(())
	}
}

impl<E: Evaluator> Operator<E> for TakeOperator {
	fn apply<T: CommandTransaction>(
		&self,
		ctx: &mut OperatorContext<E, T>,
		change: &FlowChange,
	) -> Result<FlowChange> {
		// Load current state
		let mut state = self.load_state(ctx.txn)?;
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
		self.save_state(ctx.txn, &state)?;

		Ok(FlowChange {
			diffs: output_diffs,
			metadata: change.metadata.clone(),
		})
	}
}
