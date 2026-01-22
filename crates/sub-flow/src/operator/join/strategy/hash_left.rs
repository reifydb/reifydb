// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{common::CommitVersion, value::column::columns::Columns};
use reifydb_runtime::hash::Hash128;
use reifydb_sdk::flow::FlowDiff;

use super::hash::{
	add_to_state_entry_batch, emit_joined_columns_batch, emit_remove_joined_columns_batch,
	emit_update_joined_columns, is_first_right_row, pull_left_columns, remove_from_state_entry,
	update_row_in_entry,
};
use crate::{
	operator::join::{
		operator::JoinOperator,
		state::{JoinSide, JoinState},
	},
	transaction::FlowTransaction,
};

pub(crate) struct LeftHashJoin;

impl LeftHashJoin {
	/// Handle insert for rows with undefined join keys (emits unmatched left row)
	pub(crate) fn handle_insert_undefined(
		&self,
		txn: &mut FlowTransaction,
		post: &Columns,
		row_idx: usize,
		side: JoinSide,
		_state: &mut JoinState,
		operator: &JoinOperator,
	) -> reifydb_type::Result<Vec<FlowDiff>> {
		match side {
			JoinSide::Left => {
				// Undefined key in left join still emits the row
				let unmatched = operator.unmatched_left_columns(txn, post, row_idx)?;
				Ok(vec![FlowDiff::Insert {
					post: unmatched,
				}])
			}
			JoinSide::Right => {
				// Right side inserts with undefined keys don't produce output
				Ok(Vec::new())
			}
		}
	}

	/// Handle remove for rows with undefined join keys
	pub(crate) fn handle_remove_undefined(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		row_idx: usize,
		side: JoinSide,
		_state: &mut JoinState,
		operator: &JoinOperator,
		_version: CommitVersion,
	) -> reifydb_type::Result<Vec<FlowDiff>> {
		let row_number = pre.row_numbers[row_idx];

		match side {
			JoinSide::Left => {
				// Undefined key - remove the unmatched row
				let unmatched = operator.unmatched_left_columns(txn, pre, row_idx)?;
				operator.cleanup_left_row_joins(txn, *row_number)?;
				Ok(vec![FlowDiff::Remove {
					pre: unmatched,
				}])
			}
			JoinSide::Right => {
				// Right side removes with undefined keys don't produce output
				Ok(Vec::new())
			}
		}
	}

	/// Handle update for rows with undefined join keys
	pub(crate) fn handle_update_undefined(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		post: &Columns,
		row_idx: usize,
		side: JoinSide,
		_state: &mut JoinState,
		operator: &JoinOperator,
		_version: CommitVersion,
	) -> reifydb_type::Result<Vec<FlowDiff>> {
		match side {
			JoinSide::Left => {
				// Both keys are undefined - update the row
				let unmatched_pre = operator.unmatched_left_columns(txn, pre, row_idx)?;
				let unmatched_post = operator.unmatched_left_columns(txn, post, row_idx)?;
				Ok(vec![FlowDiff::Update {
					pre: unmatched_pre,
					post: unmatched_post,
				}])
			}
			JoinSide::Right => {
				// Right side updates with undefined keys don't produce output
				Ok(Vec::new())
			}
		}
	}

	/// Handle insert for rows with defined join keys (batched by key)
	pub(crate) fn handle_insert(
		&self,
		txn: &mut FlowTransaction,
		post: &Columns,
		indices: &[usize],
		side: JoinSide,
		key_hash: &Hash128,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> reifydb_type::Result<Vec<FlowDiff>> {
		if indices.is_empty() {
			return Ok(Vec::new());
		}

		let mut result = Vec::new();

		match side {
			JoinSide::Left => {
				// Add all rows to state first
				add_to_state_entry_batch(txn, &mut state.left, key_hash, post, indices)?;

				// Check if there are matching right rows
				if let Some(diff) = emit_joined_columns_batch(
					txn,
					post,
					indices,
					JoinSide::Left,
					&state.right,
					key_hash,
					operator,
					&operator.right_parent,
				)? {
					result.push(diff);
				} else {
					// No matches - emit unmatched left rows for all
					let unmatched = operator.unmatched_left_columns_batch(txn, post, indices)?;
					result.push(FlowDiff::Insert {
						post: unmatched,
					});
				}
			}
			JoinSide::Right => {
				let is_first = is_first_right_row(txn, &state.right, key_hash)?;

				// Add all rows to state first
				add_to_state_entry_batch(txn, &mut state.right, key_hash, post, indices)?;

				// If first right row(s), remove previously emitted unmatched left rows
				if is_first {
					if let Some(left_entry) = state.left.get(txn, key_hash)? {
						let left_columns = operator.left_parent.pull(txn, &left_entry.rows)?;
						let left_indices: Vec<usize> = (0..left_columns.row_count()).collect();
						let unmatched = operator.unmatched_left_columns_batch(
							txn,
							&left_columns,
							&left_indices,
						)?;
						result.push(FlowDiff::Remove {
							pre: unmatched,
						});
					}
				}

				// Emit all joined rows in one batch
				if let Some(diff) = emit_joined_columns_batch(
					txn,
					post,
					indices,
					JoinSide::Right,
					&state.left,
					key_hash,
					operator,
					&operator.left_parent,
				)? {
					result.push(diff);
				}
			}
		}

		Ok(result)
	}

	/// Handle remove for rows with defined join keys (batched by key)
	pub(crate) fn handle_remove(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		indices: &[usize],
		side: JoinSide,
		key_hash: &Hash128,
		state: &mut JoinState,
		operator: &JoinOperator,
		_version: CommitVersion,
	) -> reifydb_type::Result<Vec<FlowDiff>> {
		if indices.is_empty() {
			return Ok(Vec::new());
		}

		let mut result = Vec::new();

		match side {
			JoinSide::Left => {
				// Clean up row number mappings for all left rows
				for &idx in indices {
					let row_number = pre.row_numbers[idx];
					operator.cleanup_left_row_joins(txn, *row_number)?;
				}

				// First emit all remove diffs in one batch
				if let Some(diff) = emit_remove_joined_columns_batch(
					txn,
					pre,
					indices,
					JoinSide::Left,
					&state.right,
					key_hash,
					operator,
					&operator.right_parent,
				)? {
					result.push(diff);
				} else {
					// No joined rows to remove - remove unmatched left rows
					let unmatched = operator.unmatched_left_columns_batch(txn, pre, indices)?;
					result.push(FlowDiff::Remove {
						pre: unmatched,
					});
				}

				// Then remove all rows from state
				for &idx in indices {
					let row_number = pre.row_numbers[idx];
					remove_from_state_entry(txn, &mut state.left, key_hash, row_number)?;
				}
			}
			JoinSide::Right => {
				// First emit all remove diffs in one batch
				if let Some(diff) = emit_remove_joined_columns_batch(
					txn,
					pre,
					indices,
					JoinSide::Right,
					&state.left,
					key_hash,
					operator,
					&operator.left_parent,
				)? {
					result.push(diff);
				}

				// Check if this will make right entries empty
				let will_become_empty = if let Some(entry) = state.right.get(txn, key_hash)? {
					entry.rows.len() <= indices.len()
				} else {
					false
				};

				// Remove all rows from state
				for &idx in indices {
					let row_number = pre.row_numbers[idx];
					remove_from_state_entry(txn, &mut state.right, key_hash, row_number)?;
				}

				// If right side became empty, re-emit left rows as unmatched
				if will_become_empty && !state.right.contains_key(txn, key_hash)? {
					let left_columns =
						pull_left_columns(txn, &state.left, key_hash, &operator.left_parent)?;
					if !left_columns.is_empty() {
						let left_indices: Vec<usize> = (0..left_columns.row_count()).collect();
						let unmatched = operator.unmatched_left_columns_batch(
							txn,
							&left_columns,
							&left_indices,
						)?;
						result.push(FlowDiff::Insert {
							post: unmatched,
						});
					}
				}
			}
		}

		Ok(result)
	}

	/// Handle update for rows with defined join keys (batched by key)
	pub(crate) fn handle_update(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		post: &Columns,
		indices: &[usize],
		side: JoinSide,
		old_key: &Hash128,
		new_key: &Hash128,
		state: &mut JoinState,
		operator: &JoinOperator,
		version: CommitVersion,
	) -> reifydb_type::Result<Vec<FlowDiff>> {
		if indices.is_empty() {
			return Ok(Vec::new());
		}

		let mut result = Vec::new();

		if old_key == new_key {
			// Key didn't change, update in place
			for &row_idx in indices {
				let old_row_number = pre.row_numbers[row_idx];
				let new_row_number = post.row_numbers[row_idx];

				match side {
					JoinSide::Left => {
						// Update the row number in state
						if update_row_in_entry(
							txn,
							&mut state.left,
							old_key,
							old_row_number,
							new_row_number,
						)? {
							// Emit updates for all joined rows
							if let Some(diff) = emit_update_joined_columns(
								txn,
								pre,
								post,
								row_idx,
								JoinSide::Left,
								&state.right,
								old_key,
								operator,
								&operator.right_parent,
							)? {
								result.push(diff);
							} else {
								// No matching right rows - update unmatched left row
								let unmatched_pre = operator
									.unmatched_left_columns(txn, pre, row_idx)?;
								let unmatched_post = operator
									.unmatched_left_columns(txn, post, row_idx)?;
								result.push(FlowDiff::Update {
									pre: unmatched_pre,
									post: unmatched_post,
								});
							}
						}
					}
					JoinSide::Right => {
						// Update the row number in state
						if update_row_in_entry(
							txn,
							&mut state.right,
							old_key,
							old_row_number,
							new_row_number,
						)? {
							// Emit updates for all joined rows
							if let Some(diff) = emit_update_joined_columns(
								txn,
								pre,
								post,
								row_idx,
								JoinSide::Right,
								&state.left,
								old_key,
								operator,
								&operator.left_parent,
							)? {
								result.push(diff);
							}
						}
					}
				}
			}
		} else {
			// Key changed - treat as remove + insert
			let remove_diffs =
				self.handle_remove(txn, pre, indices, side, old_key, state, operator, version)?;
			result.extend(remove_diffs);

			let insert_diffs = self.handle_insert(txn, post, indices, side, new_key, state, operator)?;
			result.extend(insert_diffs);
		}

		Ok(result)
	}
}
