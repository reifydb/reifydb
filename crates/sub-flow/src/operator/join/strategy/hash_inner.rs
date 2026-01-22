// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{common::CommitVersion, value::column::columns::Columns};
use reifydb_runtime::hash::Hash128;
use reifydb_sdk::flow::FlowDiff;

use super::hash::{
	add_to_state_entry_batch, emit_joined_columns_batch, emit_remove_joined_columns_batch,
	emit_update_joined_columns, remove_from_state_entry, update_row_in_entry,
};
use crate::{
	operator::join::{
		operator::JoinOperator,
		state::{JoinSide, JoinState},
	},
	transaction::FlowTransaction,
};

pub(crate) struct InnerHashJoin;

impl InnerHashJoin {
	/// Handle insert for rows with undefined join keys (no output for inner join)
	pub(crate) fn handle_insert_undefined(
		&self,
		_txn: &mut FlowTransaction,
		_post: &Columns,
		_row_idx: usize,
		_side: JoinSide,
		_state: &mut JoinState,
		_operator: &JoinOperator,
	) -> reifydb_type::Result<Vec<FlowDiff>> {
		// Undefined keys produce no output in inner join
		Ok(Vec::new())
	}

	/// Handle remove for rows with undefined join keys (no output for inner join)
	pub(crate) fn handle_remove_undefined(
		&self,
		_txn: &mut FlowTransaction,
		_pre: &Columns,
		_row_idx: usize,
		_side: JoinSide,
		_state: &mut JoinState,
		_operator: &JoinOperator,
		_version: CommitVersion,
	) -> reifydb_type::Result<Vec<FlowDiff>> {
		// Undefined keys produce no output in inner join
		Ok(Vec::new())
	}

	/// Handle update for rows with undefined join keys (no output for inner join)
	pub(crate) fn handle_update_undefined(
		&self,
		_txn: &mut FlowTransaction,
		_pre: &Columns,
		_post: &Columns,
		_row_idx: usize,
		_side: JoinSide,
		_state: &mut JoinState,
		_operator: &JoinOperator,
		_version: CommitVersion,
	) -> reifydb_type::Result<Vec<FlowDiff>> {
		// Undefined keys produce no output in inner join
		Ok(Vec::new())
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

		// Add all rows to state first
		match side {
			JoinSide::Left => {
				add_to_state_entry_batch(txn, &mut state.left, key_hash, post, indices)?;
			}
			JoinSide::Right => {
				add_to_state_entry_batch(txn, &mut state.right, key_hash, post, indices)?;
			}
		}

		// Then emit all joined rows in one batch
		let opposite_store = match side {
			JoinSide::Left => &state.right,
			JoinSide::Right => &state.left,
		};
		let opposite_parent = match side {
			JoinSide::Left => &operator.right_parent,
			JoinSide::Right => &operator.left_parent,
		};

		if let Some(diff) = emit_joined_columns_batch(
			txn,
			post,
			indices,
			side,
			opposite_store,
			key_hash,
			operator,
			opposite_parent,
		)? {
			result.push(diff);
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

		// Clean up row number mappings for left rows
		if matches!(side, JoinSide::Left) {
			for &idx in indices {
				let row_number = pre.row_numbers[idx];
				operator.cleanup_left_row_joins(txn, *row_number)?;
			}
		}

		// First emit all remove diffs in one batch
		let opposite_store = match side {
			JoinSide::Left => &state.right,
			JoinSide::Right => &state.left,
		};
		let opposite_parent = match side {
			JoinSide::Left => &operator.right_parent,
			JoinSide::Right => &operator.left_parent,
		};

		if let Some(diff) = emit_remove_joined_columns_batch(
			txn,
			pre,
			indices,
			side,
			opposite_store,
			key_hash,
			operator,
			opposite_parent,
		)? {
			result.push(diff);
		}

		// Then remove all rows from state
		for &idx in indices {
			let row_number = pre.row_numbers[idx];
			match side {
				JoinSide::Left => {
					remove_from_state_entry(txn, &mut state.left, key_hash, row_number)?;
				}
				JoinSide::Right => {
					remove_from_state_entry(txn, &mut state.right, key_hash, row_number)?;
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
							// Emit updates for all joined rows (only if right rows exist)
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
							// Emit updates for all joined rows (only if left rows exist)
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
