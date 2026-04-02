// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::change::Diff, value::column::columns::Columns};
use reifydb_runtime::hash::Hash128;
use reifydb_type::Result;

use super::{
	JoinContext, UpdateKeys,
	hash::{
		JoinEmitContext, add_to_state_entry_batch, emit_joined_columns_batch, emit_remove_joined_columns_batch,
		emit_update_joined_columns, remove_from_state_entry, update_row_in_entry,
	},
};
use crate::{operator::join::state::JoinSide, transaction::FlowTransaction};

pub(crate) struct InnerHashJoin;

impl InnerHashJoin {
	/// Handle insert for rows with undefined join keys (no output for inner join)
	pub(crate) fn handle_insert_undefined(
		&self,
		_txn: &mut FlowTransaction,
		_post: &Columns,
		_row_idx: usize,
		_ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		// Undefined keys produce no output in inner join
		Ok(Vec::new())
	}

	/// Handle remove for rows with undefined join keys (no output for inner join)
	pub(crate) fn handle_remove_undefined(
		&self,
		_txn: &mut FlowTransaction,
		_pre: &Columns,
		_row_idx: usize,
		_ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
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
		_ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		// Undefined keys produce no output in inner join
		Ok(Vec::new())
	}

	/// Handle insert for rows with defined join keys (batched by key)
	pub(crate) fn handle_insert(
		&self,
		txn: &mut FlowTransaction,
		post: &Columns,
		indices: &[usize],
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		if indices.is_empty() {
			return Ok(Vec::new());
		}

		let mut result = Vec::new();

		// Add all rows to state first
		match ctx.side {
			JoinSide::Left => {
				add_to_state_entry_batch(txn, &mut ctx.state.left, key_hash, post, indices)?;
			}
			JoinSide::Right => {
				add_to_state_entry_batch(txn, &mut ctx.state.right, key_hash, post, indices)?;
			}
		}

		// Then emit all joined rows in one batch
		let emit_ctx = JoinEmitContext {
			opposite_store: match ctx.side {
				JoinSide::Left => &ctx.state.right,
				JoinSide::Right => &ctx.state.left,
			},
			key_hash,
			operator: ctx.operator,
			opposite_parent: match ctx.side {
				JoinSide::Left => &ctx.operator.right_parent,
				JoinSide::Right => &ctx.operator.left_parent,
			},
		};

		if let Some(diff) = emit_joined_columns_batch(txn, post, indices, ctx.side, &emit_ctx)? {
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
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		if indices.is_empty() {
			return Ok(Vec::new());
		}

		let mut result = Vec::new();

		// Clean up row number mappings for left rows
		if matches!(ctx.side, JoinSide::Left) {
			for &idx in indices {
				let row_number = pre.row_numbers[idx];
				ctx.operator.cleanup_left_row_joins(txn, *row_number)?;
			}
		}

		// First emit all remove diffs in one batch
		let emit_ctx = JoinEmitContext {
			opposite_store: match ctx.side {
				JoinSide::Left => &ctx.state.right,
				JoinSide::Right => &ctx.state.left,
			},
			key_hash,
			operator: ctx.operator,
			opposite_parent: match ctx.side {
				JoinSide::Left => &ctx.operator.right_parent,
				JoinSide::Right => &ctx.operator.left_parent,
			},
		};

		if let Some(diff) = emit_remove_joined_columns_batch(txn, pre, indices, ctx.side, &emit_ctx)? {
			result.push(diff);
		}

		// Then remove all rows from state
		for &idx in indices {
			let row_number = pre.row_numbers[idx];
			match ctx.side {
				JoinSide::Left => {
					remove_from_state_entry(txn, &mut ctx.state.left, key_hash, row_number)?;
				}
				JoinSide::Right => {
					remove_from_state_entry(txn, &mut ctx.state.right, key_hash, row_number)?;
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
		keys: UpdateKeys,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		if indices.is_empty() {
			return Ok(Vec::new());
		}

		let mut result = Vec::new();

		if keys.pre == keys.post {
			// Key didn't change, update in place
			for &row_idx in indices {
				let pre_row_number = pre.row_numbers[row_idx];
				let post_row_number = post.row_numbers[row_idx];

				match ctx.side {
					JoinSide::Left => {
						// Update the row number in state
						if update_row_in_entry(
							txn,
							&mut ctx.state.left,
							keys.pre,
							pre_row_number,
							post_row_number,
						)? {
							let emit_ctx = JoinEmitContext {
								opposite_store: &ctx.state.right,
								key_hash: keys.pre,
								operator: ctx.operator,
								opposite_parent: &ctx.operator.right_parent,
							};
							// Emit updates for all joined rows (only if right rows exist)
							if let Some(diff) = emit_update_joined_columns(
								txn,
								pre,
								post,
								row_idx,
								JoinSide::Left,
								&emit_ctx,
							)? {
								result.push(diff);
							}
						}
					}
					JoinSide::Right => {
						// Update the row number in state
						if update_row_in_entry(
							txn,
							&mut ctx.state.right,
							keys.pre,
							pre_row_number,
							post_row_number,
						)? {
							let emit_ctx = JoinEmitContext {
								opposite_store: &ctx.state.left,
								key_hash: keys.pre,
								operator: ctx.operator,
								opposite_parent: &ctx.operator.left_parent,
							};
							// Emit updates for all joined rows (only if left rows exist)
							if let Some(diff) = emit_update_joined_columns(
								txn,
								pre,
								post,
								row_idx,
								JoinSide::Right,
								&emit_ctx,
							)? {
								result.push(diff);
							}
						}
					}
				}
			}
		} else {
			// Key changed - treat as remove + insert
			let remove_diffs = self.handle_remove(txn, pre, indices, keys.pre, ctx)?;
			result.extend(remove_diffs);

			let insert_diffs = self.handle_insert(txn, post, indices, keys.post, ctx)?;
			result.extend(insert_diffs);
		}

		Ok(result)
	}
}
