// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::change::Diff, value::column::columns::Columns};
use reifydb_runtime::hash::Hash128;
use reifydb_type::Result;

use super::{
	JoinContext, UpdateKeys,
	hash::{
		JoinEmitContext, add_to_state_entry_batch, emit_joined_columns_batch, emit_remove_joined_columns_batch,
		emit_update_joined_columns, is_first_right_row, pull_left_columns, remove_from_state_entry,
		update_row_in_entry,
	},
};
use crate::{operator::join::state::JoinSide, transaction::FlowTransaction};

pub(crate) struct LeftHashJoin;

impl LeftHashJoin {
	/// Handle insert for rows with undefined join keys (emits unmatched left row)
	pub(crate) fn handle_insert_undefined(
		&self,
		txn: &mut FlowTransaction,
		post: &Columns,
		row_idx: usize,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match ctx.side {
			JoinSide::Left => {
				// Undefined key in left join still emits the row
				let unmatched = ctx.operator.unmatched_left_columns(txn, post, row_idx)?;
				Ok(vec![Diff::Insert {
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
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		let row_number = pre.row_numbers[row_idx];

		match ctx.side {
			JoinSide::Left => {
				// Undefined key - remove the unmatched row
				let unmatched = ctx.operator.unmatched_left_columns(txn, pre, row_idx)?;
				ctx.operator.cleanup_left_row_joins(txn, *row_number)?;
				Ok(vec![Diff::Remove {
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
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match ctx.side {
			JoinSide::Left => {
				// Both keys are undefined - update the row
				let unmatched_pre = ctx.operator.unmatched_left_columns(txn, pre, row_idx)?;
				let unmatched_post = ctx.operator.unmatched_left_columns(txn, post, row_idx)?;
				Ok(vec![Diff::Update {
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
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		if indices.is_empty() {
			return Ok(Vec::new());
		}

		let mut result = Vec::new();

		match ctx.side {
			JoinSide::Left => {
				// Add all rows to state first
				add_to_state_entry_batch(txn, &mut ctx.state.left, key_hash, post, indices)?;

				let emit_ctx = JoinEmitContext {
					opposite_store: &ctx.state.right,
					key_hash,
					operator: ctx.operator,
					opposite_parent: &ctx.operator.right_parent,
				};

				// Check if there are matching right rows
				if let Some(diff) =
					emit_joined_columns_batch(txn, post, indices, JoinSide::Left, &emit_ctx)?
				{
					result.push(diff);
				} else {
					// No matches - emit unmatched left rows for all
					let unmatched =
						ctx.operator.unmatched_left_columns_batch(txn, post, indices)?;
					result.push(Diff::Insert {
						post: unmatched,
					});
				}
			}
			JoinSide::Right => {
				let is_first = is_first_right_row(txn, &ctx.state.right, key_hash)?;

				// Add all rows to state first
				add_to_state_entry_batch(txn, &mut ctx.state.right, key_hash, post, indices)?;

				// If first right row(s), remove previously emitted unmatched left rows
				if is_first && let Some(left_entry) = ctx.state.left.get(txn, key_hash)? {
					let left_columns = ctx.operator.left_parent.pull(txn, &left_entry.rows)?;
					if !left_columns.is_empty() {
						let left_indices: Vec<usize> = (0..left_columns.row_count()).collect();
						let unmatched = ctx.operator.unmatched_left_columns_batch(
							txn,
							&left_columns,
							&left_indices,
						)?;
						result.push(Diff::Remove {
							pre: unmatched,
						});
					}
				}

				let emit_ctx = JoinEmitContext {
					opposite_store: &ctx.state.left,
					key_hash,
					operator: ctx.operator,
					opposite_parent: &ctx.operator.left_parent,
				};

				// Emit all joined rows in one batch
				if let Some(diff) =
					emit_joined_columns_batch(txn, post, indices, JoinSide::Right, &emit_ctx)?
				{
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
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		if indices.is_empty() {
			return Ok(Vec::new());
		}

		let mut result = Vec::new();

		match ctx.side {
			JoinSide::Left => {
				// Clean up row number mappings for all left rows
				for &idx in indices {
					let row_number = pre.row_numbers[idx];
					ctx.operator.cleanup_left_row_joins(txn, *row_number)?;
				}

				let emit_ctx = JoinEmitContext {
					opposite_store: &ctx.state.right,
					key_hash,
					operator: ctx.operator,
					opposite_parent: &ctx.operator.right_parent,
				};

				// First emit all remove diffs in one batch
				if let Some(diff) =
					emit_remove_joined_columns_batch(txn, pre, indices, JoinSide::Left, &emit_ctx)?
				{
					result.push(diff);
				} else {
					// No joined rows to remove - remove unmatched left rows
					let unmatched = ctx.operator.unmatched_left_columns_batch(txn, pre, indices)?;
					result.push(Diff::Remove {
						pre: unmatched,
					});
				}

				// Then remove all rows from state
				for &idx in indices {
					let row_number = pre.row_numbers[idx];
					remove_from_state_entry(txn, &mut ctx.state.left, key_hash, row_number)?;
				}
			}
			JoinSide::Right => {
				let emit_ctx = JoinEmitContext {
					opposite_store: &ctx.state.left,
					key_hash,
					operator: ctx.operator,
					opposite_parent: &ctx.operator.left_parent,
				};

				// First emit all remove diffs in one batch
				if let Some(diff) =
					emit_remove_joined_columns_batch(txn, pre, indices, JoinSide::Right, &emit_ctx)?
				{
					result.push(diff);
				}

				// Check if this will make right entries empty
				let will_become_empty = if let Some(entry) = ctx.state.right.get(txn, key_hash)? {
					entry.rows.len() <= indices.len()
				} else {
					false
				};

				// Remove all rows from state
				for &idx in indices {
					let row_number = pre.row_numbers[idx];
					remove_from_state_entry(txn, &mut ctx.state.right, key_hash, row_number)?;
				}

				// If right side became empty, re-emit left rows as unmatched
				if will_become_empty && !ctx.state.right.contains_key(txn, key_hash)? {
					let left_columns = pull_left_columns(
						txn,
						&ctx.state.left,
						key_hash,
						&ctx.operator.left_parent,
					)?;
					if !left_columns.is_empty() {
						let left_indices: Vec<usize> = (0..left_columns.row_count()).collect();
						let unmatched = ctx.operator.unmatched_left_columns_batch(
							txn,
							&left_columns,
							&left_indices,
						)?;
						result.push(Diff::Insert {
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

							// Emit updates for all joined rows
							if let Some(diff) = emit_update_joined_columns(
								txn,
								pre,
								post,
								row_idx,
								JoinSide::Left,
								&emit_ctx,
							)? {
								result.push(diff);
							} else {
								// No matching right rows - update unmatched left row
								let unmatched_pre = ctx
									.operator
									.unmatched_left_columns(txn, pre, row_idx)?;
								let unmatched_post = ctx
									.operator
									.unmatched_left_columns(txn, post, row_idx)?;
								result.push(Diff::Update {
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

							// Emit updates for all joined rows
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
