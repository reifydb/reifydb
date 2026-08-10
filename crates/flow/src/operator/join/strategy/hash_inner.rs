// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::change::Diff, value::column::columns::Columns};
use reifydb_value::{Result, util::hash::Hash128};

use super::{
	JoinContext, UpdateKeys,
	hash::{
		JoinEmitContext, add_to_state_entry_batch, emit_joined_columns_batch, emit_remove_joined_columns_batch,
		emit_update_joined_columns, update_single_row_in_entry,
	},
};
use crate::{
	operator::join::{
		snapshot::{SnapshotJoinContext, publish_joined, resync_joined, retire_right, withdraw_joined},
		state::JoinSide,
	},
	transaction::FlowTransaction,
};

pub(crate) struct InnerHashJoin;

impl InnerHashJoin {
	pub(crate) fn handle_insert_undefined(
		&self,
		_txn: &mut FlowTransaction,
		_post: &Columns,
		_row_idx: usize,
		_ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		Ok(Vec::new())
	}

	pub(crate) fn handle_remove_undefined(
		&self,
		_txn: &mut FlowTransaction,
		_pre: &Columns,
		_row_idx: usize,
		_ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		Ok(Vec::new())
	}

	pub(crate) fn handle_update_both_undefined(
		&self,
		_txn: &mut FlowTransaction,
		_pre: &Columns,
		_post: &Columns,
		_row_idx: usize,
		_ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		Ok(Vec::new())
	}

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
				add_to_state_entry_batch(txn, &mut ctx.state.left, key_hash, post, indices)?;
			}
			JoinSide::Right => {
				add_to_state_entry_batch(txn, &mut ctx.state.right, key_hash, post, indices)?;
			}
		}

		if ctx.operator.snapshot && matches!(ctx.side, JoinSide::Right) {
			return Ok(result);
		}

		if ctx.operator.snapshot {
			let ledger = ctx.operator.snapshot_ledger();
			let snapshot_ctx = SnapshotJoinContext {
				ledger: &ledger,
				operator: ctx.operator,
				right_store: &ctx.state.right,
			};
			result.extend(publish_joined(txn, &snapshot_ctx, key_hash, post, indices, false)?);
			return Ok(result);
		}

		let emit_ctx = JoinEmitContext {
			opposite_store: match ctx.side {
				JoinSide::Left => &ctx.state.right,
				JoinSide::Right => &ctx.state.left,
			},
			key_hash,
			operator: ctx.operator,
		};

		result.extend(emit_joined_columns_batch(txn, post, indices, ctx.side, &emit_ctx)?);

		Ok(result)
	}

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

		if ctx.operator.snapshot {
			let ledger = ctx.operator.snapshot_ledger();
			let snapshot_ctx = SnapshotJoinContext {
				ledger: &ledger,
				operator: ctx.operator,
				right_store: &ctx.state.right,
			};
			match ctx.side {
				JoinSide::Left => {
					for &idx in indices {
						result.extend(withdraw_joined(txn, &snapshot_ctx, key_hash, pre, idx)?);
					}
				}
				JoinSide::Right => {
					for &idx in indices {
						retire_right(txn, &snapshot_ctx, key_hash, pre.row_numbers()[idx])?;
					}
				}
			}
		}

		let snapshot_right = ctx.operator.snapshot;

		if !snapshot_right {
			let emit_ctx = JoinEmitContext {
				opposite_store: match ctx.side {
					JoinSide::Left => &ctx.state.right,
					JoinSide::Right => &ctx.state.left,
				},
				key_hash,
				operator: ctx.operator,
			};

			result.extend(emit_remove_joined_columns_batch(txn, pre, indices, ctx.side, &emit_ctx)?);
		}

		let group = match ctx.side {
			JoinSide::Left => ctx.state.left.group_of(txn, key_hash)?,
			JoinSide::Right => ctx.state.right.group_of(txn, key_hash)?,
		};
		for &idx in indices {
			let row_number = pre.row_numbers()[idx];

			if matches!(ctx.side, JoinSide::Left) {
				ctx.operator.cleanup_left_row_joins(txn, *row_number)?;
			}

			if let Some(group) = group {
				match ctx.side {
					JoinSide::Left => {
						ctx.state.left.remove_row_in(txn, group, row_number)?;
					}
					JoinSide::Right => {
						ctx.state.right.remove_row_in(txn, group, row_number)?;
					}
				}
			}
		}

		Ok(result)
	}

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

		if keys.pre != keys.post {
			let mut result = self.handle_remove(txn, pre, indices, keys.pre, ctx)?;
			result.extend(self.handle_insert(txn, post, indices, keys.post, ctx)?);
			return Ok(result);
		}

		let mut result = Vec::new();
		for &row_idx in indices {
			result.extend(self.update_in_place_one_row(txn, pre, post, row_idx, keys, ctx)?);
		}
		Ok(result)
	}

	#[inline]
	fn update_in_place_one_row(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		post: &Columns,
		row_idx: usize,
		keys: UpdateKeys,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		let pre_row_number = pre.row_numbers()[row_idx];

		if ctx.operator.snapshot {
			let ledger = ctx.operator.snapshot_ledger();
			let snapshot_ctx = SnapshotJoinContext {
				ledger: &ledger,
				operator: ctx.operator,
				right_store: &ctx.state.right,
			};
			return match ctx.side {
				JoinSide::Left => {
					let diffs = resync_joined(txn, &snapshot_ctx, keys, pre, post, row_idx, false)?;
					update_single_row_in_entry(
						txn,
						&ctx.state.left,
						keys.pre,
						pre_row_number,
						post,
						row_idx,
					)?;
					Ok(diffs)
				}
				JoinSide::Right => {
					retire_right(txn, &snapshot_ctx, keys.pre, pre_row_number)?;
					update_single_row_in_entry(
						txn,
						&ctx.state.right,
						keys.pre,
						pre_row_number,
						post,
						row_idx,
					)?;
					Ok(Vec::new())
				}
			};
		}

		let updated = match ctx.side {
			JoinSide::Left => update_single_row_in_entry(
				txn,
				&ctx.state.left,
				keys.pre,
				pre_row_number,
				post,
				row_idx,
			)?,
			JoinSide::Right => update_single_row_in_entry(
				txn,
				&ctx.state.right,
				keys.pre,
				pre_row_number,
				post,
				row_idx,
			)?,
		};

		if !updated {
			return self.handle_insert(txn, post, &[row_idx], keys.post, ctx);
		}

		if ctx.operator.snapshot && matches!(ctx.side, JoinSide::Right) {
			return Ok(Vec::new());
		}

		let emit_ctx = JoinEmitContext {
			opposite_store: match ctx.side {
				JoinSide::Left => &ctx.state.right,
				JoinSide::Right => &ctx.state.left,
			},
			key_hash: keys.pre,
			operator: ctx.operator,
		};

		emit_update_joined_columns(txn, pre, post, row_idx, ctx.side, &emit_ctx)
	}
}
