// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::change::Diff, value::column::columns::Columns};
use reifydb_flow::transaction::FlowTransaction;
use reifydb_value::{Result, util::hash::Hash128};

use super::{
	JoinContext, UpdateKeys,
	hash::{
		JoinEmitContext, add_to_state_entry_batch, emit_joined_columns_batch, emit_remove_joined_columns_batch,
		emit_update_joined_columns, for_each_left_block, is_first_right_row, remove_from_state_entry,
		update_row_in_entry,
	},
};
use crate::operator::join::{
	Identity,
	snapshot::{SnapshotJoinContext, publish_joined, resync_joined, retire_right, withdraw_joined},
	state::JoinSide,
};

pub(crate) struct LeftHashJoin;

impl LeftHashJoin {
	pub(crate) fn handle_insert_undefined(
		&self,
		txn: &mut FlowTransaction,
		post: &Columns,
		row_idx: usize,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match ctx.side {
			JoinSide::Left => {
				Ok(ctx.operator.unmatched_left_columns(txn, post, row_idx, Identity::Mint)?.published())
			}
			JoinSide::Right => Ok(Vec::new()),
		}
	}

	pub(crate) fn handle_remove_undefined(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		row_idx: usize,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		let row_number = pre.row_numbers()[row_idx];

		match ctx.side {
			JoinSide::Left => {
				let unmatched =
					ctx.operator.unmatched_left_columns(txn, pre, row_idx, Identity::Consume)?;
				ctx.operator.cleanup_left_row_joins(txn, *row_number)?;
				Ok(unmatched.withdrawn().into_iter().collect())
			}
			JoinSide::Right => Ok(Vec::new()),
		}
	}

	pub(crate) fn handle_update_both_undefined(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		post: &Columns,
		row_idx: usize,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match ctx.side {
			JoinSide::Left => {
				let unmatched_pre =
					ctx.operator.unmatched_left_columns(txn, pre, row_idx, Identity::Existing)?;
				if unmatched_pre.is_empty() {
					return Ok(Vec::new());
				}
				let unmatched_post =
					ctx.operator.unmatched_left_columns(txn, post, row_idx, Identity::Existing)?;
				Ok(vec![Diff::update(unmatched_pre.existing, unmatched_post.existing)])
			}
			JoinSide::Right => Ok(Vec::new()),
		}
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
		match ctx.side {
			JoinSide::Left => self.handle_insert_left(txn, post, indices, key_hash, ctx),
			JoinSide::Right => self.handle_insert_right(txn, post, indices, key_hash, ctx),
		}
	}

	#[inline]
	fn handle_insert_left(
		&self,
		txn: &mut FlowTransaction,
		post: &Columns,
		indices: &[usize],
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		add_to_state_entry_batch(txn, &mut ctx.state.left, key_hash, post, indices)?;

		if ctx.operator.snapshot {
			let ledger = ctx.operator.snapshot_ledger();
			let snapshot_ctx = SnapshotJoinContext {
				ledger: &ledger,
				operator: ctx.operator,
				right_store: &ctx.state.right,
			};
			return publish_joined(txn, &snapshot_ctx, key_hash, post, indices, true);
		}

		let emit_ctx = JoinEmitContext {
			opposite_store: &ctx.state.right,
			key_hash,
			operator: ctx.operator,
		};
		let joined = emit_joined_columns_batch(txn, post, indices, JoinSide::Left, &emit_ctx)?;
		if !joined.is_empty() {
			return Ok(joined);
		}
		Ok(ctx.operator.unmatched_left_columns_batch(txn, post, indices, Identity::Mint)?.published())
	}

	#[inline]
	fn handle_insert_right(
		&self,
		txn: &mut FlowTransaction,
		post: &Columns,
		indices: &[usize],
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		let is_first = is_first_right_row(txn, &ctx.state.right, key_hash)?;

		let mut result = Vec::new();
		add_to_state_entry_batch(txn, &mut ctx.state.right, key_hash, post, indices)?;

		if ctx.operator.snapshot {
			return Ok(result);
		}

		if is_first && ctx.state.left.contains_key(txn, key_hash)? {
			let operator = ctx.operator;
			for_each_left_block(txn, &ctx.state.left, key_hash, |txn, left_columns| {
				let left_indices: Vec<usize> = (0..left_columns.row_count()).collect();
				let unmatched = operator.unmatched_left_columns_batch(
					txn,
					left_columns,
					&left_indices,
					Identity::Consume,
				)?;
				result.extend(unmatched.withdrawn());
				Ok(())
			})?;
		}

		let emit_ctx = JoinEmitContext {
			opposite_store: &ctx.state.left,
			key_hash,
			operator: ctx.operator,
		};

		result.extend(emit_joined_columns_batch(txn, post, indices, JoinSide::Right, &emit_ctx)?);
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
		match ctx.side {
			JoinSide::Left => self.handle_remove_left(txn, pre, indices, key_hash, ctx),
			JoinSide::Right => self.handle_remove_right(txn, pre, indices, key_hash, ctx),
		}
	}

	#[inline]
	fn handle_remove_left(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		indices: &[usize],
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		let result = if ctx.operator.snapshot {
			let ledger = ctx.operator.snapshot_ledger();
			let snapshot_ctx = SnapshotJoinContext {
				ledger: &ledger,
				operator: ctx.operator,
				right_store: &ctx.state.right,
			};
			let mut withdrawn = Vec::new();
			for &idx in indices {
				withdrawn.extend(withdraw_joined(txn, &snapshot_ctx, key_hash, pre, idx)?);
			}
			withdrawn
		} else {
			let emit_ctx = JoinEmitContext {
				opposite_store: &ctx.state.right,
				key_hash,
				operator: ctx.operator,
			};
			let mut emitted =
				emit_remove_joined_columns_batch(txn, pre, indices, JoinSide::Left, &emit_ctx)?;
			if emitted.is_empty() {
				let unmatched = ctx.operator.unmatched_left_columns_batch(
					txn,
					pre,
					indices,
					Identity::Consume,
				)?;
				emitted.extend(unmatched.withdrawn());
			}
			emitted
		};

		for &idx in indices {
			let row_number = pre.row_numbers()[idx];
			ctx.operator.cleanup_left_row_joins(txn, *row_number)?;
			remove_from_state_entry(txn, &mut ctx.state.left, key_hash, row_number)?;
		}
		Ok(result)
	}

	#[inline]
	fn handle_remove_right(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		indices: &[usize],
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		let mut result = Vec::new();

		if !ctx.operator.snapshot {
			let emit_ctx = JoinEmitContext {
				opposite_store: &ctx.state.left,
				key_hash,
				operator: ctx.operator,
			};

			result.extend(emit_remove_joined_columns_batch(txn, pre, indices, JoinSide::Right, &emit_ctx)?);
		}

		if ctx.operator.snapshot {
			let ledger = ctx.operator.snapshot_ledger();
			let snapshot_ctx = SnapshotJoinContext {
				ledger: &ledger,
				operator: ctx.operator,
				right_store: &ctx.state.right,
			};
			for &idx in indices {
				retire_right(txn, &snapshot_ctx, key_hash, pre.row_numbers()[idx])?;
			}
		}

		for &idx in indices {
			let row_number = pre.row_numbers()[idx];
			remove_from_state_entry(txn, &mut ctx.state.right, key_hash, row_number)?;
		}

		if !ctx.operator.snapshot && !ctx.state.right.contains_key(txn, key_hash)? {
			let operator = ctx.operator;
			for_each_left_block(txn, &ctx.state.left, key_hash, |txn, left_columns| {
				let left_indices: Vec<usize> = (0..left_columns.row_count()).collect();
				let unmatched = operator.unmatched_left_columns_batch(
					txn,
					left_columns,
					&left_indices,
					Identity::Mint,
				)?;
				result.extend(unmatched.published());
				Ok(())
			})?;
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
			let diffs = match ctx.side {
				JoinSide::Left => self.update_in_place_left(txn, pre, post, row_idx, keys, ctx)?,
				JoinSide::Right => self.update_in_place_right(txn, pre, post, row_idx, keys, ctx)?,
			};
			result.extend(diffs);
		}
		Ok(result)
	}

	#[inline]
	fn update_in_place_left(
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
			let resynced = resync_joined(txn, &snapshot_ctx, keys, pre, post, row_idx, true)?;
			if !update_row_in_entry(txn, &mut ctx.state.left, keys.pre, pre_row_number, post, row_idx)? {
				return self.handle_insert(txn, post, &[row_idx], keys.post, ctx);
			}
			return Ok(resynced);
		}

		if !update_row_in_entry(txn, &mut ctx.state.left, keys.pre, pre_row_number, post, row_idx)? {
			return self.handle_insert(txn, post, &[row_idx], keys.post, ctx);
		}

		let emit_ctx = JoinEmitContext {
			opposite_store: &ctx.state.right,
			key_hash: keys.pre,
			operator: ctx.operator,
		};

		let joined = emit_update_joined_columns(txn, pre, post, row_idx, JoinSide::Left, &emit_ctx)?;
		if !joined.is_empty() {
			return Ok(joined);
		}
		let unmatched_pre = ctx.operator.unmatched_left_columns(txn, pre, row_idx, Identity::Existing)?;
		if unmatched_pre.is_empty() {
			return Ok(Vec::new());
		}
		let unmatched_post = ctx.operator.unmatched_left_columns(txn, post, row_idx, Identity::Existing)?;
		Ok(vec![Diff::update(unmatched_pre.existing, unmatched_post.existing)])
	}

	#[inline]
	fn update_in_place_right(
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
			retire_right(txn, &snapshot_ctx, keys.pre, pre_row_number)?;
		}

		if !update_row_in_entry(txn, &mut ctx.state.right, keys.pre, pre_row_number, post, row_idx)? {
			return self.handle_insert(txn, post, &[row_idx], keys.post, ctx);
		}

		if ctx.operator.snapshot {
			return Ok(Vec::new());
		}

		let emit_ctx = JoinEmitContext {
			opposite_store: &ctx.state.left,
			key_hash: keys.pre,
			operator: ctx.operator,
		};

		emit_update_joined_columns(txn, pre, post, row_idx, JoinSide::Right, &emit_ctx)
	}
}
