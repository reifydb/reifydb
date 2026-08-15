// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::change::Diff, value::column::columns::Columns};
use reifydb_value::{Result, util::hash::Hash128};

use super::{
	JoinContext, UpdateKeys,
	hash::{
		JoinEmitContext, add_to_state_entry_batch, emit_joined_columns_batch, emit_remove_joined_columns_batch,
		emit_update_joined_columns, for_each_left_block, is_first_right_row, update_single_row_in_entry,
	},
};
use crate::operator::{
	host::HostContext,
	join::{
		Identity,
		snapshot::{SnapshotJoinContext, publish_joined, resync_joined, retire_right, withdraw_joined},
		state::JoinSide,
	},
};

pub(crate) struct LeftHashJoin;

impl LeftHashJoin {
	pub(crate) fn handle_insert_undefined(
		&self,
		host: &mut dyn HostContext,
		post: &Columns,
		row_idx: usize,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match ctx.side {
			JoinSide::Left => Ok(ctx
				.operator
				.unmatched_left_columns(host, post, row_idx, Identity::Mint)?
				.published()),
			JoinSide::Right => Ok(Vec::new()),
		}
	}

	pub(crate) fn handle_remove_undefined(
		&self,
		host: &mut dyn HostContext,
		pre: &Columns,
		row_idx: usize,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		let row_number = pre.row_numbers()[row_idx];

		match ctx.side {
			JoinSide::Left => {
				let unmatched =
					ctx.operator.unmatched_left_columns(host, pre, row_idx, Identity::Consume)?;
				ctx.operator.cleanup_left_row_joins(host, *row_number)?;
				Ok(unmatched.withdrawn().into_iter().collect())
			}
			JoinSide::Right => Ok(Vec::new()),
		}
	}

	pub(crate) fn handle_update_both_undefined(
		&self,
		host: &mut dyn HostContext,
		pre: &Columns,
		post: &Columns,
		row_idx: usize,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match ctx.side {
			JoinSide::Left => {
				let unmatched_pre =
					ctx.operator.unmatched_left_columns(host, pre, row_idx, Identity::Existing)?;
				if unmatched_pre.is_empty() {
					return Ok(Vec::new());
				}
				let unmatched_post =
					ctx.operator.unmatched_left_columns(host, post, row_idx, Identity::Existing)?;
				Ok(vec![Diff::update(unmatched_pre.existing, unmatched_post.existing)])
			}
			JoinSide::Right => Ok(Vec::new()),
		}
	}

	pub(crate) fn handle_insert(
		&self,
		host: &mut dyn HostContext,
		post: &Columns,
		indices: &[usize],
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		if indices.is_empty() {
			return Ok(Vec::new());
		}
		match ctx.side {
			JoinSide::Left => self.handle_insert_left(host, post, indices, key_hash, ctx),
			JoinSide::Right => self.handle_insert_right(host, post, indices, key_hash, ctx),
		}
	}

	#[inline]
	fn handle_insert_left(
		&self,
		host: &mut dyn HostContext,
		post: &Columns,
		indices: &[usize],
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		add_to_state_entry_batch(host, &mut ctx.state.left, key_hash, post, indices)?;

		if ctx.operator.snapshot {
			let ledger = ctx.operator.snapshot_ledger();
			let snapshot_ctx = SnapshotJoinContext {
				ledger: &ledger,
				operator: ctx.operator,
				right_store: &ctx.state.right,
			};
			return publish_joined(host, &snapshot_ctx, key_hash, post, indices, true);
		}

		let emit_ctx = JoinEmitContext {
			opposite_store: &ctx.state.right,
			key_hash,
			operator: ctx.operator,
		};
		let joined = emit_joined_columns_batch(host, post, indices, JoinSide::Left, &emit_ctx)?;
		if !joined.is_empty() {
			return Ok(joined);
		}
		Ok(ctx.operator.unmatched_left_columns_batch(host, post, indices, Identity::Mint)?.published())
	}

	#[inline]
	fn handle_insert_right(
		&self,
		host: &mut dyn HostContext,
		post: &Columns,
		indices: &[usize],
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		let is_first = is_first_right_row(host, &ctx.state.right, key_hash)?;

		let mut result = Vec::new();
		add_to_state_entry_batch(host, &mut ctx.state.right, key_hash, post, indices)?;

		if ctx.operator.snapshot {
			return Ok(result);
		}

		if is_first && ctx.state.left.contains_key(host, key_hash)? {
			let operator = ctx.operator;
			for_each_left_block(host, &ctx.state.left, key_hash, |host, left_columns| {
				let left_indices: Vec<usize> = (0..left_columns.row_count()).collect();
				let unmatched = operator.unmatched_left_columns_batch(
					host,
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

		result.extend(emit_joined_columns_batch(host, post, indices, JoinSide::Right, &emit_ctx)?);
		Ok(result)
	}

	pub(crate) fn handle_remove(
		&self,
		host: &mut dyn HostContext,
		pre: &Columns,
		indices: &[usize],
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		if indices.is_empty() {
			return Ok(Vec::new());
		}
		match ctx.side {
			JoinSide::Left => self.handle_remove_left(host, pre, indices, key_hash, ctx),
			JoinSide::Right => self.handle_remove_right(host, pre, indices, key_hash, ctx),
		}
	}

	#[inline]
	fn handle_remove_left(
		&self,
		host: &mut dyn HostContext,
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
				withdrawn.extend(withdraw_joined(host, &snapshot_ctx, key_hash, pre, idx)?);
			}
			withdrawn
		} else {
			let emit_ctx = JoinEmitContext {
				opposite_store: &ctx.state.right,
				key_hash,
				operator: ctx.operator,
			};
			let mut emitted =
				emit_remove_joined_columns_batch(host, pre, indices, JoinSide::Left, &emit_ctx)?;
			if emitted.is_empty() {
				let unmatched = ctx.operator.unmatched_left_columns_batch(
					host,
					pre,
					indices,
					Identity::Consume,
				)?;
				emitted.extend(unmatched.withdrawn());
			}
			emitted
		};

		let left_group = ctx.state.left.group_of(host, key_hash)?;
		for &idx in indices {
			let row_number = pre.row_numbers()[idx];
			ctx.operator.cleanup_left_row_joins(host, *row_number)?;
			if let Some(group) = left_group {
				ctx.state.left.remove_row_in(host, group, row_number)?;
			}
		}
		Ok(result)
	}

	#[inline]
	fn handle_remove_right(
		&self,
		host: &mut dyn HostContext,
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

			result.extend(emit_remove_joined_columns_batch(
				host,
				pre,
				indices,
				JoinSide::Right,
				&emit_ctx,
			)?);
		}

		if ctx.operator.snapshot {
			let ledger = ctx.operator.snapshot_ledger();
			let snapshot_ctx = SnapshotJoinContext {
				ledger: &ledger,
				operator: ctx.operator,
				right_store: &ctx.state.right,
			};
			for &idx in indices {
				retire_right(host, &snapshot_ctx, key_hash, pre.row_numbers()[idx])?;
			}
		}

		let right_group = ctx.state.right.group_of(host, key_hash)?;
		for &idx in indices {
			let row_number = pre.row_numbers()[idx];
			if let Some(group) = right_group {
				ctx.state.right.remove_row_in(host, group, row_number)?;
			}
		}

		if !ctx.operator.snapshot && !ctx.state.right.contains_key(host, key_hash)? {
			let operator = ctx.operator;
			for_each_left_block(host, &ctx.state.left, key_hash, |host, left_columns| {
				let left_indices: Vec<usize> = (0..left_columns.row_count()).collect();
				let unmatched = operator.unmatched_left_columns_batch(
					host,
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
		host: &mut dyn HostContext,
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
			let mut result = self.handle_remove(host, pre, indices, keys.pre, ctx)?;
			result.extend(self.handle_insert(host, post, indices, keys.post, ctx)?);
			return Ok(result);
		}

		let mut result = Vec::new();
		for &row_idx in indices {
			let diffs = match ctx.side {
				JoinSide::Left => self.update_in_place_left(host, pre, post, row_idx, keys, ctx)?,
				JoinSide::Right => self.update_in_place_right(host, pre, post, row_idx, keys, ctx)?,
			};
			result.extend(diffs);
		}
		Ok(result)
	}

	#[inline]
	fn update_in_place_left(
		&self,
		host: &mut dyn HostContext,
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
			let resynced = resync_joined(host, &snapshot_ctx, keys, pre, post, row_idx, true)?;
			if !update_single_row_in_entry(host, &ctx.state.left, keys.pre, pre_row_number, post, row_idx)?
			{
				if ctx.operator.lateness_of(JoinSide::Left).is_some() {
					return Ok(Vec::new());
				}
				return self.handle_insert(host, post, &[row_idx], keys.post, ctx);
			}
			return Ok(resynced);
		}

		if !update_single_row_in_entry(host, &ctx.state.left, keys.pre, pre_row_number, post, row_idx)? {
			if ctx.operator.lateness_of(JoinSide::Left).is_some() {
				return Ok(Vec::new());
			}
			return self.handle_insert(host, post, &[row_idx], keys.post, ctx);
		}

		let emit_ctx = JoinEmitContext {
			opposite_store: &ctx.state.right,
			key_hash: keys.pre,
			operator: ctx.operator,
		};

		let joined = emit_update_joined_columns(host, pre, post, row_idx, JoinSide::Left, &emit_ctx)?;
		if !joined.is_empty() {
			return Ok(joined);
		}
		let unmatched_pre = ctx.operator.unmatched_left_columns(host, pre, row_idx, Identity::Existing)?;
		if unmatched_pre.is_empty() {
			return Ok(Vec::new());
		}
		let unmatched_post = ctx.operator.unmatched_left_columns(host, post, row_idx, Identity::Existing)?;
		Ok(vec![Diff::update(unmatched_pre.existing, unmatched_post.existing)])
	}

	#[inline]
	fn update_in_place_right(
		&self,
		host: &mut dyn HostContext,
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
			retire_right(host, &snapshot_ctx, keys.pre, pre_row_number)?;
		}

		if !update_single_row_in_entry(host, &ctx.state.right, keys.pre, pre_row_number, post, row_idx)? {
			if ctx.operator.lateness_of(JoinSide::Right).is_some() {
				return Ok(Vec::new());
			}
			return self.handle_insert(host, post, &[row_idx], keys.post, ctx);
		}

		if ctx.operator.snapshot {
			return Ok(Vec::new());
		}

		let emit_ctx = JoinEmitContext {
			opposite_store: &ctx.state.left,
			key_hash: keys.pre,
			operator: ctx.operator,
		};

		emit_update_joined_columns(host, pre, post, row_idx, JoinSide::Right, &emit_ctx)
	}
}
