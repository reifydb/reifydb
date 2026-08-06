// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::change::Diff, value::column::columns::Columns};
use reifydb_flow::transaction::FlowTransaction;
use reifydb_value::{Result, util::hash::Hash128};
use tracing::instrument;

use super::{
	JoinContext, UpdateKeys,
	hash::{
		add_to_state_entry_batch, for_each_left_block, prepare_entry_update,
		update_row_in_entry,
	},
	latest::{overwrite_right_slot, read_right_slot, remove_right_slot},
	latest_inner::{republished_slot, update_diff},
};
use crate::operator::join::{
	snapshot::{SnapshotJoinContext, publish_slot, retire_slot, withdraw_slot},
	state::JoinSide,
};

pub(crate) struct LatestLeftHashJoin;

impl LatestLeftHashJoin {
	pub(crate) fn handle_insert_undefined(
		&self,
		_txn: &mut FlowTransaction,
		post: &Columns,
		row_idx: usize,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match ctx.side {
			JoinSide::Left => Ok(vec![Diff::insert(ctx.operator.unmatched_left_latest(post, &[row_idx]))]),
			JoinSide::Right => Ok(Vec::new()),
		}
	}

	pub(crate) fn handle_remove_undefined(
		&self,
		_txn: &mut FlowTransaction,
		pre: &Columns,
		row_idx: usize,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match ctx.side {
			JoinSide::Left => Ok(vec![Diff::remove(ctx.operator.unmatched_left_latest(pre, &[row_idx]))]),
			JoinSide::Right => Ok(Vec::new()),
		}
	}

	pub(crate) fn handle_update_both_undefined(
		&self,
		_txn: &mut FlowTransaction,
		pre: &Columns,
		post: &Columns,
		row_idx: usize,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match ctx.side {
			JoinSide::Left => {
				let pre_unmatched = ctx.operator.unmatched_left_latest(pre, &[row_idx]);
				let post_unmatched = ctx.operator.unmatched_left_latest(post, &[row_idx]);
				Ok(vec![Diff::update(pre_unmatched, post_unmatched)])
			}
			JoinSide::Right => Ok(Vec::new()),
		}
	}

	#[instrument(name = "flow::operator::join::latest_left::handle_insert", level = "trace", skip_all, fields(rows = indices.len()))]
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
			JoinSide::Left => {
				if ctx.operator.snapshot {
					let ledger = ctx.operator.snapshot_ledger();
					let snapshot_ctx = SnapshotJoinContext {
						ledger: &ledger,
						operator: ctx.operator,
						right_store: &ctx.state.right,
					};
					let published =
						publish_slot(txn, &snapshot_ctx, key_hash, post, indices, true)?;
					return Ok(published
						.map(|columns| vec![Diff::insert(columns)])
						.unwrap_or_default());
				}
				add_to_state_entry_batch(txn, &mut ctx.state.left, key_hash, post, indices)?;
				let joined = match read_right_slot(txn, &ctx.state.right, key_hash)? {
					Some(slot) => ctx.operator.join_left_with_slot(post, indices, &slot),
					None => ctx.operator.unmatched_left_latest(post, indices),
				};
				Ok(vec![Diff::insert(joined)])
			}
			JoinSide::Right => self.handle_right_insert(txn, post, indices, key_hash, ctx),
		}
	}

	#[instrument(name = "flow::operator::join::latest_left::handle_right_insert", level = "trace", skip_all, fields(rows = indices.len()))]
	fn handle_right_insert(
		&self,
		txn: &mut FlowTransaction,
		post: &Columns,
		indices: &[usize],
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		if ctx.operator.snapshot {
			let ledger = ctx.operator.snapshot_ledger();
			let snapshot_ctx = SnapshotJoinContext {
				ledger: &ledger,
				operator: ctx.operator,
				right_store: &ctx.state.right,
			};
			retire_slot(txn, &snapshot_ctx, key_hash)?;
			overwrite_right_slot(txn, &ctx.state.right, key_hash, post, indices)?;
			return Ok(Vec::new());
		}
		let old = read_right_slot(txn, &ctx.state.right, key_hash)?;
		let new = overwrite_right_slot(txn, &ctx.state.right, key_hash, post, indices)?;
		let operator = ctx.operator;
		let mut result = Vec::new();
		for_each_left_block(txn, &ctx.state.left, key_hash, |_txn, left| {
			let left_indices: Vec<usize> = (0..left.row_count()).collect();
			match (&old, &new) {
				(Some(old_slot), Some(new_slot)) => {
					let pre = operator.join_left_with_slot(left, &left_indices, old_slot);
					let post = operator.join_left_with_slot(left, &left_indices, new_slot);
					result.push(Diff::update(pre, post));
				}
				(None, Some(new_slot)) => {
					let pre = operator.unmatched_left_latest(left, &left_indices);
					let post = operator.join_left_with_slot(left, &left_indices, new_slot);
					result.push(Diff::update(pre, post));
				}
				_ => {}
			}
			Ok(())
		})?;
		Ok(result)
	}

	#[instrument(name = "flow::operator::join::latest_left::handle_remove", level = "trace", skip_all)]
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
			JoinSide::Left => {
				if ctx.operator.snapshot {
					let ledger = ctx.operator.snapshot_ledger();
					let snapshot_ctx = SnapshotJoinContext {
						ledger: &ledger,
						operator: ctx.operator,
						right_store: &ctx.state.right,
					};
					let mut withdrawn = Vec::new();
					if let Some(group) = ctx.state.right.group_of(txn, key_hash)? {
						for &idx in indices {
							if let Some(columns) =
								withdraw_slot(txn, &snapshot_ctx, group, pre, idx)?
							{
								withdrawn.push(Diff::remove(columns));
							}
						}
					}
					return Ok(withdrawn);
				}
				let removed = match read_right_slot(txn, &ctx.state.right, key_hash)? {
					Some(slot) => ctx.operator.join_left_with_slot(pre, indices, &slot),
					None => ctx.operator.unmatched_left_latest(pre, indices),
				};
				let result = vec![Diff::remove(removed)];
				if let Some(group) = ctx.state.left.group_of(txn, key_hash)? {
					for &idx in indices {
						ctx.state.left.remove_row_in(txn, group, pre.row_numbers()[idx])?;
					}
				}
				Ok(result)
			}
			JoinSide::Right => self.handle_right_remove(txn, key_hash, ctx),
		}
	}

	fn handle_right_remove(
		&self,
		txn: &mut FlowTransaction,
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		if ctx.operator.snapshot {
			let ledger = ctx.operator.snapshot_ledger();
			let snapshot_ctx = SnapshotJoinContext {
				ledger: &ledger,
				operator: ctx.operator,
				right_store: &ctx.state.right,
			};
			retire_slot(txn, &snapshot_ctx, key_hash)?;
			remove_right_slot(txn, &ctx.state.right, key_hash)?;
			return Ok(Vec::new());
		}
		let old = read_right_slot(txn, &ctx.state.right, key_hash)?;
		remove_right_slot(txn, &ctx.state.right, key_hash)?;
		let operator = ctx.operator;
		let mut result = Vec::new();
		if let Some(old_slot) = old {
			for_each_left_block(txn, &ctx.state.left, key_hash, |_txn, left| {
				let left_indices: Vec<usize> = (0..left.row_count()).collect();
				let pre = operator.join_left_with_slot(left, &left_indices, &old_slot);
				let post = operator.unmatched_left_latest(left, &left_indices);
				result.push(Diff::update(pre, post));
				Ok(())
			})?;
		}
		Ok(result)
	}

	#[instrument(name = "flow::operator::join::latest_left::handle_update", level = "trace", skip_all)]
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

		match ctx.side {
			JoinSide::Left => {
				if ctx.operator.snapshot {
					let ledger = ctx.operator.snapshot_ledger();
					let snapshot_ctx = SnapshotJoinContext {
						ledger: &ledger,
						operator: ctx.operator,
						right_store: &ctx.state.right,
					};
					let mut result = Vec::new();
					let withdraw_group = ctx.state.right.group_of(txn, keys.pre)?;
					for &idx in indices {
						if let Some(slot) = republished_slot(
							txn,
							&snapshot_ctx,
							withdraw_group,
							pre,
							post,
							idx,
						)? {
							result.push(Diff::update(
								ctx.operator.join_left_with_slot(pre, &[idx], &slot),
								ctx.operator.join_left_with_slot(post, &[idx], &slot),
							));
							continue;
						}
						let withdrawn = match withdraw_group {
							Some(group) => {
								withdraw_slot(txn, &snapshot_ctx, group, pre, idx)?
							}
							None => None,
						};
						let published = publish_slot(
							txn,
							&snapshot_ctx,
							keys.post,
							post,
							&[idx],
							true,
						)?;
						result.extend(update_diff(withdrawn, published));
					}
					return Ok(result);
				}

				let prepared = prepare_entry_update(txn, &ctx.state.left, keys.pre, post)?;
				for &idx in indices {
					if let Some(prepared) = &prepared {
						update_row_in_entry(
							txn,
							&ctx.state.left,
							prepared,
							pre.row_numbers()[idx],
							post,
							idx,
						)?;
					}
				}
				let (pre_joined, post_joined) = match read_right_slot(txn, &ctx.state.right, keys.pre)?
				{
					Some(slot) => (
						ctx.operator.join_left_with_slot(pre, indices, &slot),
						ctx.operator.join_left_with_slot(post, indices, &slot),
					),
					None => (
						ctx.operator.unmatched_left_latest(pre, indices),
						ctx.operator.unmatched_left_latest(post, indices),
					),
				};
				Ok(vec![Diff::update(pre_joined, post_joined)])
			}
			JoinSide::Right => self.handle_right_insert(txn, post, indices, keys.post, ctx),
		}
	}
}
