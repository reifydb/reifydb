// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_core::{interface::change::Diff, key::operator::state::GroupId};
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{
		row_number::RowNumber,
		system_columns::{require_row_numbers, row_numbers},
	},
};
use tracing::instrument;

use super::{
	JoinContext, UpdateKeys,
	hash::{add_to_state_entry_batch, for_each_left_block, prepare_entry_update, update_row_in_entry},
	latest::{overwrite_right_slot, read_right_slot, remove_right_rows, republish, write_right_rows},
	latest_inner::{republished_slot, update_diff},
};
use crate::operator::{
	host::HostContext,
	join::{
		Identity,
		operator::JoinOperator,
		snapshot::{SnapshotJoinContext, publish_slot, retire_slot, withdraw_slot},
		state::JoinSide,
	},
};

pub(crate) struct LatestLeftHashJoin;

fn publish_unkeyed(
	host: &mut dyn HostContext,
	operator: &JoinOperator,
	left: &RecordBatch,
	row_idx: usize,
	reuse: Option<RowNumber>,
) -> Result<RecordBatch> {
	let left_number = require_row_numbers(left)?[row_idx];
	let (id, _) = operator.snapshot_ledger().publish_unmatched_as(host, GroupId::UNKEYED, left_number, reuse)?;
	operator.unmatched_left_latest(left, &[row_idx], &[id])
}

fn withdraw_unkeyed(
	host: &mut dyn HostContext,
	ctx: &JoinContext,
	pre: &RecordBatch,
	row_idx: usize,
) -> Result<Option<(RecordBatch, RowNumber)>> {
	let ledger = ctx.operator.snapshot_ledger();
	let snapshot_ctx = SnapshotJoinContext {
		ledger: &ledger,
		operator: ctx.operator,
		right_store: &ctx.state.right,
	};
	withdraw_slot(host, &snapshot_ctx, GroupId::UNKEYED, pre, row_idx)
}

impl LatestLeftHashJoin {
	pub(crate) fn handle_insert_undefined(
		&self,
		host: &mut dyn HostContext,
		post: &RecordBatch,
		row_idx: usize,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match ctx.side {
			JoinSide::Left if ctx.operator.snapshot => {
				Ok(vec![Diff::insert(publish_unkeyed(host, ctx.operator, post, row_idx, None)?)])
			}
			JoinSide::Left => Ok(ctx
				.operator
				.latest_columns(host, post, &[row_idx], None, Identity::Mint)?
				.published()),
			JoinSide::Right => Ok(Vec::new()),
		}
	}

	pub(crate) fn handle_remove_undefined(
		&self,
		host: &mut dyn HostContext,
		pre: &RecordBatch,
		row_idx: usize,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match ctx.side {
			JoinSide::Left if ctx.operator.snapshot => Ok(withdraw_unkeyed(host, ctx, pre, row_idx)?
				.map(|(columns, _)| vec![Diff::remove(columns)])
				.unwrap_or_default()),
			JoinSide::Left => Ok(ctx
				.operator
				.latest_columns(host, pre, &[row_idx], None, Identity::Consume)?
				.withdrawn()
				.into_iter()
				.collect()),
			JoinSide::Right => Ok(Vec::new()),
		}
	}

	pub(crate) fn handle_update_both_undefined(
		&self,
		host: &mut dyn HostContext,
		pre: &RecordBatch,
		post: &RecordBatch,
		row_idx: usize,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match ctx.side {
			JoinSide::Left if ctx.operator.snapshot => {
				let withdrawn = withdraw_unkeyed(host, ctx, pre, row_idx)?;
				let reuse = withdrawn.as_ref().map(|(_, id)| *id);
				let published = publish_unkeyed(host, ctx.operator, post, row_idx, reuse)?;
				Ok(update_diff(withdrawn.map(|(columns, _)| columns), Some(published)))
			}
			JoinSide::Left => republish(host, ctx.operator, (pre, None), (post, None), &[row_idx]),
			JoinSide::Right => Ok(Vec::new()),
		}
	}

	#[instrument(name = "flow::operator::join::latest_left::handle_insert", level = "trace", skip_all, fields(rows = indices.len()))]
	pub(crate) fn handle_insert(
		&self,
		host: &mut dyn HostContext,
		post: &RecordBatch,
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
						publish_slot(host, &snapshot_ctx, key_hash, post, indices, true, None)?;
					return Ok(published
						.map(|columns| vec![Diff::insert(columns)])
						.unwrap_or_default());
				}
				add_to_state_entry_batch(host, &mut ctx.state.left, key_hash, post, indices)?;
				let slot = read_right_slot(host, &ctx.state.right, key_hash)?;
				Ok(ctx.operator
					.latest_columns(host, post, indices, slot.as_ref(), Identity::Mint)?
					.published())
			}
			JoinSide::Right => self.handle_right_insert(host, post, indices, key_hash, ctx),
		}
	}

	#[instrument(name = "flow::operator::join::latest_left::handle_right_insert", level = "trace", skip_all, fields(rows = indices.len()))]
	fn handle_right_insert(
		&self,
		host: &mut dyn HostContext,
		post: &RecordBatch,
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
			retire_slot(host, &snapshot_ctx, key_hash)?;
			write_right_rows(host, &ctx.state.right, key_hash, post, indices, ctx.operator.pick())?;
			return Ok(Vec::new());
		}
		let old = read_right_slot(host, &ctx.state.right, key_hash)?;
		let new = overwrite_right_slot(host, &ctx.state.right, key_hash, post, indices, ctx.operator.pick())?;
		let operator = ctx.operator;
		let mut result = Vec::new();
		for_each_left_block(host, &ctx.state.left, key_hash, |host, left| {
			let left_indices: Vec<usize> = (0..left.num_rows()).collect();
			if let Some(new_slot) = &new {
				result.extend(republish(
					host,
					operator,
					(left, old.as_ref()),
					(left, Some(new_slot)),
					&left_indices,
				)?);
			}
			Ok(())
		})?;
		Ok(result)
	}

	#[instrument(name = "flow::operator::join::latest_left::handle_remove", level = "trace", skip_all)]
	pub(crate) fn handle_remove(
		&self,
		host: &mut dyn HostContext,
		pre: &RecordBatch,
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
					let group = ctx.state.right.group_of(key_hash);
					for &idx in indices {
						if let Some((columns, _)) =
							withdraw_slot(host, &snapshot_ctx, group, pre, idx)?
						{
							withdrawn.push(Diff::remove(columns));
						}
					}
					return Ok(withdrawn);
				}
				let mut held = Vec::with_capacity(indices.len());
				for &idx in indices {
					if ctx.state.left.remove_row(host, key_hash, require_row_numbers(pre)?[idx])? {
						held.push(idx);
					}
				}
				if held.is_empty() {
					return Ok(Vec::new());
				}
				let slot = read_right_slot(host, &ctx.state.right, key_hash)?;
				Ok(ctx.operator
					.latest_columns(host, pre, &held, slot.as_ref(), Identity::Consume)?
					.withdrawn()
					.into_iter()
					.collect())
			}
			JoinSide::Right => self.handle_right_remove(host, pre, indices, key_hash, ctx),
		}
	}

	fn handle_right_remove(
		&self,
		host: &mut dyn HostContext,
		pre: &RecordBatch,
		indices: &[usize],
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		let pre_numbers = require_row_numbers(pre)?;
		let numbers: Vec<RowNumber> = indices.iter().map(|&idx| pre_numbers[idx]).collect();
		if ctx.operator.snapshot {
			let ledger = ctx.operator.snapshot_ledger();
			let snapshot_ctx = SnapshotJoinContext {
				ledger: &ledger,
				operator: ctx.operator,
				right_store: &ctx.state.right,
			};
			retire_slot(host, &snapshot_ctx, key_hash)?;
			remove_right_rows(host, &ctx.state.right, key_hash, &numbers)?;
			return Ok(Vec::new());
		}
		let old = read_right_slot(host, &ctx.state.right, key_hash)?;
		remove_right_rows(host, &ctx.state.right, key_hash, &numbers)?;
		let new = read_right_slot(host, &ctx.state.right, key_hash)?;
		let operator = ctx.operator;
		let mut result = Vec::new();
		let Some(old_slot) = old else {
			return Ok(result);
		};
		if let Some(new_slot) = &new
			&& row_numbers(new_slot)? == row_numbers(&old_slot)?
		{
			return Ok(result);
		}
		for_each_left_block(host, &ctx.state.left, key_hash, |host, left| {
			let left_indices: Vec<usize> = (0..left.num_rows()).collect();
			result.extend(republish(
				host,
				operator,
				(left, Some(&old_slot)),
				(left, new.as_ref()),
				&left_indices,
			)?);
			Ok(())
		})?;
		Ok(result)
	}

	#[instrument(name = "flow::operator::join::latest_left::handle_update", level = "trace", skip_all)]
	pub(crate) fn handle_update(
		&self,
		host: &mut dyn HostContext,
		pre: &RecordBatch,
		post: &RecordBatch,
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
					let withdraw_group = ctx.state.right.group_of(keys.pre);
					for &idx in indices {
						if let Some((slot, id)) = republished_slot(
							host,
							&snapshot_ctx,
							withdraw_group,
							pre,
							post,
							idx,
						)? {
							result.push(Diff::update(
								ctx.operator.join_left_with_slot(
									pre,
									&[idx],
									&slot,
									&[id],
								)?,
								ctx.operator.join_left_with_slot(
									post,
									&[idx],
									&slot,
									&[id],
								)?,
							));
							continue;
						}
						let withdrawn =
							withdraw_slot(host, &snapshot_ctx, withdraw_group, pre, idx)?;
						let published = publish_slot(
							host,
							&snapshot_ctx,
							keys.post,
							post,
							&[idx],
							true,
							withdrawn.as_ref().map(|(_, id)| *id),
						)?;
						result.extend(update_diff(
							withdrawn.map(|(columns, _)| columns),
							published,
						));
					}
					return Ok(result);
				}

				let prepared = prepare_entry_update(host, &ctx.state.left, keys.pre, post)?;
				let mut held = Vec::with_capacity(indices.len());
				let mut expired = Vec::new();
				for &idx in indices {
					match update_row_in_entry(
						host,
						&ctx.state.left,
						&prepared,
						require_row_numbers(pre)?[idx],
						post,
						idx,
					)? {
						true => held.push(idx),
						false => expired.push(idx),
					}
				}
				let slot = read_right_slot(host, &ctx.state.right, keys.pre)?;
				let mut result = republish(
					host,
					ctx.operator,
					(pre, slot.as_ref()),
					(post, slot.as_ref()),
					&held,
				)?;
				result.extend(self.handle_insert(host, post, &expired, keys.post, ctx)?);
				Ok(result)
			}
			JoinSide::Right => self.handle_right_insert(host, post, indices, keys.post, ctx),
		}
	}
}
