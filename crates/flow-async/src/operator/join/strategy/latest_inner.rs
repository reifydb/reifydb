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
};
use crate::operator::{
	host::HostContext,
	join::{
		Identity,
		snapshot::{SnapshotJoinContext, publish_slot, retain_published_slot, retire_slot, withdraw_slot},
		state::JoinSide,
	},
};

pub(crate) struct LatestInnerHashJoin;

impl LatestInnerHashJoin {
	pub(crate) fn handle_insert_undefined(
		&self,
		_host: &mut dyn HostContext,
		_post: &RecordBatch,
		_row_idx: usize,
		_ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		Ok(Vec::new())
	}

	pub(crate) fn handle_remove_undefined(
		&self,
		_host: &mut dyn HostContext,
		_pre: &RecordBatch,
		_row_idx: usize,
		_ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		Ok(Vec::new())
	}

	pub(crate) fn handle_update_both_undefined(
		&self,
		_host: &mut dyn HostContext,
		_pre: &RecordBatch,
		_post: &RecordBatch,
		_row_idx: usize,
		_ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		Ok(Vec::new())
	}

	#[instrument(name = "flow::operator::join::latest_inner::handle_insert", level = "trace", skip_all, fields(rows = indices.len()))]
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
					let published = publish_slot(
						host,
						&snapshot_ctx,
						key_hash,
						post,
						indices,
						false,
						None,
					)?;
					return Ok(published
						.map(|columns| vec![Diff::insert(columns)])
						.unwrap_or_default());
				}
				add_to_state_entry_batch(host, &ctx.state.left, key_hash, ctx.rows, indices)?;
				match read_right_slot(host, &ctx.state.right, key_hash)? {
					Some(slot) => Ok(ctx
						.operator
						.latest_columns(host, post, indices, Some(&slot), Identity::Mint)?
						.published()),
					None => Ok(Vec::new()),
				}
			}
			JoinSide::Right => self.handle_right_insert(host, indices, key_hash, ctx),
		}
	}

	#[instrument(name = "flow::operator::join::latest_inner::handle_right_insert", level = "trace", skip_all, fields(rows = indices.len()))]
	fn handle_right_insert(
		&self,
		host: &mut dyn HostContext,
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
			write_right_rows(host, &ctx.state.right, key_hash, ctx.rows, indices, ctx.operator.pick())?;
			return Ok(Vec::new());
		}
		let old = read_right_slot(host, &ctx.state.right, key_hash)?;
		let new =
			overwrite_right_slot(host, &ctx.state.right, key_hash, ctx.rows, indices, ctx.operator.pick())?;
		let operator = ctx.operator;
		let mut result = Vec::new();
		for_each_left_block(host, &ctx.state.left, key_hash, |host, left| {
			let left_indices: Vec<usize> = (0..left.num_rows()).collect();
			match (&old, &new) {
				(Some(old_slot), Some(new_slot)) => result.extend(republish(
					host,
					operator,
					(left, Some(old_slot)),
					(left, Some(new_slot)),
					&left_indices,
				)?),
				(None, Some(new_slot)) => result.extend(operator
					.latest_columns(host, left, &left_indices, Some(new_slot), Identity::Mint)?
					.published()),
				_ => {}
			}
			Ok(())
		})?;
		Ok(result)
	}

	#[instrument(name = "flow::operator::join::latest_inner::handle_remove", level = "trace", skip_all)]
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
			match &new {
				Some(new_slot) => result.extend(republish(
					host,
					operator,
					(left, Some(&old_slot)),
					(left, Some(new_slot)),
					&left_indices,
				)?),
				None => result.extend(operator
					.latest_columns(host, left, &left_indices, Some(&old_slot), Identity::Consume)?
					.withdrawn()),
			}
			Ok(())
		})?;
		Ok(result)
	}

	#[instrument(name = "flow::operator::join::latest_inner::handle_update", level = "trace", skip_all)]
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
							false,
							withdrawn.as_ref().map(|(_, id)| *id),
						)?;
						result.extend(update_diff(
							withdrawn.map(|(columns, _)| columns),
							published,
						));
					}
					return Ok(result);
				}

				let prepared = prepare_entry_update(host, &ctx.state.left, keys.pre, ctx.rows)?;
				let mut held = Vec::with_capacity(indices.len());
				let mut expired = Vec::new();
				for &idx in indices {
					match update_row_in_entry(
						host,
						&ctx.state.left,
						&prepared,
						require_row_numbers(pre)?[idx],
						ctx.rows,
						idx,
					)? {
						true => held.push(idx),
						false => expired.push(idx),
					}
				}
				let mut result = match read_right_slot(host, &ctx.state.right, keys.pre)? {
					Some(slot) => republish(
						host,
						ctx.operator,
						(pre, Some(&slot)),
						(post, Some(&slot)),
						&held,
					)?,
					None => Vec::new(),
				};
				result.extend(self.handle_insert(host, post, &expired, keys.post, ctx)?);
				Ok(result)
			}
			JoinSide::Right => self.handle_right_insert(host, indices, keys.post, ctx),
		}
	}
}

pub(crate) fn republished_slot(
	host: &mut dyn HostContext,
	ctx: &SnapshotJoinContext,
	group: GroupId,
	pre: &RecordBatch,
	post: &RecordBatch,
	idx: usize,
) -> Result<Option<(RecordBatch, RowNumber)>> {
	let left = require_row_numbers(pre)?[idx];
	if left != require_row_numbers(post)?[idx] {
		return Ok(None);
	}
	retain_published_slot(host, ctx, group, left)
}

pub(crate) fn update_diff(withdrawn: Option<RecordBatch>, published: Option<RecordBatch>) -> Vec<Diff> {
	match (withdrawn, published) {
		(Some(before), Some(after)) => vec![Diff::update(before, after)],
		(Some(before), None) => vec![Diff::remove(before)],
		(None, Some(after)) => vec![Diff::insert(after)],
		(None, None) => Vec::new(),
	}
}
