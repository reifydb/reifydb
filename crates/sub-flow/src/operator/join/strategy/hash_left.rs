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
				Ok(vec![Diff::insert(unmatched)])
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
				Ok(vec![Diff::remove(unmatched)])
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
				Ok(vec![Diff::update(unmatched_pre, unmatched_post)])
			}
			JoinSide::Right => {
				// Right side updates with undefined keys don't produce output
				Ok(Vec::new())
			}
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

		let emit_ctx = JoinEmitContext {
			opposite_store: &ctx.state.right,
			key_hash,
			operator: ctx.operator,
			opposite_parent: &ctx.operator.right_parent,
		};

		if let Some(diff) = emit_joined_columns_batch(txn, post, indices, JoinSide::Left, &emit_ctx)? {
			return Ok(vec![diff]);
		}
		let unmatched = ctx.operator.unmatched_left_columns_batch(txn, post, indices)?;
		Ok(vec![Diff::insert(unmatched)])
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
		add_to_state_entry_batch(txn, &mut ctx.state.right, key_hash, post, indices)?;

		let mut result = Vec::new();
		// First right row(s): remove previously emitted unmatched left rows.
		// Pull via `pull_from_store` so we read the state's cached column snapshot;
		// `parent.pull` fails for transactional cross-view joins because the parent
		// view's rows aren't committed yet.
		if is_first && ctx.state.left.get(txn, key_hash)?.is_some() {
			let left_columns =
				pull_left_columns(txn, &ctx.state.left, key_hash, &ctx.operator.left_parent)?;
			if left_columns.has_rows() {
				let left_indices: Vec<usize> = (0..left_columns.row_count()).collect();
				let unmatched =
					ctx.operator.unmatched_left_columns_batch(txn, &left_columns, &left_indices)?;
				result.push(Diff::remove(unmatched));
			}
		}

		let emit_ctx = JoinEmitContext {
			opposite_store: &ctx.state.left,
			key_hash,
			operator: ctx.operator,
			opposite_parent: &ctx.operator.left_parent,
		};

		if let Some(diff) = emit_joined_columns_batch(txn, post, indices, JoinSide::Right, &emit_ctx)? {
			result.push(diff);
		}
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

		let mut result = Vec::new();
		if let Some(diff) = emit_remove_joined_columns_batch(txn, pre, indices, JoinSide::Left, &emit_ctx)? {
			result.push(diff);
		} else {
			let unmatched = ctx.operator.unmatched_left_columns_batch(txn, pre, indices)?;
			result.push(Diff::remove(unmatched));
		}

		for &idx in indices {
			let row_number = pre.row_numbers[idx];
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
		let emit_ctx = JoinEmitContext {
			opposite_store: &ctx.state.left,
			key_hash,
			operator: ctx.operator,
			opposite_parent: &ctx.operator.left_parent,
		};

		let mut result = Vec::new();
		if let Some(diff) = emit_remove_joined_columns_batch(txn, pre, indices, JoinSide::Right, &emit_ctx)? {
			result.push(diff);
		}

		let will_become_empty = if let Some(entry) = ctx.state.right.get(txn, key_hash)? {
			entry.rows.len() <= indices.len()
		} else {
			false
		};

		for &idx in indices {
			let row_number = pre.row_numbers[idx];
			remove_from_state_entry(txn, &mut ctx.state.right, key_hash, row_number)?;
		}

		if will_become_empty && !ctx.state.right.contains_key(txn, key_hash)? {
			let left_columns =
				pull_left_columns(txn, &ctx.state.left, key_hash, &ctx.operator.left_parent)?;
			if left_columns.has_rows() {
				let left_indices: Vec<usize> = (0..left_columns.row_count()).collect();
				let unmatched =
					ctx.operator.unmatched_left_columns_batch(txn, &left_columns, &left_indices)?;
				result.push(Diff::insert(unmatched));
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

		// Key changed: treat as remove + insert.
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
		let pre_row_number = pre.row_numbers[row_idx];
		let post_row_number = post.row_numbers[row_idx];

		if !update_row_in_entry(txn, &mut ctx.state.left, keys.pre, pre_row_number, post_row_number)? {
			return self.handle_insert(txn, post, &[row_idx], keys.post, ctx);
		}

		let emit_ctx = JoinEmitContext {
			opposite_store: &ctx.state.right,
			key_hash: keys.pre,
			operator: ctx.operator,
			opposite_parent: &ctx.operator.right_parent,
		};

		if let Some(diff) = emit_update_joined_columns(txn, pre, post, row_idx, JoinSide::Left, &emit_ctx)? {
			return Ok(vec![diff]);
		}
		let unmatched_pre = ctx.operator.unmatched_left_columns(txn, pre, row_idx)?;
		let unmatched_post = ctx.operator.unmatched_left_columns(txn, post, row_idx)?;
		Ok(vec![Diff::update(unmatched_pre, unmatched_post)])
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
		let pre_row_number = pre.row_numbers[row_idx];
		let post_row_number = post.row_numbers[row_idx];

		if !update_row_in_entry(txn, &mut ctx.state.right, keys.pre, pre_row_number, post_row_number)? {
			return self.handle_insert(txn, post, &[row_idx], keys.post, ctx);
		}

		let emit_ctx = JoinEmitContext {
			opposite_store: &ctx.state.left,
			key_hash: keys.pre,
			operator: ctx.operator,
			opposite_parent: &ctx.operator.left_parent,
		};

		match emit_update_joined_columns(txn, pre, post, row_idx, JoinSide::Right, &emit_ctx)? {
			Some(diff) => Ok(vec![diff]),
			None => Ok(Vec::new()),
		}
	}
}
