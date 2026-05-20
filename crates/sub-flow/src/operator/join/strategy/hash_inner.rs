// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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

	pub(crate) fn handle_update_undefined(
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

		let emit_ctx = JoinEmitContext {
			opposite_store: match ctx.side {
				JoinSide::Left => &ctx.state.right,
				JoinSide::Right => &ctx.state.left,
			},
			key_hash,
			operator: ctx.operator,
		};

		if let Some(diff) = emit_joined_columns_batch(txn, post, indices, ctx.side, &emit_ctx)? {
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

		let mut result = Vec::new();

		let snapshot_right = ctx.operator.snapshot && matches!(ctx.side, JoinSide::Right);

		if !snapshot_right {
			let emit_ctx = JoinEmitContext {
				opposite_store: match ctx.side {
					JoinSide::Left => &ctx.state.right,
					JoinSide::Right => &ctx.state.left,
				},
				key_hash,
				operator: ctx.operator,
			};

			if let Some(diff) = emit_remove_joined_columns_batch(txn, pre, indices, ctx.side, &emit_ctx)? {
				result.push(diff);
			}
		}

		for &idx in indices {
			let row_number = pre.row_numbers[idx];

			if matches!(ctx.side, JoinSide::Left) {
				ctx.operator.cleanup_left_row_joins(txn, *row_number)?;
			}

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
		let pre_row_number = pre.row_numbers[row_idx];

		let updated = match ctx.side {
			JoinSide::Left => {
				update_row_in_entry(txn, &mut ctx.state.left, keys.pre, pre_row_number, post, row_idx)?
			}
			JoinSide::Right => {
				update_row_in_entry(txn, &mut ctx.state.right, keys.pre, pre_row_number, post, row_idx)?
			}
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

		match emit_update_joined_columns(txn, pre, post, row_idx, ctx.side, &emit_ctx)? {
			Some(diff) => Ok(vec![diff]),
			None => Ok(Vec::new()),
		}
	}
}
