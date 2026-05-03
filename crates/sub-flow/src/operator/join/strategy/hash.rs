// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{interface::change::Diff, value::column::columns::Columns};
use reifydb_runtime::hash::Hash128;
use reifydb_type::{Result, value::row_number::RowNumber};

use crate::{
	operator::{
		Operators,
		join::{
			operator::JoinOperator,
			state::{JoinSide, JoinSideEntry},
			store::Store,
		},
	},
	transaction::FlowTransaction,
};

pub(crate) fn add_to_state_entry(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	columns: &Columns,
	row_idx: usize,
) -> Result<()> {
	let row_number = columns.row_numbers[row_idx];
	let mut entry = store.get_or_insert_with(txn, key_hash, || JoinSideEntry {
		rows: Vec::new(),
	})?;
	entry.rows.push(row_number);
	store.set(txn, key_hash, &entry)?;
	Ok(())
}

pub(crate) fn add_to_state_entry_batch(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	columns: &Columns,
	indices: &[usize],
) -> Result<()> {
	let mut entry = store.get_or_insert_with(txn, key_hash, || JoinSideEntry {
		rows: Vec::new(),
	})?;
	for &idx in indices {
		entry.rows.push(columns.row_numbers[idx]);
	}
	store.set(txn, key_hash, &entry)?;
	Ok(())
}

pub(crate) fn remove_from_state_entry(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	row_number: RowNumber,
) -> Result<bool> {
	if let Some(mut entry) = store.get(txn, key_hash)? {
		entry.rows.retain(|&r| r != row_number);

		if entry.rows.is_empty() {
			store.remove(txn, key_hash)?;
			Ok(true)
		} else {
			store.set(txn, key_hash, &entry)?;
			Ok(false)
		}
	} else {
		Ok(false)
	}
}

pub(crate) fn update_row_in_entry(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	pre_row_number: RowNumber,
	post_row_number: RowNumber,
) -> Result<bool> {
	if let Some(mut entry) = store.get(txn, key_hash)? {
		for row in &mut entry.rows {
			if *row == pre_row_number {
				*row = post_row_number;
				store.set(txn, key_hash, &entry)?;
				return Ok(true);
			}
		}
	}
	Ok(false)
}

pub(crate) fn has_right_rows(txn: &mut FlowTransaction, right_store: &Store, key_hash: &Hash128) -> Result<bool> {
	right_store.contains_key(txn, key_hash)
}

pub(crate) fn is_first_right_row(txn: &mut FlowTransaction, right_store: &Store, key_hash: &Hash128) -> Result<bool> {
	Ok(!right_store.contains_key(txn, key_hash)?)
}

pub(crate) fn pull_from_store(
	txn: &mut FlowTransaction,
	store: &Store,
	key_hash: &Hash128,
	parent: &Arc<Operators>,
) -> Result<Columns> {
	if let Some(entry) = store.get(txn, key_hash)? {
		parent.pull(txn, &entry.rows)
	} else {
		Ok(Columns::empty())
	}
}

pub(crate) struct JoinEmitContext<'a> {
	pub opposite_store: &'a Store,
	pub key_hash: &'a Hash128,
	pub operator: &'a JoinOperator,
	pub opposite_parent: &'a Arc<Operators>,
}

pub(crate) fn emit_joined_columns(
	txn: &mut FlowTransaction,
	primary: &Columns,
	primary_idx: usize,
	primary_side: JoinSide,
	ctx: &JoinEmitContext<'_>,
) -> Result<Option<Diff>> {
	let opposite = pull_from_store(txn, ctx.opposite_store, ctx.key_hash, ctx.opposite_parent)?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let joined = match primary_side {
		JoinSide::Left => ctx.operator.join_columns_one_to_many(txn, primary, primary_idx, &opposite)?,
		JoinSide::Right => ctx.operator.join_columns_many_to_one(txn, &opposite, primary, primary_idx)?,
	};

	if joined.is_empty() {
		Ok(None)
	} else {
		Ok(Some(Diff::insert(joined)))
	}
}

pub(crate) fn emit_remove_joined_columns(
	txn: &mut FlowTransaction,
	primary: &Columns,
	primary_idx: usize,
	primary_side: JoinSide,
	ctx: &JoinEmitContext<'_>,
) -> Result<Option<Diff>> {
	let opposite = pull_from_store(txn, ctx.opposite_store, ctx.key_hash, ctx.opposite_parent)?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let joined = match primary_side {
		JoinSide::Left => ctx.operator.join_columns_one_to_many(txn, primary, primary_idx, &opposite)?,
		JoinSide::Right => ctx.operator.join_columns_many_to_one(txn, &opposite, primary, primary_idx)?,
	};

	if joined.is_empty() {
		Ok(None)
	} else {
		Ok(Some(Diff::remove(joined)))
	}
}

pub(crate) fn emit_update_joined_columns(
	txn: &mut FlowTransaction,
	pre: &Columns,
	post: &Columns,
	row_idx: usize,
	primary_side: JoinSide,
	ctx: &JoinEmitContext<'_>,
) -> Result<Option<Diff>> {
	let opposite = pull_from_store(txn, ctx.opposite_store, ctx.key_hash, ctx.opposite_parent)?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let (pre_joined, post_joined) = match primary_side {
		JoinSide::Left => (
			ctx.operator.join_columns_one_to_many(txn, pre, row_idx, &opposite)?,
			ctx.operator.join_columns_one_to_many(txn, post, row_idx, &opposite)?,
		),
		JoinSide::Right => (
			ctx.operator.join_columns_many_to_one(txn, &opposite, pre, row_idx)?,
			ctx.operator.join_columns_many_to_one(txn, &opposite, post, row_idx)?,
		),
	};

	if pre_joined.is_empty() || post_joined.is_empty() {
		Ok(None)
	} else {
		Ok(Some(Diff::update(pre_joined, post_joined)))
	}
}

pub(crate) fn emit_joined_columns_batch(
	txn: &mut FlowTransaction,
	primary: &Columns,
	primary_indices: &[usize],
	primary_side: JoinSide,
	ctx: &JoinEmitContext<'_>,
) -> Result<Option<Diff>> {
	if primary_indices.is_empty() {
		return Ok(None);
	}

	let opposite = pull_from_store(txn, ctx.opposite_store, ctx.key_hash, ctx.opposite_parent)?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let joined = match primary_side {
		JoinSide::Left => {
			let opposite_indices: Vec<usize> = (0..opposite.row_count()).collect();
			ctx.operator.join_columns_cartesian(
				txn,
				primary,
				primary_indices,
				&opposite,
				&opposite_indices,
			)?
		}
		JoinSide::Right => {
			let opposite_indices: Vec<usize> = (0..opposite.row_count()).collect();
			ctx.operator.join_columns_cartesian(
				txn,
				&opposite,
				&opposite_indices,
				primary,
				primary_indices,
			)?
		}
	};

	if joined.is_empty() {
		Ok(None)
	} else {
		Ok(Some(Diff::insert(joined)))
	}
}

pub(crate) fn emit_remove_joined_columns_batch(
	txn: &mut FlowTransaction,
	primary: &Columns,
	primary_indices: &[usize],
	primary_side: JoinSide,
	ctx: &JoinEmitContext<'_>,
) -> Result<Option<Diff>> {
	if primary_indices.is_empty() {
		return Ok(None);
	}

	let opposite = pull_from_store(txn, ctx.opposite_store, ctx.key_hash, ctx.opposite_parent)?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let joined = match primary_side {
		JoinSide::Left => {
			let opposite_indices: Vec<usize> = (0..opposite.row_count()).collect();
			ctx.operator.join_columns_cartesian(
				txn,
				primary,
				primary_indices,
				&opposite,
				&opposite_indices,
			)?
		}
		JoinSide::Right => {
			let opposite_indices: Vec<usize> = (0..opposite.row_count()).collect();
			ctx.operator.join_columns_cartesian(
				txn,
				&opposite,
				&opposite_indices,
				primary,
				primary_indices,
			)?
		}
	};

	if joined.is_empty() {
		Ok(None)
	} else {
		Ok(Some(Diff::remove(joined)))
	}
}

pub(crate) fn pull_left_columns(
	txn: &mut FlowTransaction,
	left_store: &Store,
	key_hash: &Hash128,
	left_parent: &Arc<Operators>,
) -> Result<Columns> {
	pull_from_store(txn, left_store, key_hash, left_parent)
}
