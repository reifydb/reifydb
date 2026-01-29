// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::columns::Columns;
use reifydb_runtime::hash::Hash128;
use reifydb_sdk::flow::FlowDiff;
use reifydb_type::value::row_number::RowNumber;

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

/// Add a row to a state entry (left or right)
/// Takes Columns + row index instead of Row to avoid allocation
pub(crate) fn add_to_state_entry(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	columns: &Columns,
	row_idx: usize,
) -> reifydb_type::Result<()> {
	let row_number = columns.row_numbers[row_idx];
	let mut entry = store.get_or_insert_with(txn, key_hash, || JoinSideEntry {
		rows: Vec::new(),
	})?;
	entry.rows.push(row_number);
	store.set(txn, key_hash, &entry)?;
	Ok(())
}

/// Add multiple rows to a state entry
pub(crate) fn add_to_state_entry_batch(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	columns: &Columns,
	indices: &[usize],
) -> reifydb_type::Result<()> {
	let mut entry = store.get_or_insert_with(txn, key_hash, || JoinSideEntry {
		rows: Vec::new(),
	})?;
	for &idx in indices {
		entry.rows.push(columns.row_numbers[idx]);
	}
	store.set(txn, key_hash, &entry)?;
	Ok(())
}

/// Remove a row from state entry and cleanup if empty
pub(crate) fn remove_from_state_entry(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	row_number: RowNumber,
) -> reifydb_type::Result<bool> {
	if let Some(mut entry) = store.get(txn, key_hash)? {
		entry.rows.retain(|&r| r != row_number);

		if entry.rows.is_empty() {
			store.remove(txn, key_hash)?;
			Ok(true) // Entry was removed
		} else {
			store.set(txn, key_hash, &entry)?;
			Ok(false) // Entry still has rows
		}
	} else {
		Ok(false) // Entry didn't exist
	}
}

/// Update a row in-place within a state entry
pub(crate) fn update_row_in_entry(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	old_row_number: RowNumber,
	new_row_number: RowNumber,
) -> reifydb_type::Result<bool> {
	if let Some(mut entry) = store.get(txn, key_hash)? {
		for row in &mut entry.rows {
			if *row == old_row_number {
				*row = new_row_number;
				store.set(txn, key_hash, &entry)?;
				return Ok(true);
			}
		}
	}
	Ok(false)
}

/// Check if a right side has any rows for a given key
pub(crate) fn has_right_rows(
	txn: &mut FlowTransaction,
	right_store: &Store,
	key_hash: &Hash128,
) -> reifydb_type::Result<bool> {
	Ok(right_store.contains_key(txn, key_hash)?)
}

/// Check if it's the first right row being added for a key
pub(crate) fn is_first_right_row(
	txn: &mut FlowTransaction,
	right_store: &Store,
	key_hash: &Hash128,
) -> reifydb_type::Result<bool> {
	Ok(!right_store.contains_key(txn, key_hash)?)
}

/// Get all rows from a store for a given key as Columns (no Row conversion)
pub(crate) fn pull_from_store(
	txn: &mut FlowTransaction,
	store: &Store,
	key_hash: &Hash128,
	parent: &Arc<Operators>,
) -> reifydb_type::Result<Columns> {
	if let Some(entry) = store.get(txn, key_hash)? {
		parent.pull(txn, &entry.rows)
	} else {
		Ok(Columns::empty())
	}
}

/// Emit joined columns when inserting a row that has matches on the opposite side.
/// Uses index-based access, no Row allocation.
pub(crate) fn emit_joined_columns(
	txn: &mut FlowTransaction,
	primary: &Columns,
	primary_idx: usize,
	primary_side: JoinSide,
	opposite_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	opposite_parent: &Arc<Operators>,
) -> reifydb_type::Result<Option<FlowDiff>> {
	let opposite = pull_from_store(txn, opposite_store, key_hash, opposite_parent)?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let joined = match primary_side {
		JoinSide::Left => operator.join_columns_one_to_many(txn, primary, primary_idx, &opposite)?,
		JoinSide::Right => operator.join_columns_many_to_one(txn, &opposite, primary, primary_idx)?,
	};

	if joined.is_empty() {
		Ok(None)
	} else {
		Ok(Some(FlowDiff::Insert {
			post: joined,
		}))
	}
}

/// Emit removal of joined columns
pub(crate) fn emit_remove_joined_columns(
	txn: &mut FlowTransaction,
	primary: &Columns,
	primary_idx: usize,
	primary_side: JoinSide,
	opposite_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	opposite_parent: &Arc<Operators>,
) -> reifydb_type::Result<Option<FlowDiff>> {
	let opposite = pull_from_store(txn, opposite_store, key_hash, opposite_parent)?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let joined = match primary_side {
		JoinSide::Left => operator.join_columns_one_to_many(txn, primary, primary_idx, &opposite)?,
		JoinSide::Right => operator.join_columns_many_to_one(txn, &opposite, primary, primary_idx)?,
	};

	if joined.is_empty() {
		Ok(None)
	} else {
		Ok(Some(FlowDiff::Remove {
			pre: joined,
		}))
	}
}

/// Emit updates for joined columns when a row is updated
pub(crate) fn emit_update_joined_columns(
	txn: &mut FlowTransaction,
	pre: &Columns,
	post: &Columns,
	row_idx: usize,
	primary_side: JoinSide,
	opposite_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	opposite_parent: &Arc<Operators>,
) -> reifydb_type::Result<Option<FlowDiff>> {
	let opposite = pull_from_store(txn, opposite_store, key_hash, opposite_parent)?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let (pre_joined, post_joined) = match primary_side {
		JoinSide::Left => (
			operator.join_columns_one_to_many(txn, pre, row_idx, &opposite)?,
			operator.join_columns_one_to_many(txn, post, row_idx, &opposite)?,
		),
		JoinSide::Right => (
			operator.join_columns_many_to_one(txn, &opposite, pre, row_idx)?,
			operator.join_columns_many_to_one(txn, &opposite, post, row_idx)?,
		),
	};

	if pre_joined.is_empty() || post_joined.is_empty() {
		Ok(None)
	} else {
		Ok(Some(FlowDiff::Update {
			pre: pre_joined,
			post: post_joined,
		}))
	}
}

/// Emit joined columns for a batch of rows with the same key
pub(crate) fn emit_joined_columns_batch(
	txn: &mut FlowTransaction,
	primary: &Columns,
	primary_indices: &[usize],
	primary_side: JoinSide,
	opposite_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	opposite_parent: &Arc<Operators>,
) -> reifydb_type::Result<Option<FlowDiff>> {
	if primary_indices.is_empty() {
		return Ok(None);
	}

	let opposite = pull_from_store(txn, opposite_store, key_hash, opposite_parent)?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let joined = match primary_side {
		JoinSide::Left => operator.join_columns_cartesian(txn, primary, primary_indices, &opposite)?,
		JoinSide::Right => {
			// For right side, we need to swap: opposite (left) x primary (right)
			// But cartesian takes (left, left_indices, right)
			// So we need to join opposite with all rows, repeated for each primary index
			let opposite_indices: Vec<usize> = (0..opposite.row_count()).collect();
			operator.join_columns_cartesian(txn, &opposite, &opposite_indices, primary)?
		}
	};

	if joined.is_empty() {
		Ok(None)
	} else {
		Ok(Some(FlowDiff::Insert {
			post: joined,
		}))
	}
}

/// Emit removal of joined columns for a batch
pub(crate) fn emit_remove_joined_columns_batch(
	txn: &mut FlowTransaction,
	primary: &Columns,
	primary_indices: &[usize],
	primary_side: JoinSide,
	opposite_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	opposite_parent: &Arc<Operators>,
) -> reifydb_type::Result<Option<FlowDiff>> {
	if primary_indices.is_empty() {
		return Ok(None);
	}

	let opposite = pull_from_store(txn, opposite_store, key_hash, opposite_parent)?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let joined = match primary_side {
		JoinSide::Left => operator.join_columns_cartesian(txn, primary, primary_indices, &opposite)?,
		JoinSide::Right => {
			let opposite_indices: Vec<usize> = (0..opposite.row_count()).collect();
			operator.join_columns_cartesian(txn, &opposite, &opposite_indices, primary)?
		}
	};

	if joined.is_empty() {
		Ok(None)
	} else {
		Ok(Some(FlowDiff::Remove {
			pre: joined,
		}))
	}
}

/// Get all left rows for a given key as Columns
pub(crate) fn pull_left_columns(
	txn: &mut FlowTransaction,
	left_store: &Store,
	key_hash: &Hash128,
	left_parent: &Arc<Operators>,
) -> reifydb_type::Result<Columns> {
	pull_from_store(txn, left_store, key_hash, left_parent)
}
