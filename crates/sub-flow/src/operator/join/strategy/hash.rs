// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::Columns;
use reifydb_hash::Hash128;
use reifydb_sdk::FlowDiff;
use reifydb_type::RowNumber;

use crate::{
	operator::{
		Operators,
		join::{JoinSide, JoinSideEntry, Store, operator::JoinOperator},
	},
	transaction::FlowTransaction,
};

/// Add a row to a state entry (left or right)
/// Takes Columns + row index instead of Row to avoid allocation
pub(crate) async fn add_to_state_entry(
	txn: &mut FlowTransaction<'_>,
	store: &mut Store,
	key_hash: &Hash128,
	columns: &Columns,
	row_idx: usize,
) -> crate::Result<()> {
	let row_number = columns.row_numbers[row_idx];
	let mut entry = store
		.get_or_insert_with(txn, key_hash, || JoinSideEntry {
			rows: Vec::new(),
		})
		.await?;
	entry.rows.push(row_number);
	store.set(txn, key_hash, &entry).await?;
	Ok(())
}

/// Add multiple rows to a state entry
pub(crate) async fn add_to_state_entry_batch(
	txn: &mut FlowTransaction<'_>,
	store: &mut Store,
	key_hash: &Hash128,
	columns: &Columns,
	indices: &[usize],
) -> crate::Result<()> {
	let mut entry = store
		.get_or_insert_with(txn, key_hash, || JoinSideEntry {
			rows: Vec::new(),
		})
		.await?;
	for &idx in indices {
		entry.rows.push(columns.row_numbers[idx]);
	}
	store.set(txn, key_hash, &entry).await?;
	Ok(())
}

/// Remove a row from state entry and cleanup if empty
pub(crate) async fn remove_from_state_entry(
	txn: &mut FlowTransaction<'_>,
	store: &mut Store,
	key_hash: &Hash128,
	row_number: RowNumber,
) -> crate::Result<bool> {
	if let Some(mut entry) = store.get(txn, key_hash).await? {
		entry.rows.retain(|&r| r != row_number);

		if entry.rows.is_empty() {
			store.remove(txn, key_hash).await?;
			Ok(true) // Entry was removed
		} else {
			store.set(txn, key_hash, &entry).await?;
			Ok(false) // Entry still has rows
		}
	} else {
		Ok(false) // Entry didn't exist
	}
}

/// Update a row in-place within a state entry
pub(crate) async fn update_row_in_entry(
	txn: &mut FlowTransaction<'_>,
	store: &mut Store,
	key_hash: &Hash128,
	old_row_number: RowNumber,
	new_row_number: RowNumber,
) -> crate::Result<bool> {
	if let Some(mut entry) = store.get(txn, key_hash).await? {
		for row in &mut entry.rows {
			if *row == old_row_number {
				*row = new_row_number;
				store.set(txn, key_hash, &entry).await?;
				return Ok(true);
			}
		}
	}
	Ok(false)
}

/// Check if a right side has any rows for a given key
pub(crate) async fn has_right_rows(
	txn: &mut FlowTransaction<'_>,
	right_store: &Store,
	key_hash: &Hash128,
) -> crate::Result<bool> {
	Ok(right_store.contains_key(txn, key_hash).await?)
}

/// Check if it's the first right row being added for a key
pub(crate) async fn is_first_right_row(
	txn: &mut FlowTransaction<'_>,
	right_store: &Store,
	key_hash: &Hash128,
) -> crate::Result<bool> {
	Ok(!right_store.contains_key(txn, key_hash).await?)
}

/// Get all rows from a store for a given key as Columns (no Row conversion)
pub(crate) async fn pull_from_store(
	txn: &mut FlowTransaction<'_>,
	store: &Store,
	key_hash: &Hash128,
	parent: &Arc<Operators>,
) -> crate::Result<Columns> {
	if let Some(entry) = store.get(txn, key_hash).await? {
		parent.pull(txn, &entry.rows).await
	} else {
		Ok(Columns::empty())
	}
}

/// Emit joined columns when inserting a row that has matches on the opposite side.
/// Uses index-based access, no Row allocation.
pub(crate) async fn emit_joined_columns(
	txn: &mut FlowTransaction<'_>,
	primary: &Columns,
	primary_idx: usize,
	primary_side: JoinSide,
	opposite_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	opposite_parent: &Arc<Operators>,
) -> crate::Result<Option<FlowDiff>> {
	let opposite = pull_from_store(txn, opposite_store, key_hash, opposite_parent).await?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let joined = match primary_side {
		JoinSide::Left => operator.join_columns_one_to_many(txn, primary, primary_idx, &opposite).await?,
		JoinSide::Right => operator.join_columns_many_to_one(txn, &opposite, primary, primary_idx).await?,
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
pub(crate) async fn emit_remove_joined_columns(
	txn: &mut FlowTransaction<'_>,
	primary: &Columns,
	primary_idx: usize,
	primary_side: JoinSide,
	opposite_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	opposite_parent: &Arc<Operators>,
) -> crate::Result<Option<FlowDiff>> {
	let opposite = pull_from_store(txn, opposite_store, key_hash, opposite_parent).await?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let joined = match primary_side {
		JoinSide::Left => operator.join_columns_one_to_many(txn, primary, primary_idx, &opposite).await?,
		JoinSide::Right => operator.join_columns_many_to_one(txn, &opposite, primary, primary_idx).await?,
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
pub(crate) async fn emit_update_joined_columns(
	txn: &mut FlowTransaction<'_>,
	pre: &Columns,
	post: &Columns,
	row_idx: usize,
	primary_side: JoinSide,
	opposite_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	opposite_parent: &Arc<Operators>,
) -> crate::Result<Option<FlowDiff>> {
	let opposite = pull_from_store(txn, opposite_store, key_hash, opposite_parent).await?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let (pre_joined, post_joined) = match primary_side {
		JoinSide::Left => (
			operator.join_columns_one_to_many(txn, pre, row_idx, &opposite).await?,
			operator.join_columns_one_to_many(txn, post, row_idx, &opposite).await?,
		),
		JoinSide::Right => (
			operator.join_columns_many_to_one(txn, &opposite, pre, row_idx).await?,
			operator.join_columns_many_to_one(txn, &opposite, post, row_idx).await?,
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
pub(crate) async fn emit_joined_columns_batch(
	txn: &mut FlowTransaction<'_>,
	primary: &Columns,
	primary_indices: &[usize],
	primary_side: JoinSide,
	opposite_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	opposite_parent: &Arc<Operators>,
) -> crate::Result<Option<FlowDiff>> {
	if primary_indices.is_empty() {
		return Ok(None);
	}

	let opposite = pull_from_store(txn, opposite_store, key_hash, opposite_parent).await?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let joined = match primary_side {
		JoinSide::Left => operator.join_columns_cartesian(txn, primary, primary_indices, &opposite).await?,
		JoinSide::Right => {
			// For right side, we need to swap: opposite (left) x primary (right)
			// But cartesian takes (left, left_indices, right)
			// So we need to join opposite with all rows, repeated for each primary index
			let opposite_indices: Vec<usize> = (0..opposite.row_count()).collect();
			operator.join_columns_cartesian(txn, &opposite, &opposite_indices, primary).await?
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
pub(crate) async fn emit_remove_joined_columns_batch(
	txn: &mut FlowTransaction<'_>,
	primary: &Columns,
	primary_indices: &[usize],
	primary_side: JoinSide,
	opposite_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	opposite_parent: &Arc<Operators>,
) -> crate::Result<Option<FlowDiff>> {
	if primary_indices.is_empty() {
		return Ok(None);
	}

	let opposite = pull_from_store(txn, opposite_store, key_hash, opposite_parent).await?;
	if opposite.is_empty() {
		return Ok(None);
	}

	let joined = match primary_side {
		JoinSide::Left => operator.join_columns_cartesian(txn, primary, primary_indices, &opposite).await?,
		JoinSide::Right => {
			let opposite_indices: Vec<usize> = (0..opposite.row_count()).collect();
			operator.join_columns_cartesian(txn, &opposite, &opposite_indices, primary).await?
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
pub(crate) async fn pull_left_columns(
	txn: &mut FlowTransaction<'_>,
	left_store: &Store,
	key_hash: &Hash128,
	left_parent: &Arc<Operators>,
) -> crate::Result<Columns> {
	pull_from_store(txn, left_store, key_hash, left_parent).await
}
