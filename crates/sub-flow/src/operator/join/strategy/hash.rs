use std::sync::Arc;

use reifydb_core::{CommitVersion, Row};
use reifydb_flow_operator_sdk::FlowDiff;
use reifydb_hash::Hash128;

use crate::{
	operator::{
		Operators,
		join::{JoinSide, JoinSideEntry, Store, operator::JoinOperator},
	},
	transaction::FlowTransaction,
};

/// Add a row to a state entry (left or right)
pub(crate) fn add_to_state_entry(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	row: &Row,
) -> crate::Result<()> {
	let mut entry = store.get_or_insert_with(txn, key_hash, || JoinSideEntry {
		rows: Vec::new(),
	})?;
	entry.rows.push(row.number);
	store.set(txn, key_hash, &entry)?;
	Ok(())
}

/// Remove a row from state entry and cleanup if empty
pub(crate) fn remove_from_state_entry(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	row: &Row,
) -> crate::Result<bool> {
	if let Some(mut entry) = store.get(txn, key_hash)? {
		entry.rows.retain(|&r| r != row.number);

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
	old_row: &Row,
	new_row: &Row,
) -> crate::Result<bool> {
	if let Some(mut entry) = store.get(txn, key_hash)? {
		for row in &mut entry.rows {
			if *row == old_row.number {
				*row = new_row.number;
				store.set(txn, key_hash, &entry)?;
				return Ok(true);
			}
		}
	}
	Ok(false)
}

/// Emit joined rows when inserting a row that has matches on the opposite side.
/// Unified function that handles both left-to-right and right-to-left joins.
pub(crate) fn emit_joined_rows(
	txn: &mut FlowTransaction,
	primary_row: &Row,
	primary_side: JoinSide,
	opposite_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	opposite_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	let opposite_rows = get_rows_from_store(txn, opposite_store, key_hash, opposite_parent)?;
	if opposite_rows.is_empty() {
		return Ok(Vec::new());
	}

	let joined_rows = match primary_side {
		JoinSide::Left => operator.join_rows_batch(txn, primary_row, &opposite_rows)?,
		JoinSide::Right => operator.join_rows_batch_right(txn, &opposite_rows, primary_row)?,
	};

	Ok(joined_rows.into_iter().map(|post| FlowDiff::Insert { post }).collect())
}

/// Emit joined rows when inserting a left row that has right matches
pub(crate) fn emit_joined_rows_left_to_right(
	txn: &mut FlowTransaction,
	left_row: &Row,
	right_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	right_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	emit_joined_rows(txn, left_row, JoinSide::Left, right_store, key_hash, operator, right_parent)
}

/// Emit joined rows when inserting a right row that has left matches
pub(crate) fn emit_joined_rows_right_to_left(
	txn: &mut FlowTransaction,
	right_row: &Row,
	left_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	left_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	emit_joined_rows(txn, right_row, JoinSide::Right, left_store, key_hash, operator, left_parent)
}

/// Emit removal of all joined rows involving a row.
/// Unified function that handles both left and right removals.
pub(crate) fn emit_remove_joined_rows(
	txn: &mut FlowTransaction,
	primary_row: &Row,
	primary_side: JoinSide,
	opposite_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	opposite_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	let opposite_rows = get_rows_from_store(txn, opposite_store, key_hash, opposite_parent)?;
	if opposite_rows.is_empty() {
		return Ok(Vec::new());
	}

	let joined_rows = match primary_side {
		JoinSide::Left => operator.join_rows_batch(txn, primary_row, &opposite_rows)?,
		JoinSide::Right => operator.join_rows_batch_right(txn, &opposite_rows, primary_row)?,
	};

	Ok(joined_rows.into_iter().map(|pre| FlowDiff::Remove { pre }).collect())
}

/// Emit removal of all joined rows involving a left row
pub(crate) fn emit_remove_joined_rows_left(
	txn: &mut FlowTransaction,
	left_row: &Row,
	right_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	right_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	emit_remove_joined_rows(txn, left_row, JoinSide::Left, right_store, key_hash, operator, right_parent)
}

/// Emit removal of all joined rows involving a right row
pub(crate) fn emit_remove_joined_rows_right(
	txn: &mut FlowTransaction,
	right_row: &Row,
	left_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	left_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	emit_remove_joined_rows(txn, right_row, JoinSide::Right, left_store, key_hash, operator, left_parent)
}

/// Emit updates for all joined rows when a row is updated.
/// Unified function that handles both left and right updates.
pub(crate) fn emit_update_joined_rows(
	txn: &mut FlowTransaction,
	old_row: &Row,
	new_row: &Row,
	primary_side: JoinSide,
	opposite_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	opposite_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	let opposite_rows = get_rows_from_store(txn, opposite_store, key_hash, opposite_parent)?;
	if opposite_rows.is_empty() {
		return Ok(Vec::new());
	}

	let (pre_rows, post_rows) = match primary_side {
		JoinSide::Left => (
			operator.join_rows_batch(txn, old_row, &opposite_rows)?,
			operator.join_rows_batch(txn, new_row, &opposite_rows)?,
		),
		JoinSide::Right => (
			operator.join_rows_batch_right(txn, &opposite_rows, old_row)?,
			operator.join_rows_batch_right(txn, &opposite_rows, new_row)?,
		),
	};

	Ok(pre_rows.into_iter().zip(post_rows).map(|(pre, post)| FlowDiff::Update { pre, post }).collect())
}

/// Emit updates for all joined rows when a left row is updated
pub(crate) fn emit_update_joined_rows_left(
	txn: &mut FlowTransaction,
	old_left_row: &Row,
	new_left_row: &Row,
	right_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	right_parent: &Arc<Operators>,
	_version: CommitVersion,
) -> crate::Result<Vec<FlowDiff>> {
	emit_update_joined_rows(txn, old_left_row, new_left_row, JoinSide::Left, right_store, key_hash, operator, right_parent)
}

/// Emit updates for all joined rows when a right row is updated
pub(crate) fn emit_update_joined_rows_right(
	txn: &mut FlowTransaction,
	old_right_row: &Row,
	new_right_row: &Row,
	left_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	left_parent: &Arc<Operators>,
	_version: CommitVersion,
) -> crate::Result<Vec<FlowDiff>> {
	emit_update_joined_rows(txn, old_right_row, new_right_row, JoinSide::Right, left_store, key_hash, operator, left_parent)
}

/// Check if a right side has any rows for a given key
pub(crate) fn has_right_rows(
	txn: &mut FlowTransaction,
	right_store: &Store,
	key_hash: &Hash128,
) -> crate::Result<bool> {
	Ok(right_store.contains_key(txn, key_hash)?)
}

/// Check if it's the first right row being added for a key
pub(crate) fn is_first_right_row(
	txn: &mut FlowTransaction,
	right_store: &Store,
	key_hash: &Hash128,
) -> crate::Result<bool> {
	Ok(!right_store.contains_key(txn, key_hash)?)
}

/// Get all rows from a store for a given key (unified left/right helper)
pub(crate) fn get_rows_from_store(
	txn: &mut FlowTransaction,
	store: &Store,
	key_hash: &Hash128,
	parent: &Arc<Operators>,
) -> crate::Result<Vec<Row>> {
	if let Some(entry) = store.get(txn, key_hash)? {
		let row_opts = parent.get_rows(txn, &entry.rows)?;
		Ok(row_opts.into_iter().flatten().collect())
	} else {
		Ok(Vec::new())
	}
}

/// Get all left rows for a given key
pub(crate) fn get_left_rows(
	txn: &mut FlowTransaction,
	left_store: &Store,
	key_hash: &Hash128,
	left_parent: &Arc<Operators>,
	_version: CommitVersion,
) -> crate::Result<Vec<Row>> {
	get_rows_from_store(txn, left_store, key_hash, left_parent)
}

/// Get all right rows for a given key
pub(crate) fn get_right_rows(
	txn: &mut FlowTransaction,
	right_store: &Store,
	key_hash: &Hash128,
	right_parent: &Arc<Operators>,
	_version: CommitVersion,
) -> crate::Result<Vec<Row>> {
	get_rows_from_store(txn, right_store, key_hash, right_parent)
}


/// Batch emit joined rows for multiple inserts with the same key.
/// Unified function that handles both left and right batch inserts.
pub(crate) fn emit_joined_rows_batch(
	txn: &mut FlowTransaction,
	primary_rows: &[Row],
	primary_side: JoinSide,
	opposite_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	opposite_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	if primary_rows.is_empty() {
		return Ok(Vec::new());
	}

	let opposite_rows = get_rows_from_store(txn, opposite_store, key_hash, opposite_parent)?;
	if opposite_rows.is_empty() {
		return Ok(Vec::new());
	}

	// join_rows_batch_full always takes (left_rows, right_rows) in that order
	let joined_rows = match primary_side {
		JoinSide::Left => operator.join_rows_batch_full(txn, primary_rows, &opposite_rows)?,
		JoinSide::Right => operator.join_rows_batch_full(txn, &opposite_rows, primary_rows)?,
	};

	Ok(joined_rows.into_iter().map(|post| FlowDiff::Insert { post }).collect())
}

/// Batch emit joined rows for multiple left inserts with the same key
pub(crate) fn emit_joined_rows_batch_left(
	txn: &mut FlowTransaction,
	left_rows: &[Row],
	right_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	right_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	emit_joined_rows_batch(txn, left_rows, JoinSide::Left, right_store, key_hash, operator, right_parent)
}

/// Batch emit joined rows for multiple right inserts with the same key
pub(crate) fn emit_joined_rows_batch_right(
	txn: &mut FlowTransaction,
	right_rows: &[Row],
	left_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	left_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	emit_joined_rows_batch(txn, right_rows, JoinSide::Right, left_store, key_hash, operator, left_parent)
}

/// Batch emit removals for multiple removes with the same key.
/// Unified function that handles both left and right batch removals.
pub(crate) fn emit_remove_joined_rows_batch(
	txn: &mut FlowTransaction,
	primary_rows: &[Row],
	primary_side: JoinSide,
	opposite_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	opposite_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	if primary_rows.is_empty() {
		return Ok(Vec::new());
	}

	let opposite_rows = get_rows_from_store(txn, opposite_store, key_hash, opposite_parent)?;
	if opposite_rows.is_empty() {
		return Ok(Vec::new());
	}

	let joined_rows = match primary_side {
		JoinSide::Left => operator.join_rows_batch_full(txn, primary_rows, &opposite_rows)?,
		JoinSide::Right => operator.join_rows_batch_full(txn, &opposite_rows, primary_rows)?,
	};

	Ok(joined_rows.into_iter().map(|pre| FlowDiff::Remove { pre }).collect())
}

/// Batch emit removals for multiple left removes with the same key
pub(crate) fn emit_remove_joined_rows_batch_left(
	txn: &mut FlowTransaction,
	left_rows: &[Row],
	right_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	right_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	emit_remove_joined_rows_batch(txn, left_rows, JoinSide::Left, right_store, key_hash, operator, right_parent)
}

/// Batch emit removals for multiple right removes with the same key
pub(crate) fn emit_remove_joined_rows_batch_right(
	txn: &mut FlowTransaction,
	right_rows: &[Row],
	left_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	left_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	emit_remove_joined_rows_batch(txn, right_rows, JoinSide::Right, left_store, key_hash, operator, left_parent)
}
