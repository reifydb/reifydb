use std::sync::Arc;

use reifydb_core::{CommitVersion, Row};
use reifydb_engine::StandardCommandTransaction;
use reifydb_hash::Hash128;

use crate::{
	flow::FlowDiff,
	operator::{
		Operators,
		join::{JoinSideEntry, Store, operator::JoinOperator},
	},
};

/// Add a encoded to a state entry (left or right)
pub(crate) fn add_to_state_entry(
	txn: &mut StandardCommandTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	row: &Row,
) -> crate::Result<()> {
	let mut entry = store.get_or_insert_with::<StandardCommandTransaction, _>(txn, key_hash, || JoinSideEntry {
		rows: Vec::new(),
	})?;
	entry.rows.push(row.number);
	store.set(txn, key_hash, &entry)?;
	Ok(())
}

/// Remove a encoded from state entry and cleanup if empty
pub(crate) fn remove_from_state_entry(
	txn: &mut StandardCommandTransaction,
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

/// Update a encoded in-place within a state entry
pub(crate) fn update_row_in_entry(
	txn: &mut StandardCommandTransaction,
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

/// Emit joined rows when inserting a left encoded that has right matches
pub(crate) fn emit_joined_rows_left_to_right(
	txn: &mut StandardCommandTransaction,
	left_row: &Row,
	right_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	right_parent: &Arc<Operators>,
	version: CommitVersion,
) -> crate::Result<Vec<FlowDiff>> {
	let mut result = Vec::new();

	if let Some(right_entry) = right_store.get(txn, key_hash)? {
		let right_rows = right_parent.get_rows(txn, &right_entry.rows, version)?;

		for right_row_opt in right_rows {
			if let Some(right_row) = right_row_opt {
				result.push(FlowDiff::Insert {
					post: operator.join_rows(txn, left_row, &right_row)?,
				});
			}
		}
	}

	Ok(result)
}

/// Emit joined rows when inserting a right encoded that has left matches
pub(crate) fn emit_joined_rows_right_to_left(
	txn: &mut StandardCommandTransaction,
	right_row: &Row,
	left_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	left_parent: &Arc<Operators>,
	version: CommitVersion,
) -> crate::Result<Vec<FlowDiff>> {
	let mut result = Vec::new();

	if let Some(left_entry) = left_store.get(txn, key_hash)? {
		let left_rows = left_parent.get_rows(txn, &left_entry.rows, version)?;

		for left_row_opt in left_rows {
			if let Some(left_row) = left_row_opt {
				result.push(FlowDiff::Insert {
					post: operator.join_rows(txn, &left_row, right_row)?,
				});
			}
		}
	}

	Ok(result)
}

/// Emit removal of all joined rows involving a left encoded
pub(crate) fn emit_remove_joined_rows_left(
	txn: &mut StandardCommandTransaction,
	left_row: &Row,
	right_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	right_parent: &Arc<Operators>,
	version: CommitVersion,
) -> crate::Result<Vec<FlowDiff>> {
	let mut result = Vec::new();

	if let Some(right_entry) = right_store.get(txn, key_hash)? {
		let right_rows = right_parent.get_rows(txn, &right_entry.rows, version)?;

		for right_row_opt in right_rows {
			if let Some(right_row) = right_row_opt {
				result.push(FlowDiff::Remove {
					pre: operator.join_rows(txn, left_row, &right_row)?,
				});
			}
		}
	}

	Ok(result)
}

/// Emit removal of all joined rows involving a right encoded
pub(crate) fn emit_remove_joined_rows_right(
	txn: &mut StandardCommandTransaction,
	right_row: &Row,
	left_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	left_parent: &Arc<Operators>,
	version: CommitVersion,
) -> crate::Result<Vec<FlowDiff>> {
	let mut result = Vec::new();

	if let Some(left_entry) = left_store.get(txn, key_hash)? {
		let left_rows = left_parent.get_rows(txn, &left_entry.rows, version)?;

		for left_row_opt in left_rows {
			if let Some(left_row) = left_row_opt {
				result.push(FlowDiff::Remove {
					pre: operator.join_rows(txn, &left_row, right_row)?,
				});
			}
		}
	}

	Ok(result)
}

/// Emit updates for all joined rows when a left encoded is updated
pub(crate) fn emit_update_joined_rows_left(
	txn: &mut StandardCommandTransaction,
	old_left_row: &Row,
	new_left_row: &Row,
	right_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	right_parent: &Arc<Operators>,
	version: CommitVersion,
) -> crate::Result<Vec<FlowDiff>> {
	let mut result = Vec::new();

	if let Some(right_entry) = right_store.get(txn, key_hash)? {
		let right_rows = right_parent.get_rows(txn, &right_entry.rows, version)?;

		for right_row_opt in right_rows {
			if let Some(right_row) = right_row_opt {
				result.push(FlowDiff::Update {
					pre: operator.join_rows(txn, old_left_row, &right_row)?,
					post: operator.join_rows(txn, new_left_row, &right_row)?,
				});
			}
		}
	}

	Ok(result)
}

/// Emit updates for all joined rows when a right encoded is updated
pub(crate) fn emit_update_joined_rows_right(
	txn: &mut StandardCommandTransaction,
	old_right_row: &Row,
	new_right_row: &Row,
	left_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	left_parent: &Arc<Operators>,
	version: CommitVersion,
) -> crate::Result<Vec<FlowDiff>> {
	let mut result = Vec::new();

	if let Some(left_entry) = left_store.get(txn, key_hash)? {
		let left_rows = left_parent.get_rows(txn, &left_entry.rows, version)?;

		for left_row_opt in left_rows {
			if let Some(left_row) = left_row_opt {
				result.push(FlowDiff::Update {
					pre: operator.join_rows(txn, &left_row, old_right_row)?,
					post: operator.join_rows(txn, &left_row, new_right_row)?,
				});
			}
		}
	}

	Ok(result)
}

/// Check if a right side has any rows for a given key
pub(crate) fn has_right_rows(
	txn: &mut StandardCommandTransaction,
	right_store: &Store,
	key_hash: &Hash128,
) -> crate::Result<bool> {
	Ok(right_store.contains_key(txn, key_hash)?)
}

/// Check if it's the first right encoded being added for a key
pub(crate) fn is_first_right_row(
	txn: &mut StandardCommandTransaction,
	right_store: &Store,
	key_hash: &Hash128,
) -> crate::Result<bool> {
	Ok(!right_store.contains_key(txn, key_hash)?)
}

/// Get all left rows for a given key
pub(crate) fn get_left_rows(
	txn: &mut StandardCommandTransaction,
	left_store: &Store,
	key_hash: &Hash128,
	left_parent: &Arc<Operators>,
	version: CommitVersion,
) -> crate::Result<Vec<Row>> {
	let mut rows = Vec::new();
	if let Some(left_entry) = left_store.get(txn, key_hash)? {
		let left_rows = left_parent.get_rows(txn, &left_entry.rows, version)?;

		for left_row_opt in left_rows {
			if let Some(left_row) = left_row_opt {
				rows.push(left_row);
			}
		}
	}
	Ok(rows)
}

/// Get all right rows for a given key
pub(crate) fn get_right_rows(
	txn: &mut StandardCommandTransaction,
	right_store: &Store,
	key_hash: &Hash128,
	right_parent: &Arc<Operators>,
	version: CommitVersion,
) -> crate::Result<Vec<Row>> {
	let mut rows = Vec::new();
	if let Some(right_entry) = right_store.get(txn, key_hash)? {
		let right_rows = right_parent.get_rows(txn, &right_entry.rows, version)?;

		for right_row_opt in right_rows {
			if let Some(right_row) = right_row_opt {
				rows.push(right_row);
			}
		}
	}
	Ok(rows)
}
