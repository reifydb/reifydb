use std::sync::Arc;

use reifydb_core::{CommitVersion, Row, value::column::Columns};
use reifydb_hash::Hash128;
use reifydb_sdk::FlowDiff;

use crate::{
	operator::{
		Operators,
		join::{JoinSide, JoinSideEntry, Store, operator::JoinOperator},
	},
	transaction::FlowTransaction,
};

/// Add a row to a state entry (left or right)
pub(crate) async fn add_to_state_entry(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	row: &Row,
) -> crate::Result<()> {
	let mut entry = store
		.get_or_insert_with(txn, key_hash, || JoinSideEntry {
			rows: Vec::new(),
		})
		.await?;
	entry.rows.push(row.number);
	store.set(txn, key_hash, &entry)?;
	Ok(())
}

/// Remove a row from state entry and cleanup if empty
pub(crate) async fn remove_from_state_entry(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	row: &Row,
) -> crate::Result<bool> {
	if let Some(mut entry) = store.get(txn, key_hash).await? {
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
pub(crate) async fn update_row_in_entry(
	txn: &mut FlowTransaction,
	store: &mut Store,
	key_hash: &Hash128,
	old_row: &Row,
	new_row: &Row,
) -> crate::Result<bool> {
	if let Some(mut entry) = store.get(txn, key_hash).await? {
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
pub(crate) async fn emit_joined_rows(
	txn: &mut FlowTransaction,
	primary_row: &Row,
	primary_side: JoinSide,
	opposite_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	opposite_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	let opposite_rows = pull_from_store(txn, opposite_store, key_hash, opposite_parent).await?;
	if opposite_rows.is_empty() {
		return Ok(Vec::new());
	}

	let joined_rows = match primary_side {
		JoinSide::Left => operator.join_rows_multiple_right(txn, primary_row, &opposite_rows).await?,
		JoinSide::Right => operator.join_rows_multiple_left(txn, &opposite_rows, primary_row).await?,
	};

	Ok(joined_rows
		.into_iter()
		.map(|post| FlowDiff::Insert {
			post: Columns::from_row(&post),
		})
		.collect())
}

/// Emit joined rows when inserting a left row that has right matches
pub(crate) async fn emit_joined_rows_left_to_right(
	txn: &mut FlowTransaction,
	left_row: &Row,
	right_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	right_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	emit_joined_rows(txn, left_row, JoinSide::Left, right_store, key_hash, operator, right_parent).await
}

/// Emit joined rows when inserting a right row that has left matches
pub(crate) async fn emit_joined_rows_right_to_left(
	txn: &mut FlowTransaction,
	right_row: &Row,
	left_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	left_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	emit_joined_rows(txn, right_row, JoinSide::Right, left_store, key_hash, operator, left_parent).await
}

/// Emit removal of all joined rows involving a row.
/// Unified function that handles both left and right removals.
pub(crate) async fn emit_remove_joined_rows(
	txn: &mut FlowTransaction,
	primary_row: &Row,
	primary_side: JoinSide,
	opposite_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	opposite_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	let opposite_rows = pull_from_store(txn, opposite_store, key_hash, opposite_parent).await?;
	if opposite_rows.is_empty() {
		return Ok(Vec::new());
	}

	let joined_rows = match primary_side {
		JoinSide::Left => operator.join_rows_multiple_right(txn, primary_row, &opposite_rows).await?,
		JoinSide::Right => operator.join_rows_multiple_left(txn, &opposite_rows, primary_row).await?,
	};

	Ok(joined_rows
		.into_iter()
		.map(|pre| FlowDiff::Remove {
			pre: Columns::from_row(&pre),
		})
		.collect())
}

/// Emit removal of all joined rows involving a left row
pub(crate) async fn emit_remove_joined_rows_left(
	txn: &mut FlowTransaction,
	left_row: &Row,
	right_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	right_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	emit_remove_joined_rows(txn, left_row, JoinSide::Left, right_store, key_hash, operator, right_parent).await
}

/// Emit removal of all joined rows involving a right row
pub(crate) async fn emit_remove_joined_rows_right(
	txn: &mut FlowTransaction,
	right_row: &Row,
	left_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	left_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	emit_remove_joined_rows(txn, right_row, JoinSide::Right, left_store, key_hash, operator, left_parent).await
}

/// Emit updates for all joined rows when a row is updated.
/// Unified function that handles both left and right updates.
pub(crate) async fn emit_update_joined_rows(
	txn: &mut FlowTransaction,
	old_row: &Row,
	new_row: &Row,
	primary_side: JoinSide,
	opposite_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	opposite_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	let opposite_rows = pull_from_store(txn, opposite_store, key_hash, opposite_parent).await?;
	if opposite_rows.is_empty() {
		return Ok(Vec::new());
	}

	let (pre_rows, post_rows) = match primary_side {
		JoinSide::Left => (
			operator.join_rows_multiple_right(txn, old_row, &opposite_rows).await?,
			operator.join_rows_multiple_right(txn, new_row, &opposite_rows).await?,
		),
		JoinSide::Right => (
			operator.join_rows_multiple_left(txn, &opposite_rows, old_row).await?,
			operator.join_rows_multiple_left(txn, &opposite_rows, new_row).await?,
		),
	};

	Ok(pre_rows
		.into_iter()
		.zip(post_rows)
		.map(|(pre, post)| FlowDiff::Update {
			pre: Columns::from_row(&pre),
			post: Columns::from_row(&post),
		})
		.collect())
}

/// Emit updates for all joined rows when a left row is updated
pub(crate) async fn emit_update_joined_rows_left(
	txn: &mut FlowTransaction,
	old_left_row: &Row,
	new_left_row: &Row,
	right_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	right_parent: &Arc<Operators>,
	_version: CommitVersion,
) -> crate::Result<Vec<FlowDiff>> {
	emit_update_joined_rows(
		txn,
		old_left_row,
		new_left_row,
		JoinSide::Left,
		right_store,
		key_hash,
		operator,
		right_parent,
	)
	.await
}

/// Emit updates for all joined rows when a right row is updated
pub(crate) async fn emit_update_joined_rows_right(
	txn: &mut FlowTransaction,
	old_right_row: &Row,
	new_right_row: &Row,
	left_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	left_parent: &Arc<Operators>,
	_version: CommitVersion,
) -> crate::Result<Vec<FlowDiff>> {
	emit_update_joined_rows(
		txn,
		old_right_row,
		new_right_row,
		JoinSide::Right,
		left_store,
		key_hash,
		operator,
		left_parent,
	)
	.await
}

/// Check if a right side has any rows for a given key
pub(crate) async fn has_right_rows(
	txn: &mut FlowTransaction,
	right_store: &Store,
	key_hash: &Hash128,
) -> crate::Result<bool> {
	Ok(right_store.contains_key(txn, key_hash).await?)
}

/// Check if it's the first right row being added for a key
pub(crate) async fn is_first_right_row(
	txn: &mut FlowTransaction,
	right_store: &Store,
	key_hash: &Hash128,
) -> crate::Result<bool> {
	Ok(!right_store.contains_key(txn, key_hash).await?)
}

/// Get all rows from a store for a given key (unified left/right helper)
pub(crate) async fn pull_from_store(
	txn: &mut FlowTransaction,
	store: &Store,
	key_hash: &Hash128,
	parent: &Arc<Operators>,
) -> crate::Result<Vec<Row>> {
	if let Some(entry) = store.get(txn, key_hash).await? {
		let columns = parent.pull(txn, &entry.rows).await?;
		// Convert Columns to Vec<Row>
		let mut rows = Vec::with_capacity(columns.row_count());
		for row_idx in 0..columns.row_count() {
			rows.push(columns.extract_row(row_idx).to_single_row());
		}
		Ok(rows)
	} else {
		Ok(Vec::new())
	}
}

/// Get all left rows for a given key
pub(crate) async fn pull_left_rows(
	txn: &mut FlowTransaction,
	left_store: &Store,
	key_hash: &Hash128,
	left_parent: &Arc<Operators>,
	_version: CommitVersion,
) -> crate::Result<Vec<Row>> {
	pull_from_store(txn, left_store, key_hash, left_parent).await
}

/// Get all right rows for a given key
pub(crate) async fn pull_right_rows(
	txn: &mut FlowTransaction,
	right_store: &Store,
	key_hash: &Hash128,
	right_parent: &Arc<Operators>,
	_version: CommitVersion,
) -> crate::Result<Vec<Row>> {
	pull_from_store(txn, right_store, key_hash, right_parent).await
}

/// Batch emit joined rows for multiple inserts with the same key.
/// Unified function that handles both left and right batch inserts.
pub(crate) async fn emit_joined_rows_multiple(
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

	let opposite_rows = pull_from_store(txn, opposite_store, key_hash, opposite_parent).await?;

	if opposite_rows.is_empty() {
		return Ok(Vec::new());
	}

	// join_rows_cartesian always takes (left_rows, right_rows) in that order
	let joined_rows = match primary_side {
		JoinSide::Left => operator.join_rows_cartesian(txn, primary_rows, &opposite_rows).await?,
		JoinSide::Right => operator.join_rows_cartesian(txn, &opposite_rows, primary_rows).await?,
	};

	Ok(joined_rows
		.into_iter()
		.map(|post| FlowDiff::Insert {
			post: Columns::from_row(&post),
		})
		.collect())
}

/// Batch emit joined rows for multiple left inserts with the same key
pub(crate) async fn emit_joined_rows_multiple_left(
	txn: &mut FlowTransaction,
	left_rows: &[Row],
	right_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	right_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	emit_joined_rows_multiple(txn, left_rows, JoinSide::Left, right_store, key_hash, operator, right_parent).await
}

/// Batch emit joined rows for multiple right inserts with the same key
pub(crate) async fn emit_joined_rows_multiple_right(
	txn: &mut FlowTransaction,
	right_rows: &[Row],
	left_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	left_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	emit_joined_rows_multiple(txn, right_rows, JoinSide::Right, left_store, key_hash, operator, left_parent).await
}

/// Batch emit removals for multiple removes with the same key.
/// Unified function that handles both left and right batch removals.
pub(crate) async fn emit_remove_joined_rows_multiple(
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

	let opposite_rows = pull_from_store(txn, opposite_store, key_hash, opposite_parent).await?;
	if opposite_rows.is_empty() {
		return Ok(Vec::new());
	}

	let joined_rows = match primary_side {
		JoinSide::Left => operator.join_rows_cartesian(txn, primary_rows, &opposite_rows).await?,
		JoinSide::Right => operator.join_rows_cartesian(txn, &opposite_rows, primary_rows).await?,
	};

	Ok(joined_rows
		.into_iter()
		.map(|pre| FlowDiff::Remove {
			pre: Columns::from_row(&pre),
		})
		.collect())
}

/// Batch emit removals for multiple left removes with the same key
pub(crate) async fn emit_remove_joined_rows_multiple_left(
	txn: &mut FlowTransaction,
	left_rows: &[Row],
	right_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	right_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	emit_remove_joined_rows_multiple(txn, left_rows, JoinSide::Left, right_store, key_hash, operator, right_parent)
		.await
}

/// Batch emit removals for multiple right removes with the same key
pub(crate) async fn emit_remove_joined_rows_multiple_right(
	txn: &mut FlowTransaction,
	right_rows: &[Row],
	left_store: &Store,
	key_hash: &Hash128,
	operator: &JoinOperator,
	left_parent: &Arc<Operators>,
) -> crate::Result<Vec<FlowDiff>> {
	emit_remove_joined_rows_multiple(txn, right_rows, JoinSide::Right, left_store, key_hash, operator, left_parent)
		.await
}
