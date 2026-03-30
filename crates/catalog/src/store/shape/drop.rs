// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{ColumnId, PrimaryKeyId},
		shape::ShapeId,
	},
	key::{
		column::ColumnKey, column_sequence::ColumnSequenceKey, columns::ColumnsKey, primary_key::PrimaryKeyKey,
		property::ColumnPropertyKey, retention_policy::ShapeRetentionPolicyKey, row_sequence::RowSequenceKey,
	},
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{Result, store::column::shape::primitive_column};

/// Remove all metadata associated with a shape (table, view, or ringbuffer):
/// columns, column policies, column sequences, column definitions, primary key,
/// row sequence, and retention policy.
///
/// Physical row data (RowKey) is NOT deleted here.
pub(crate) fn drop_shape_metadata(
	txn: &mut AdminTransaction,
	shape: ShapeId,
	pk_id: Option<PrimaryKeyId>,
) -> Result<()> {
	// Step 1: Scan all columns for this shape
	let range = ColumnKey::full_scan(shape);
	let mut stream = txn.range(range, 1024)?;
	let mut col_entries = Vec::new();
	for entry in stream.by_ref() {
		let entry = entry?;
		let col_id = primitive_column::SHAPE.get_u64(&entry.row, primitive_column::ID);
		col_entries.push((entry.key.clone(), ColumnId(col_id)));
	}
	drop(stream);

	// Step 2: For each column, clean up policies, sequence, and definition
	for (col_key, col_id) in &col_entries {
		// Delete column policies
		let policy_range = ColumnPropertyKey::full_scan(*col_id);
		let mut policy_stream = txn.range(policy_range, 1024)?;
		let mut policy_keys = Vec::new();
		for entry in policy_stream.by_ref() {
			policy_keys.push(entry?.key.clone());
		}
		drop(policy_stream);
		for pk in policy_keys {
			txn.remove(&pk)?;
		}

		// Delete column sequence
		txn.remove(&ColumnSequenceKey::encoded(shape, *col_id))?;

		// Delete column definition
		txn.remove(&ColumnsKey::encoded(*col_id))?;

		// Delete shape-column link
		txn.remove(col_key)?;
	}

	// Step 3: Delete primary key if present
	if let Some(pk_id) = pk_id {
		txn.remove(&PrimaryKeyKey::encoded(pk_id))?;
	}

	// Step 4: Delete row sequence
	txn.remove(&RowSequenceKey::encoded(shape))?;

	// Step 5: Delete retention policy
	txn.remove(&ShapeRetentionPolicyKey::encoded(shape))?;

	Ok(())
}
