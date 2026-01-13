// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{NamespaceTableKey, TableId, TableKey};
use reifydb_transaction::StandardCommandTransaction;

use crate::CatalogStore;

impl CatalogStore {
	pub fn delete_table(
		txn: &mut StandardCommandTransaction,
		table: TableId,
	) -> crate::Result<()> {
		// First, find the table to get its namespace
		if let Some(table_def) = Self::find_table(txn, table)? {
			// Delete the namespace-table link (secondary index)
			txn.remove(&NamespaceTableKey::encoded(table_def.namespace, table))?;
		}

		// Delete the table metadata
		txn.remove(&TableKey::encoded(table))?;

		// Note: Column deletion and other cleanup would require iterating through
		// and removing associated columns. For now, columns associated with the table
		// are orphaned when deleted.

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::Fragment;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, namespace::NamespaceToCreate, table::TableToCreate};

	#[test]
	fn test_delete_table() {
		let mut txn = create_test_command_transaction();

		// Create a namespace first
		let namespace = CatalogStore::create_namespace(
			&mut txn,
			NamespaceToCreate {
				namespace_fragment: Some(Fragment::internal("test_ns".to_string())),
				name: "test_ns".to_string(),
			},
		)
		.unwrap();

		// Create a table
		let created = CatalogStore::create_table(
			&mut txn,
			TableToCreate {
				fragment: Some(Fragment::internal("test_table".to_string())),
				table: "test_table".to_string(),
				namespace: namespace.id,
				columns: vec![],
				retention_policy: None,
			},
		)
		.unwrap();

		// Verify it exists
		let found = CatalogStore::find_table_by_name(&mut txn, namespace.id, "test_table").unwrap();
		assert!(found.is_some());

		// Delete it
		CatalogStore::delete_table(&mut txn, created.id).unwrap();

		// Verify it's gone
		let found = CatalogStore::find_table_by_name(&mut txn, namespace.id, "test_table").unwrap();
		assert!(found.is_none());
	}

	#[test]
	fn test_delete_nonexistent_table() {
		let mut txn = create_test_command_transaction();

		use reifydb_core::interface::TableId;
		// Deleting a non-existent table should not error
		let non_existent = TableId(999999);
		let result = CatalogStore::delete_table(&mut txn, non_existent);
		assert!(result.is_ok());
	}
}
