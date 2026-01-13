// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{NamespaceId, NamespaceKey};
use reifydb_transaction::StandardCommandTransaction;

use crate::CatalogStore;

impl CatalogStore {
	pub fn delete_namespace(
		txn: &mut StandardCommandTransaction,
		namespace: NamespaceId,
	) -> crate::Result<()> {
		// Delete the namespace metadata
		txn.remove(&NamespaceKey::encoded(namespace))?;

		// Note: Tables and other objects within the namespace should be deleted first
		// or will be orphaned. A more complete implementation would cascade delete.

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::Fragment;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, namespace::NamespaceToCreate};

	#[test]
	fn test_delete_namespace() {
		let mut txn = create_test_command_transaction();

		let created = CatalogStore::create_namespace(
			&mut txn,
			NamespaceToCreate {
				namespace_fragment: Some(Fragment::internal("test_ns".to_string())),
				name: "test_ns".to_string(),
			},
		)
		.unwrap();

		// Verify it exists
		let found = CatalogStore::find_namespace_by_name(&mut txn, "test_ns").unwrap();
		assert!(found.is_some());

		// Delete it
		CatalogStore::delete_namespace(&mut txn, created.id).unwrap();

		// Verify it's gone
		let found = CatalogStore::find_namespace_by_name(&mut txn, "test_ns").unwrap();
		assert!(found.is_none());
	}

	#[test]
	fn test_delete_nonexistent_namespace() {
		let mut txn = create_test_command_transaction();

		use reifydb_core::interface::NamespaceId;
		// Deleting a non-existent namespace should not error
		let non_existent = NamespaceId(999999);
		let result = CatalogStore::delete_namespace(&mut txn, non_existent);
		assert!(result.is_ok());
	}
}
