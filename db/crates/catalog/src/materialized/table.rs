// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion,
	interface::{NamespaceId, TableDef, TableId},
};

use crate::materialized::{MaterializedCatalog, VersionedTableDef};

impl MaterializedCatalog {
	/// Find a table by ID at a specific version
	pub fn find_table(&self, table: TableId, version: CommitVersion) -> Option<TableDef> {
		self.tables.get(&table).and_then(|entry| {
			let versioned = entry.value();
			versioned.get(version)
		})
	}

	/// Find a table by name in a namespace at a specific version
	pub fn find_table_by_name(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Option<TableDef> {
		self.tables_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let table_id = *entry.value();
			self.find_table(table_id, version)
		})
	}

	pub fn set_table(&self, id: TableId, version: CommitVersion, table: Option<TableDef>) {
		// Look up the current table to update the index
		if let Some(entry) = self.tables.get(&id) {
			if let Some(pre) = entry.value().get_latest() {
				// Remove old name from index
				self.tables_by_name.remove(&(pre.namespace, pre.name.clone()));
			}
		}

		// Add new name to index if setting a new value
		if let Some(ref new) = table {
			self.tables_by_name.insert((new.namespace, new.name.clone()), id);
		}

		// Update the versioned table
		let versioned = self.tables.get_or_insert_with(id, VersionedTableDef::new);
		versioned.value().insert(version, table);
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{ColumnDef, ColumnId, ColumnIndex};
	use reifydb_type::{Type, TypeConstraint};

	use super::*;

	fn create_test_table(id: TableId, namespace: NamespaceId, name: &str) -> TableDef {
		TableDef {
			id,
			namespace,
			name: name.to_string(),
			columns: vec![
				ColumnDef {
					id: ColumnId(1),
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Int4),
					policies: vec![],
					index: ColumnIndex(0),
					auto_increment: true,
				},
				ColumnDef {
					id: ColumnId(2),
					name: "name".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
				},
			],
			primary_key: None,
		}
	}

	#[test]
	fn test_set_and_find_table() {
		let catalog = MaterializedCatalog::new();
		let table_id = TableId(1);
		let namespace_id = NamespaceId(1);
		let table = create_test_table(table_id, namespace_id, "test_table");

		// Set table at version 1
		catalog.set_table(table_id, 1, Some(table.clone()));

		// Find table at version 1
		let found = catalog.find_table(table_id, 1);
		assert_eq!(found, Some(table.clone()));

		// Find table at later version (should return same table)
		let found = catalog.find_table(table_id, 5);
		assert_eq!(found, Some(table));

		// Table shouldn't exist at version 0
		let found = catalog.find_table(table_id, 0);
		assert_eq!(found, None);
	}

	#[test]
	fn test_find_table_by_name() {
		let catalog = MaterializedCatalog::new();
		let table_id = TableId(1);
		let namespace_id = NamespaceId(1);
		let table = create_test_table(table_id, namespace_id, "named_table");

		// Set table
		catalog.set_table(table_id, 1, Some(table.clone()));

		// Find by name
		let found = catalog.find_table_by_name(namespace_id, "named_table", 1);
		assert_eq!(found, Some(table));

		// Shouldn't find with wrong name
		let found = catalog.find_table_by_name(namespace_id, "wrong_name", 1);
		assert_eq!(found, None);

		// Shouldn't find in wrong namespace
		let found = catalog.find_table_by_name(NamespaceId(2), "named_table", 1);
		assert_eq!(found, None);
	}

	#[test]
	fn test_table_rename() {
		let catalog = MaterializedCatalog::new();
		let table_id = TableId(1);
		let namespace_id = NamespaceId(1);

		// Create and set initial table
		let table_v1 = create_test_table(table_id, namespace_id, "old_name");
		catalog.set_table(table_id, 1, Some(table_v1.clone()));

		// Verify initial state
		assert!(catalog.find_table_by_name(namespace_id, "old_name", 1).is_some());
		assert!(catalog.find_table_by_name(namespace_id, "new_name", 1).is_none());

		// Rename the table
		let mut table_v2 = table_v1.clone();
		table_v2.name = "new_name".to_string();
		catalog.set_table(table_id, 2, Some(table_v2.clone()));

		// Old name should be gone
		assert!(catalog.find_table_by_name(namespace_id, "old_name", 2).is_none());

		// New name can be found
		assert_eq!(catalog.find_table_by_name(namespace_id, "new_name", 2), Some(table_v2.clone()));

		// Historical query at version 1 should still show old name
		assert_eq!(catalog.find_table(table_id, 1), Some(table_v1));

		// Current version should show new name
		assert_eq!(catalog.find_table(table_id, 2), Some(table_v2));
	}

	#[test]
	fn test_table_move_between_namespaces() {
		let catalog = MaterializedCatalog::new();
		let table_id = TableId(1);
		let namespace1 = NamespaceId(1);
		let namespace2 = NamespaceId(2);

		// Create table in namespace1
		let table_v1 = create_test_table(table_id, namespace1, "movable_table");
		catalog.set_table(table_id, 1, Some(table_v1.clone()));

		// Verify it's in namespace1
		assert!(catalog.find_table_by_name(namespace1, "movable_table", 1).is_some());
		assert!(catalog.find_table_by_name(namespace2, "movable_table", 1).is_none());

		// Move to namespace2
		let mut table_v2 = table_v1.clone();
		table_v2.namespace = namespace2;
		catalog.set_table(table_id, 2, Some(table_v2.clone()));

		// Should no longer be in namespace1
		assert!(catalog.find_table_by_name(namespace1, "movable_table", 2).is_none());

		// Should now be in namespace2
		assert!(catalog.find_table_by_name(namespace2, "movable_table", 2).is_some());
	}

	#[test]
	fn test_table_deletion() {
		let catalog = MaterializedCatalog::new();
		let table_id = TableId(1);
		let namespace_id = NamespaceId(1);

		// Create and set table
		let table = create_test_table(table_id, namespace_id, "deletable_table");
		catalog.set_table(table_id, 1, Some(table.clone()));

		// Verify it exists
		assert_eq!(catalog.find_table(table_id, 1), Some(table.clone()));
		assert!(catalog.find_table_by_name(namespace_id, "deletable_table", 1).is_some());

		// Delete the table
		catalog.set_table(table_id, 2, None);

		// Should not exist at version 2
		assert_eq!(catalog.find_table(table_id, 2), None);
		assert!(catalog.find_table_by_name(namespace_id, "deletable_table", 2).is_none());

		// Should still exist at version 1 (historical)
		assert_eq!(catalog.find_table(table_id, 1), Some(table));
	}

	#[test]
	fn test_multiple_tables_in_namespace() {
		let catalog = MaterializedCatalog::new();
		let namespace_id = NamespaceId(1);

		let table1 = create_test_table(TableId(1), namespace_id, "table1");
		let table2 = create_test_table(TableId(2), namespace_id, "table2");
		let table3 = create_test_table(TableId(3), namespace_id, "table3");

		// Set multiple tables
		catalog.set_table(TableId(1), 1, Some(table1.clone()));
		catalog.set_table(TableId(2), 1, Some(table2.clone()));
		catalog.set_table(TableId(3), 1, Some(table3.clone()));

		// All should be findable
		assert_eq!(catalog.find_table_by_name(namespace_id, "table1", 1), Some(table1));
		assert_eq!(catalog.find_table_by_name(namespace_id, "table2", 1), Some(table2));
		assert_eq!(catalog.find_table_by_name(namespace_id, "table3", 1), Some(table3));
	}

	#[test]
	fn test_table_versioning() {
		let catalog = MaterializedCatalog::new();
		let table_id = TableId(1);
		let namespace_id = NamespaceId(1);

		// Create multiple versions
		let table_v1 = create_test_table(table_id, namespace_id, "table_v1");
		let mut table_v2 = table_v1.clone();
		table_v2.name = "table_v2".to_string();
		let mut table_v3 = table_v2.clone();
		table_v3.name = "table_v3".to_string();

		// Set at different versions
		catalog.set_table(table_id, 10, Some(table_v1.clone()));
		catalog.set_table(table_id, 20, Some(table_v2.clone()));
		catalog.set_table(table_id, 30, Some(table_v3.clone()));

		// Query at different versions
		assert_eq!(catalog.find_table(table_id, 5), None);
		assert_eq!(catalog.find_table(table_id, 10), Some(table_v1.clone()));
		assert_eq!(catalog.find_table(table_id, 15), Some(table_v1));
		assert_eq!(catalog.find_table(table_id, 20), Some(table_v2.clone()));
		assert_eq!(catalog.find_table(table_id, 25), Some(table_v2));
		assert_eq!(catalog.find_table(table_id, 30), Some(table_v3.clone()));
		assert_eq!(catalog.find_table(table_id, 100), Some(table_v3));
	}
}
