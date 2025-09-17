// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion,
	interface::{PrimaryKeyDef, PrimaryKeyId},
};

use crate::materialized::{MaterializedCatalog, VersionedPrimaryKeyDef};

impl MaterializedCatalog {
	/// Find a primary key by ID at a specific version
	pub fn find_primary_key(&self, primary_key_id: PrimaryKeyId, version: CommitVersion) -> Option<PrimaryKeyDef> {
		self.primary_keys.get(&primary_key_id).and_then(|entry| {
			let versioned = entry.value();
			versioned.get(version)
		})
	}

	/// Set or update a primary key at a specific version
	pub fn set_primary_key(&self, id: PrimaryKeyId, version: CommitVersion, primary_key: Option<PrimaryKeyDef>) {
		// Update the versioned primary key
		let versioned = self.primary_keys.get_or_insert_with(id, VersionedPrimaryKeyDef::new);
		versioned.value().insert(version, primary_key);
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{ColumnDef, ColumnId, ColumnIndex, PrimaryKeyId};
	use reifydb_type::{Type, TypeConstraint};

	use super::*;
	use crate::MaterializedCatalog;

	fn create_test_primary_key(id: PrimaryKeyId) -> PrimaryKeyDef {
		PrimaryKeyDef {
			id,
			columns: vec![ColumnDef {
				id: ColumnId(1),
				name: "id".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Int4),
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: true,
			}],
		}
	}

	#[test]
	fn test_set_and_find_primary_key() {
		let catalog = MaterializedCatalog::new();
		let pk_id = PrimaryKeyId(1);
		let primary_key = create_test_primary_key(pk_id);

		// Set primary key at version 1
		catalog.set_primary_key(pk_id, 1, Some(primary_key.clone()));

		// Find primary key at version 1
		let found = catalog.find_primary_key(pk_id, 1);
		assert_eq!(found, Some(primary_key.clone()));

		// Find primary key at later version (should return same primary
		// key)
		let found = catalog.find_primary_key(pk_id, 5);
		assert_eq!(found, Some(primary_key));

		// Primary key shouldn't exist at version 0
		let found = catalog.find_primary_key(pk_id, 0);
		assert_eq!(found, None);
	}

	#[test]
	fn test_primary_key_update() {
		let catalog = MaterializedCatalog::new();
		let pk_id = PrimaryKeyId(1);

		// Create initial primary key with one column
		let pk_v1 = create_test_primary_key(pk_id);
		catalog.set_primary_key(pk_id, 1, Some(pk_v1.clone()));

		// Update primary key with two columns
		let mut pk_v2 = pk_v1.clone();
		pk_v2.columns.push(ColumnDef {
			id: ColumnId(2),
			name: "name".to_string(),
			constraint: TypeConstraint::unconstrained(Type::Utf8),
			policies: vec![],
			index: ColumnIndex(1),
			auto_increment: false,
		});
		catalog.set_primary_key(pk_id, 2, Some(pk_v2.clone()));

		// Version 1 should have one column
		assert_eq!(catalog.find_primary_key(pk_id, 1).unwrap().columns.len(), 1);

		// Version 2 should have two columns
		assert_eq!(catalog.find_primary_key(pk_id, 2).unwrap().columns.len(), 2);
	}

	#[test]
	fn test_primary_key_deletion() {
		let catalog = MaterializedCatalog::new();
		let pk_id = PrimaryKeyId(1);
		let primary_key = create_test_primary_key(pk_id);

		// Set primary key
		catalog.set_primary_key(pk_id, 1, Some(primary_key.clone()));

		// Verify it exists
		assert_eq!(catalog.find_primary_key(pk_id, 1), Some(primary_key.clone()));

		// Delete the primary key
		catalog.set_primary_key(pk_id, 2, None);

		// Should not exist at version 2
		assert_eq!(catalog.find_primary_key(pk_id, 2), None);

		// Should still exist at version 1 (historical)
		assert_eq!(catalog.find_primary_key(pk_id, 1), Some(primary_key));
	}

	#[test]
	fn test_primary_key_versioning() {
		let catalog = MaterializedCatalog::new();
		let pk_id = PrimaryKeyId(1);

		// Create multiple versions
		let pk_v1 = create_test_primary_key(pk_id);
		let mut pk_v2 = pk_v1.clone();
		pk_v2.columns[0].name = "pk_id".to_string();
		let mut pk_v3 = pk_v2.clone();
		pk_v3.columns[0].constraint = TypeConstraint::unconstrained(Type::Int8);

		// Set at different versions
		catalog.set_primary_key(pk_id, 10, Some(pk_v1.clone()));
		catalog.set_primary_key(pk_id, 20, Some(pk_v2.clone()));
		catalog.set_primary_key(pk_id, 30, Some(pk_v3.clone()));

		// Query at different versions
		assert_eq!(catalog.find_primary_key(pk_id, 5), None);
		assert_eq!(catalog.find_primary_key(pk_id, 10), Some(pk_v1.clone()));
		assert_eq!(catalog.find_primary_key(pk_id, 15), Some(pk_v1));
		assert_eq!(catalog.find_primary_key(pk_id, 20), Some(pk_v2.clone()));
		assert_eq!(catalog.find_primary_key(pk_id, 25), Some(pk_v2));
		assert_eq!(catalog.find_primary_key(pk_id, 30), Some(pk_v3.clone()));
		assert_eq!(catalog.find_primary_key(pk_id, 100), Some(pk_v3));
	}
}
