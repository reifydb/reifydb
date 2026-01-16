// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{id::PrimaryKeyId, key::PrimaryKeyDef},
};

use crate::materialized::{MaterializedCatalog, MultiVersionPrimaryKeyDef};

impl MaterializedCatalog {
	/// Find a primary key by ID at a specific version
	pub fn find_primary_key_at(
		&self,
		primary_key_id: PrimaryKeyId,
		version: CommitVersion,
	) -> Option<PrimaryKeyDef> {
		self.primary_keys.get(&primary_key_id).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find a primary key by ID (returns latest version)
	pub fn find_primary_key(&self, primary_key_id: PrimaryKeyId) -> Option<PrimaryKeyDef> {
		self.primary_keys.get(&primary_key_id).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	/// Set or update a primary key at a specific version
	pub fn set_primary_key(&self, id: PrimaryKeyId, version: CommitVersion, primary_key: Option<PrimaryKeyDef>) {
		// Update the multi primary key
		let multi = self.primary_keys.get_or_insert_with(id, MultiVersionPrimaryKeyDef::new);
		if let Some(new) = primary_key {
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{
		column::{ColumnDef, ColumnIndex},
		id::{ColumnId, PrimaryKeyId},
	};
	use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

	use super::*;
	use crate::materialized::MaterializedCatalog;

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
				dictionary_id: None,
			}],
		}
	}

	#[test]
	fn test_set_and_find_primary_key() {
		let catalog = MaterializedCatalog::new();
		let pk_id = PrimaryKeyId(1);
		let primary_key = create_test_primary_key(pk_id);

		// Set primary key at version 1
		catalog.set_primary_key(pk_id, CommitVersion(1), Some(primary_key.clone()));

		// Find primary key at version 1
		let found = catalog.find_primary_key_at(pk_id, CommitVersion(1));
		assert_eq!(found, Some(primary_key.clone()));

		// Find primary key at later version (should return same primary
		// key)
		let found = catalog.find_primary_key_at(pk_id, CommitVersion(5));
		assert_eq!(found, Some(primary_key));

		// Primary key shouldn't exist at version 0
		let found = catalog.find_primary_key_at(pk_id, CommitVersion(0));
		assert_eq!(found, None);
	}

	#[test]
	fn test_primary_key_update() {
		let catalog = MaterializedCatalog::new();
		let pk_id = PrimaryKeyId(1);

		// Create initial primary key with one column
		let pk_v1 = create_test_primary_key(pk_id);
		catalog.set_primary_key(pk_id, CommitVersion(1), Some(pk_v1.clone()));

		// Update primary key with two columns
		let mut pk_v2 = pk_v1.clone();
		pk_v2.columns.push(ColumnDef {
			id: ColumnId(2),
			name: "name".to_string(),
			constraint: TypeConstraint::unconstrained(Type::Utf8),
			policies: vec![],
			index: ColumnIndex(1),
			auto_increment: false,
			dictionary_id: None,
		});
		catalog.set_primary_key(pk_id, CommitVersion(2), Some(pk_v2.clone()));

		// Version 1 should have one column
		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(1)).unwrap().columns.len(), 1);

		// Version 2 should have two columns
		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(2)).unwrap().columns.len(), 2);
	}

	#[test]
	fn test_primary_key_deletion() {
		let catalog = MaterializedCatalog::new();
		let pk_id = PrimaryKeyId(1);
		let primary_key = create_test_primary_key(pk_id);

		// Set primary key
		catalog.set_primary_key(pk_id, CommitVersion(1), Some(primary_key.clone()));

		// Verify it exists
		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(1)), Some(primary_key.clone()));

		// Delete the primary key
		catalog.set_primary_key(pk_id, CommitVersion(2), None);

		// Should not exist at version 2
		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(2)), None);

		// Should still exist at version 1 (historical)
		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(1)), Some(primary_key));
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
		catalog.set_primary_key(pk_id, CommitVersion(10), Some(pk_v1.clone()));
		catalog.set_primary_key(pk_id, CommitVersion(20), Some(pk_v2.clone()));
		catalog.set_primary_key(pk_id, CommitVersion(30), Some(pk_v3.clone()));

		// Query at different versions
		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(5)), None);
		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(10)), Some(pk_v1.clone()));
		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(15)), Some(pk_v1));
		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(20)), Some(pk_v2.clone()));
		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(25)), Some(pk_v2));
		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(30)), Some(pk_v3.clone()));
		assert_eq!(catalog.find_primary_key_at(pk_id, CommitVersion(100)), Some(pk_v3));
	}
}
