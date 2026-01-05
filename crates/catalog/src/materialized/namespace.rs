// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	CommitVersion,
	interface::{NamespaceDef, NamespaceId},
};

use crate::materialized::{MaterializedCatalog, MultiVersionNamespaceDef};

impl MaterializedCatalog {
	/// Find a namespace by ID at a specific version
	pub fn find_namespace_at(&self, namespace: NamespaceId, version: CommitVersion) -> Option<NamespaceDef> {
		self.namespaces.get(&namespace).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find a namespace by name at a specific version
	pub fn find_namespace_by_name_at(&self, namespace: &str, version: CommitVersion) -> Option<NamespaceDef> {
		self.namespaces_by_name.get(namespace).and_then(|entry| {
			let namespace_id = *entry.value();
			self.find_namespace_at(namespace_id, version)
		})
	}

	/// Find a namespace by ID (returns latest version)
	pub fn find_namespace(&self, namespace: NamespaceId) -> Option<NamespaceDef> {
		self.namespaces.get(&namespace).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	/// Find a namespace by name (returns latest version)
	pub fn find_namespace_by_name(&self, namespace: &str) -> Option<NamespaceDef> {
		self.namespaces_by_name.get(namespace).and_then(|entry| {
			let namespace_id = *entry.value();
			self.find_namespace(namespace_id)
		})
	}

	pub fn set_namespace(&self, id: NamespaceId, version: CommitVersion, namespace: Option<NamespaceDef>) {
		// Look up the current namespace to update the index
		if let Some(entry) = self.namespaces.get(&id) {
			if let Some(pre) = entry.value().get_latest() {
				// Remove old name from index
				self.namespaces_by_name.remove(&pre.name);
			}
		}

		let multi = self.namespaces.get_or_insert_with(id, MultiVersionNamespaceDef::new);
		if let Some(new) = namespace {
			self.namespaces_by_name.insert(new.name.clone(), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn create_test_namespace(id: NamespaceId, name: &str) -> NamespaceDef {
		NamespaceDef {
			id,
			name: name.to_string(),
		}
	}

	#[test]
	fn test_set_and_find_namespace() {
		let catalog = MaterializedCatalog::new();
		let namespace_id = NamespaceId(1);
		let namespace = create_test_namespace(namespace_id, "test_namespace");

		// Set namespace at version 1
		catalog.set_namespace(namespace_id, CommitVersion(1), Some(namespace.clone()));

		// Find namespace at version 1
		let found = catalog.find_namespace_at(namespace_id, CommitVersion(1));
		assert_eq!(found, Some(namespace.clone()));

		// Find namespace at later version (should return same
		// namespace)
		let found = catalog.find_namespace_at(namespace_id, CommitVersion(5));
		assert_eq!(found, Some(namespace));

		// Namespace shouldn't exist at version 0
		let found = catalog.find_namespace_at(namespace_id, CommitVersion(0));
		assert_eq!(found, None);
	}

	#[test]
	fn test_find_namespace_by_name() {
		let catalog = MaterializedCatalog::new();
		let namespace_id = NamespaceId(1);
		let namespace = create_test_namespace(namespace_id, "named_namespace");

		// Set namespace
		catalog.set_namespace(namespace_id, CommitVersion(1), Some(namespace.clone()));

		// Find by name
		let found = catalog.find_namespace_by_name_at("named_namespace", CommitVersion(1));
		assert_eq!(found, Some(namespace));

		// Shouldn't find with wrong name
		let found = catalog.find_namespace_by_name_at("wrong_name", CommitVersion(1));
		assert_eq!(found, None);
	}

	#[test]
	fn test_namespace_rename() {
		let catalog = MaterializedCatalog::new();
		let namespace_id = NamespaceId(1);

		// Create and set initial namespace
		let namespace_v1 = create_test_namespace(namespace_id, "old_name");
		catalog.set_namespace(namespace_id, CommitVersion(1), Some(namespace_v1.clone()));

		// Verify initial state
		assert!(catalog.find_namespace_by_name_at("old_name", CommitVersion(1)).is_some());
		assert!(catalog.find_namespace_by_name_at("new_name", CommitVersion(1)).is_none());

		// Rename the namespace
		let mut namespace_v2 = namespace_v1.clone();
		namespace_v2.name = "new_name".to_string();
		catalog.set_namespace(namespace_id, CommitVersion(2), Some(namespace_v2.clone()));

		// Old name should be gone
		assert!(catalog.find_namespace_by_name_at("old_name", CommitVersion(2)).is_none());

		// New name can be found
		assert_eq!(catalog.find_namespace_by_name_at("new_name", CommitVersion(2)), Some(namespace_v2.clone()));

		// Historical query at version 1 should still show old name
		assert_eq!(catalog.find_namespace_at(namespace_id, CommitVersion(1)), Some(namespace_v1));

		// Current version should show new name
		assert_eq!(catalog.find_namespace_at(namespace_id, CommitVersion(2)), Some(namespace_v2));
	}

	#[test]
	fn test_namespace_deletion() {
		let catalog = MaterializedCatalog::new();
		let namespace_id = NamespaceId(1);

		// Create and set namespace
		let namespace = create_test_namespace(namespace_id, "deletable_namespace");
		catalog.set_namespace(namespace_id, CommitVersion(1), Some(namespace.clone()));

		// Verify it exists
		assert_eq!(catalog.find_namespace_at(namespace_id, CommitVersion(1)), Some(namespace.clone()));
		assert!(catalog.find_namespace_by_name_at("deletable_namespace", CommitVersion(1)).is_some());

		// Delete the namespace
		catalog.set_namespace(namespace_id, CommitVersion(2), None);

		// Should not exist at version 2
		assert_eq!(catalog.find_namespace_at(namespace_id, CommitVersion(2)), None);
		assert!(catalog.find_namespace_by_name_at("deletable_namespace", CommitVersion(2)).is_none());

		// Should still exist at version 1 (historical)
		assert_eq!(catalog.find_namespace_at(namespace_id, CommitVersion(1)), Some(namespace));
	}

	#[test]
	fn test_multiple_namespaces() {
		let catalog = MaterializedCatalog::new();

		let namespace1 = create_test_namespace(NamespaceId(1), "namespace1");
		let namespace2 = create_test_namespace(NamespaceId(2), "namespace2");
		let namespace3 = create_test_namespace(NamespaceId(3), "namespace3");

		// Set multiple namespaces
		catalog.set_namespace(NamespaceId(1), CommitVersion(1), Some(namespace1.clone()));
		catalog.set_namespace(NamespaceId(2), CommitVersion(1), Some(namespace2.clone()));
		catalog.set_namespace(NamespaceId(3), CommitVersion(1), Some(namespace3.clone()));

		// All should be findable
		assert_eq!(catalog.find_namespace_by_name_at("namespace1", CommitVersion(1)), Some(namespace1));
		assert_eq!(catalog.find_namespace_by_name_at("namespace2", CommitVersion(1)), Some(namespace2));
		assert_eq!(catalog.find_namespace_by_name_at("namespace3", CommitVersion(1)), Some(namespace3));
	}

	#[test]
	fn test_namespace_versioning() {
		let catalog = MaterializedCatalog::new();
		let namespace_id = NamespaceId(2);

		// Create multiple versions
		let namespace_v1 = create_test_namespace(namespace_id, "namespace_v1");
		let mut namespace_v2 = namespace_v1.clone();
		namespace_v2.name = "namespace_v2".to_string();
		let mut namespace_v3 = namespace_v2.clone();
		namespace_v3.name = "namespace_v3".to_string();

		// Set at different versions
		catalog.set_namespace(namespace_id, CommitVersion(10), Some(namespace_v1.clone()));
		catalog.set_namespace(namespace_id, CommitVersion(20), Some(namespace_v2.clone()));
		catalog.set_namespace(namespace_id, CommitVersion(30), Some(namespace_v3.clone()));

		// Query at different versions
		assert_eq!(catalog.find_namespace_at(namespace_id, CommitVersion(5)), None);
		assert_eq!(catalog.find_namespace_at(namespace_id, CommitVersion(10)), Some(namespace_v1.clone()));
		assert_eq!(catalog.find_namespace_at(namespace_id, CommitVersion(15)), Some(namespace_v1));
		assert_eq!(catalog.find_namespace_at(namespace_id, CommitVersion(20)), Some(namespace_v2.clone()));
		assert_eq!(catalog.find_namespace_at(namespace_id, CommitVersion(25)), Some(namespace_v2));
		assert_eq!(catalog.find_namespace_at(namespace_id, CommitVersion(30)), Some(namespace_v3.clone()));
		assert_eq!(catalog.find_namespace_at(namespace_id, CommitVersion(100)), Some(namespace_v3));
	}

	#[test]
	fn test_find_latest_namespace() {
		let catalog = MaterializedCatalog::new();
		let namespace_id = NamespaceId(100);

		// Empty catalog should return None
		assert_eq!(catalog.find_namespace(namespace_id), None);

		// Create multiple versions
		let namespace_v1 = create_test_namespace(namespace_id, "namespace_v1");
		let mut namespace_v2 = namespace_v1.clone();
		namespace_v2.name = "namespace_v2".to_string();

		catalog.set_namespace(namespace_id, CommitVersion(10), Some(namespace_v1));
		catalog.set_namespace(namespace_id, CommitVersion(20), Some(namespace_v2.clone()));

		// Should return latest (v2)
		assert_eq!(catalog.find_namespace(namespace_id), Some(namespace_v2));
	}

	#[test]
	fn test_find_latest_namespace_deleted() {
		let catalog = MaterializedCatalog::new();
		let namespace_id = NamespaceId(1);

		let namespace = create_test_namespace(namespace_id, "test_namespace");
		catalog.set_namespace(namespace_id, CommitVersion(10), Some(namespace));

		// Delete at latest version
		catalog.set_namespace(namespace_id, CommitVersion(20), None);

		// Should return None (deleted at latest)
		assert_eq!(catalog.find_namespace(namespace_id), None);
	}

	#[test]
	fn test_find_latest_namespace_by_name() {
		let catalog = MaterializedCatalog::new();
		let namespace_id = NamespaceId(1);

		// Empty catalog should return None
		assert_eq!(catalog.find_namespace_by_name("test_namespace"), None);

		// Create namespace
		let namespace_v1 = create_test_namespace(namespace_id, "test_namespace");
		let mut namespace_v2 = namespace_v1.clone();
		namespace_v2.name = "renamed_namespace".to_string();

		catalog.set_namespace(namespace_id, CommitVersion(10), Some(namespace_v1));
		catalog.set_namespace(namespace_id, CommitVersion(20), Some(namespace_v2.clone()));

		// Old name should not be found
		assert_eq!(catalog.find_namespace_by_name("test_namespace"), None);

		// New name should be found with latest version
		assert_eq!(catalog.find_namespace_by_name("renamed_namespace"), Some(namespace_v2));
	}
}
