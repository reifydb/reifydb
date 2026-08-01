// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{id::NamespaceId, namespace::Namespace},
};

use crate::cache::{CatalogCache, MultiVersionNamespace};

impl CatalogCache {
	pub fn find_namespace_at(&self, namespace: NamespaceId, version: CommitVersion) -> Option<Namespace> {
		self.namespaces.get(&namespace).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_namespace_by_name_at(&self, namespace: &str, version: CommitVersion) -> Option<Namespace> {
		self.namespaces_by_name.get(namespace).and_then(|entry| {
			let namespace_id = *entry.value();
			self.find_namespace_at(namespace_id, version)
		})
	}

	pub fn find_namespace(&self, namespace: NamespaceId) -> Option<Namespace> {
		self.namespaces.get(&namespace).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn find_namespace_by_name(&self, namespace: &str) -> Option<Namespace> {
		self.namespaces_by_name.get(namespace).and_then(|entry| {
			let namespace_id = *entry.value();
			self.find_namespace(namespace_id)
		})
	}

	pub fn find_child_namespace_at(
		&self,
		parent_id: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Option<Namespace> {
		self.namespaces.iter().find_map(|entry| {
			let ns = entry.value().get(version)?;
			if ns.local_name() == name && ns.parent_id() == parent_id {
				Some(ns)
			} else {
				None
			}
		})
	}

	pub fn find_child_namespace(&self, parent_id: NamespaceId, name: &str) -> Option<Namespace> {
		self.namespaces.iter().find_map(|entry| {
			let ns = entry.value().get_latest()?;
			if ns.local_name() == name && ns.parent_id() == parent_id {
				Some(ns)
			} else {
				None
			}
		})
	}

	pub fn set_namespace(&self, id: NamespaceId, version: CommitVersion, namespace: Option<Namespace>) {
		let _guard = self.write_lock.lock();
		if let Some(entry) = self.namespaces.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			self.namespaces_by_name.remove(pre.name());
		}

		let multi = self.namespaces.get_or_insert_with(id, MultiVersionNamespace::new);
		if let Some(new) = namespace {
			self.namespaces_by_name.insert(new.name().to_string(), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn create_test_namespace(id: NamespaceId, name: &str) -> Namespace {
		let local_name = name.rsplit_once("::").map(|(_, s)| s).unwrap_or(name);
		Namespace::Local {
			id,
			name: name.to_string(),
			local_name: local_name.to_string(),
			parent_id: NamespaceId::ROOT,
		}
	}

	#[test]
	fn test_set_and_find_namespace() {
		let catalog = CatalogCache::new();
		let namespace_id = NamespaceId::SYSTEM;
		let namespace = create_test_namespace(namespace_id, "test_namespace");

		catalog.set_namespace(namespace_id, CommitVersion(1), Some(namespace.clone()));

		let found = catalog.find_namespace_at(namespace_id, CommitVersion(1));
		assert_eq!(found, Some(namespace.clone()));

		let found = catalog.find_namespace_at(namespace_id, CommitVersion(5));
		assert_eq!(found, Some(namespace));

		let found = catalog.find_namespace_at(namespace_id, CommitVersion(0));
		assert_eq!(found, None);
	}

	#[test]
	fn test_find_namespace_by_name() {
		let catalog = CatalogCache::new();
		let namespace_id = NamespaceId::SYSTEM;
		let namespace = create_test_namespace(namespace_id, "named_namespace");

		catalog.set_namespace(namespace_id, CommitVersion(1), Some(namespace.clone()));

		let found = catalog.find_namespace_by_name_at("named_namespace", CommitVersion(1));
		assert_eq!(found, Some(namespace));

		let found = catalog.find_namespace_by_name_at("wrong_name", CommitVersion(1));
		assert_eq!(found, None);
	}

	#[test]
	fn test_namespace_rename() {
		let catalog = CatalogCache::new();
		let namespace_id = NamespaceId::SYSTEM;

		let namespace_v1 = create_test_namespace(namespace_id, "old_name");
		catalog.set_namespace(namespace_id, CommitVersion(1), Some(namespace_v1.clone()));

		assert!(catalog.find_namespace_by_name_at("old_name", CommitVersion(1)).is_some());
		assert!(catalog.find_namespace_by_name_at("new_name", CommitVersion(1)).is_none());

		let namespace_v2 = create_test_namespace(namespace_id, "new_name");
		catalog.set_namespace(namespace_id, CommitVersion(2), Some(namespace_v2.clone()));

		assert!(catalog.find_namespace_by_name_at("old_name", CommitVersion(2)).is_none());

		assert_eq!(catalog.find_namespace_by_name_at("new_name", CommitVersion(2)), Some(namespace_v2.clone()));

		assert_eq!(catalog.find_namespace_at(namespace_id, CommitVersion(1)), Some(namespace_v1));

		assert_eq!(catalog.find_namespace_at(namespace_id, CommitVersion(2)), Some(namespace_v2));
	}

	#[test]
	fn test_namespace_deletion() {
		let catalog = CatalogCache::new();
		let namespace_id = NamespaceId::SYSTEM;

		let namespace = create_test_namespace(namespace_id, "deletable_namespace");
		catalog.set_namespace(namespace_id, CommitVersion(1), Some(namespace.clone()));

		assert_eq!(catalog.find_namespace_at(namespace_id, CommitVersion(1)), Some(namespace.clone()));
		assert!(catalog.find_namespace_by_name_at("deletable_namespace", CommitVersion(1)).is_some());

		catalog.set_namespace(namespace_id, CommitVersion(2), None);

		assert_eq!(catalog.find_namespace_at(namespace_id, CommitVersion(2)), None);
		assert!(catalog.find_namespace_by_name_at("deletable_namespace", CommitVersion(2)).is_none());

		assert_eq!(catalog.find_namespace_at(namespace_id, CommitVersion(1)), Some(namespace));
	}

	#[test]
	fn test_multiple_namespaces() {
		let catalog = CatalogCache::new();

		let namespace1 = create_test_namespace(NamespaceId::SYSTEM, "namespace1");
		let namespace2 = create_test_namespace(NamespaceId::DEFAULT, "namespace2");
		let namespace3 = create_test_namespace(NamespaceId(3), "namespace3");

		catalog.set_namespace(NamespaceId::SYSTEM, CommitVersion(1), Some(namespace1.clone()));
		catalog.set_namespace(NamespaceId::DEFAULT, CommitVersion(1), Some(namespace2.clone()));
		catalog.set_namespace(NamespaceId(3), CommitVersion(1), Some(namespace3.clone()));

		assert_eq!(catalog.find_namespace_by_name_at("namespace1", CommitVersion(1)), Some(namespace1));
		assert_eq!(catalog.find_namespace_by_name_at("namespace2", CommitVersion(1)), Some(namespace2));
		assert_eq!(catalog.find_namespace_by_name_at("namespace3", CommitVersion(1)), Some(namespace3));
	}

	#[test]
	fn test_namespace_versioning() {
		let catalog = CatalogCache::new();
		let namespace_id = NamespaceId(10);

		let namespace_v1 = create_test_namespace(namespace_id, "namespace_v1");
		let namespace_v2 = create_test_namespace(namespace_id, "namespace_v2");
		let namespace_v3 = create_test_namespace(namespace_id, "namespace_v3");

		catalog.set_namespace(namespace_id, CommitVersion(10), Some(namespace_v1.clone()));
		catalog.set_namespace(namespace_id, CommitVersion(20), Some(namespace_v2.clone()));
		catalog.set_namespace(namespace_id, CommitVersion(30), Some(namespace_v3.clone()));

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
		let catalog = CatalogCache::new();
		let namespace_id = NamespaceId(100);

		assert_eq!(catalog.find_namespace(namespace_id), None);

		let namespace_v1 = create_test_namespace(namespace_id, "namespace_v1");
		let namespace_v2 = create_test_namespace(namespace_id, "namespace_v2");

		catalog.set_namespace(namespace_id, CommitVersion(10), Some(namespace_v1));
		catalog.set_namespace(namespace_id, CommitVersion(20), Some(namespace_v2.clone()));

		assert_eq!(catalog.find_namespace(namespace_id), Some(namespace_v2));
	}

	#[test]
	fn test_find_latest_namespace_deleted() {
		let catalog = CatalogCache::new();
		let namespace_id = NamespaceId::SYSTEM;

		let namespace = create_test_namespace(namespace_id, "test_namespace");
		catalog.set_namespace(namespace_id, CommitVersion(10), Some(namespace));

		catalog.set_namespace(namespace_id, CommitVersion(20), None);

		assert_eq!(catalog.find_namespace(namespace_id), None);
	}

	#[test]
	fn test_find_latest_namespace_by_name() {
		let catalog = CatalogCache::new();
		let namespace_id = NamespaceId::SYSTEM;

		assert_eq!(catalog.find_namespace_by_name("test_namespace"), None);

		let namespace_v1 = create_test_namespace(namespace_id, "test_namespace");
		let namespace_v2 = create_test_namespace(namespace_id, "renamed_namespace");

		catalog.set_namespace(namespace_id, CommitVersion(10), Some(namespace_v1));
		catalog.set_namespace(namespace_id, CommitVersion(20), Some(namespace_v2.clone()));

		assert_eq!(catalog.find_namespace_by_name("test_namespace"), None);

		assert_eq!(catalog.find_namespace_by_name("renamed_namespace"), Some(namespace_v2));
	}
}
