// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{NamespaceId, SourceId},
		source::Source,
	},
};

use crate::cache::{CatalogCache, MultiVersionSource};

impl CatalogCache {
	pub fn find_source_at(&self, source: SourceId, version: CommitVersion) -> Option<Source> {
		self.sources.get(&source).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_source_by_name_at(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Option<Source> {
		self.sources_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let shape_id = *entry.value();
			self.find_source_at(shape_id, version)
		})
	}

	pub fn find_source(&self, source: SourceId) -> Option<Source> {
		self.sources.get(&source).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn find_source_by_name(&self, namespace: NamespaceId, name: &str) -> Option<Source> {
		self.sources_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let shape_id = *entry.value();
			self.find_source(shape_id)
		})
	}

	pub fn set_source(&self, id: SourceId, version: CommitVersion, source: Option<Source>) {
		if let Some(entry) = self.sources.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			self.sources_by_name.remove(&(pre.namespace, pre.name.clone()));
		}

		let multi = self.sources.get_or_insert_with(id, MultiVersionSource::new);
		if let Some(new) = source {
			self.sources_by_name.insert((new.namespace, new.name.clone()), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::flow::FlowStatus;

	use super::*;

	fn create_test_source(id: SourceId, namespace: NamespaceId, name: &str) -> Source {
		Source {
			id,
			namespace,
			name: name.to_string(),
			connector: "test_connector".to_string(),
			config: vec![],
			target_namespace: namespace,
			target_name: "target".to_string(),
			status: FlowStatus::Active,
		}
	}

	#[test]
	fn test_set_and_find_source() {
		let catalog = CatalogCache::new();
		let shape_id = SourceId(1);
		let namespace_id = NamespaceId::SYSTEM;
		let source = create_test_source(shape_id, namespace_id, "test_source");

		// Set source at version 1
		catalog.set_source(shape_id, CommitVersion(1), Some(source.clone()));

		// Find source at version 1
		let found = catalog.find_source_at(shape_id, CommitVersion(1));
		assert_eq!(found, Some(source.clone()));

		// Find source at later version (should return same source)
		let found = catalog.find_source_at(shape_id, CommitVersion(5));
		assert_eq!(found, Some(source));

		// Source shouldn't exist at version 0
		let found = catalog.find_source_at(shape_id, CommitVersion(0));
		assert_eq!(found, None);
	}

	#[test]
	fn test_find_source_by_name() {
		let catalog = CatalogCache::new();
		let shape_id = SourceId(1);
		let namespace_id = NamespaceId::SYSTEM;
		let source = create_test_source(shape_id, namespace_id, "named_source");

		// Set source
		catalog.set_source(shape_id, CommitVersion(1), Some(source.clone()));

		// Find by name
		let found = catalog.find_source_by_name_at(namespace_id, "named_source", CommitVersion(1));
		assert_eq!(found, Some(source));

		// Shouldn't find with wrong name
		let found = catalog.find_source_by_name_at(namespace_id, "wrong_name", CommitVersion(1));
		assert_eq!(found, None);

		// Shouldn't find in wrong namespace
		let found = catalog.find_source_by_name_at(NamespaceId::DEFAULT, "named_source", CommitVersion(1));
		assert_eq!(found, None);
	}

	#[test]
	fn test_source_deletion() {
		let catalog = CatalogCache::new();
		let shape_id = SourceId(1);
		let namespace_id = NamespaceId::SYSTEM;

		// Create and set source
		let source = create_test_source(shape_id, namespace_id, "deletable_source");
		catalog.set_source(shape_id, CommitVersion(1), Some(source.clone()));

		// Verify it exists
		assert_eq!(catalog.find_source_at(shape_id, CommitVersion(1)), Some(source.clone()));
		assert!(catalog.find_source_by_name_at(namespace_id, "deletable_source", CommitVersion(1)).is_some());

		// Delete the source
		catalog.set_source(shape_id, CommitVersion(2), None);

		// Should not exist at version 2
		assert_eq!(catalog.find_source_at(shape_id, CommitVersion(2)), None);
		assert!(catalog.find_source_by_name_at(namespace_id, "deletable_source", CommitVersion(2)).is_none());

		// Should still exist at version 1 (historical)
		assert_eq!(catalog.find_source_at(shape_id, CommitVersion(1)), Some(source));
	}

	#[test]
	fn test_multiple_sources_in_namespace() {
		let catalog = CatalogCache::new();
		let namespace_id = NamespaceId::SYSTEM;

		let source1 = create_test_source(SourceId(1), namespace_id, "source1");
		let source2 = create_test_source(SourceId(2), namespace_id, "source2");
		let source3 = create_test_source(SourceId(3), namespace_id, "source3");

		// Set multiple sources
		catalog.set_source(SourceId(1), CommitVersion(1), Some(source1.clone()));
		catalog.set_source(SourceId(2), CommitVersion(1), Some(source2.clone()));
		catalog.set_source(SourceId(3), CommitVersion(1), Some(source3.clone()));

		// All should be findable
		assert_eq!(catalog.find_source_by_name_at(namespace_id, "source1", CommitVersion(1)), Some(source1));
		assert_eq!(catalog.find_source_by_name_at(namespace_id, "source2", CommitVersion(1)), Some(source2));
		assert_eq!(catalog.find_source_by_name_at(namespace_id, "source3", CommitVersion(1)), Some(source3));
	}

	#[test]
	fn test_source_versioning() {
		let catalog = CatalogCache::new();
		let shape_id = SourceId(1);
		let namespace_id = NamespaceId::SYSTEM;

		// Create multiple versions
		let source_v1 = create_test_source(shape_id, namespace_id, "source_v1");
		let mut source_v2 = source_v1.clone();
		source_v2.name = "source_v2".to_string();
		let mut source_v3 = source_v2.clone();
		source_v3.name = "source_v3".to_string();

		// Set at different versions
		catalog.set_source(shape_id, CommitVersion(10), Some(source_v1.clone()));
		catalog.set_source(shape_id, CommitVersion(20), Some(source_v2.clone()));
		catalog.set_source(shape_id, CommitVersion(30), Some(source_v3.clone()));

		// Query at different versions
		assert_eq!(catalog.find_source_at(shape_id, CommitVersion(5)), None);
		assert_eq!(catalog.find_source_at(shape_id, CommitVersion(10)), Some(source_v1.clone()));
		assert_eq!(catalog.find_source_at(shape_id, CommitVersion(15)), Some(source_v1));
		assert_eq!(catalog.find_source_at(shape_id, CommitVersion(20)), Some(source_v2.clone()));
		assert_eq!(catalog.find_source_at(shape_id, CommitVersion(25)), Some(source_v2));
		assert_eq!(catalog.find_source_at(shape_id, CommitVersion(30)), Some(source_v3.clone()));
		assert_eq!(catalog.find_source_at(shape_id, CommitVersion(100)), Some(source_v3));
	}
}
