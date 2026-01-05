// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	CommitVersion,
	interface::{NamespaceId, RingBufferDef, RingBufferId},
};

use crate::materialized::{MaterializedCatalog, MultiVersionRingBufferDef};

impl MaterializedCatalog {
	/// Find a ringbuffer by ID at a specific version
	pub fn find_ringbuffer_at(&self, ringbuffer: RingBufferId, version: CommitVersion) -> Option<RingBufferDef> {
		self.ringbuffers.get(&ringbuffer).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find a ringbuffer by name in a namespace at a specific version
	pub fn find_ringbuffer_by_name_at(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Option<RingBufferDef> {
		self.ringbuffers_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let ringbuffer_id = *entry.value();
			self.find_ringbuffer_at(ringbuffer_id, version)
		})
	}

	/// Find a ringbuffer by ID (returns latest version)
	pub fn find_ringbuffer(&self, ringbuffer: RingBufferId) -> Option<RingBufferDef> {
		self.ringbuffers.get(&ringbuffer).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	/// Find a ringbuffer by name in a namespace (returns latest version)
	pub fn find_ringbuffer_by_name(&self, namespace: NamespaceId, name: &str) -> Option<RingBufferDef> {
		self.ringbuffers_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let ringbuffer_id = *entry.value();
			self.find_ringbuffer(ringbuffer_id)
		})
	}

	pub fn set_ringbuffer(&self, id: RingBufferId, version: CommitVersion, ringbuffer: Option<RingBufferDef>) {
		// Look up the current ringbuffer to update the index
		if let Some(entry) = self.ringbuffers.get(&id) {
			if let Some(pre) = entry.value().get_latest() {
				self.ringbuffers_by_name.remove(&(pre.namespace, pre.name.clone()));
			}
		}

		let multi = self.ringbuffers.get_or_insert_with(id, MultiVersionRingBufferDef::new);
		if let Some(new) = ringbuffer {
			self.ringbuffers_by_name.insert((new.namespace, new.name.clone()), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{ColumnDef, ColumnId, ColumnIndex};
	use reifydb_type::{Type, TypeConstraint};

	use super::*;

	fn create_test_ringbuffer(id: RingBufferId, namespace: NamespaceId, name: &str) -> RingBufferDef {
		RingBufferDef {
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
					dictionary_id: None,
				},
				ColumnDef {
					id: ColumnId(2),
					name: "data".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
			],
			capacity: 1000,
			primary_key: None,
		}
	}

	#[test]
	fn test_set_and_find_ringbuffer() {
		let catalog = MaterializedCatalog::new();
		let rb_id = RingBufferId(1);
		let namespace_id = NamespaceId(1);
		let ringbuffer = create_test_ringbuffer(rb_id, namespace_id, "test_rb");

		// Set ringbuffer at version 1
		catalog.set_ringbuffer(rb_id, CommitVersion(1), Some(ringbuffer.clone()));

		// Find ringbuffer at version 1
		let found = catalog.find_ringbuffer_at(rb_id, CommitVersion(1));
		assert_eq!(found, Some(ringbuffer.clone()));

		// Find ringbuffer at later version (should return same ringbuffer)
		let found = catalog.find_ringbuffer_at(rb_id, CommitVersion(5));
		assert_eq!(found, Some(ringbuffer));

		// RingBuffer shouldn't exist at version 0
		let found = catalog.find_ringbuffer_at(rb_id, CommitVersion(0));
		assert_eq!(found, None);
	}

	#[test]
	fn test_find_ringbuffer_by_name() {
		let catalog = MaterializedCatalog::new();
		let rb_id = RingBufferId(1);
		let namespace_id = NamespaceId(1);
		let ringbuffer = create_test_ringbuffer(rb_id, namespace_id, "named_rb");

		// Set ringbuffer
		catalog.set_ringbuffer(rb_id, CommitVersion(1), Some(ringbuffer.clone()));

		// Find by name
		let found = catalog.find_ringbuffer_by_name_at(namespace_id, "named_rb", CommitVersion(1));
		assert_eq!(found, Some(ringbuffer));

		// Shouldn't find with wrong name
		let found = catalog.find_ringbuffer_by_name_at(namespace_id, "wrong_name", CommitVersion(1));
		assert_eq!(found, None);

		// Shouldn't find in wrong namespace
		let found = catalog.find_ringbuffer_by_name_at(NamespaceId(2), "named_rb", CommitVersion(1));
		assert_eq!(found, None);
	}

	#[test]
	fn test_ringbuffer_rename() {
		let catalog = MaterializedCatalog::new();
		let rb_id = RingBufferId(1);
		let namespace_id = NamespaceId(1);

		// Create and set initial ringbuffer
		let rb_v1 = create_test_ringbuffer(rb_id, namespace_id, "old_name");
		catalog.set_ringbuffer(rb_id, CommitVersion(1), Some(rb_v1.clone()));

		// Verify initial state
		assert!(catalog.find_ringbuffer_by_name_at(namespace_id, "old_name", CommitVersion(1)).is_some());
		assert!(catalog.find_ringbuffer_by_name_at(namespace_id, "new_name", CommitVersion(1)).is_none());

		// Rename the ringbuffer
		let mut rb_v2 = rb_v1.clone();
		rb_v2.name = "new_name".to_string();
		catalog.set_ringbuffer(rb_id, CommitVersion(2), Some(rb_v2.clone()));

		// Old name should be gone
		assert!(catalog.find_ringbuffer_by_name_at(namespace_id, "old_name", CommitVersion(2)).is_none());

		// New name can be found
		assert_eq!(
			catalog.find_ringbuffer_by_name_at(namespace_id, "new_name", CommitVersion(2)),
			Some(rb_v2.clone())
		);

		// Historical query at version 1 should still show old name
		assert_eq!(catalog.find_ringbuffer_at(rb_id, CommitVersion(1)), Some(rb_v1));

		// Current version should show new name
		assert_eq!(catalog.find_ringbuffer_at(rb_id, CommitVersion(2)), Some(rb_v2));
	}

	#[test]
	fn test_ringbuffer_deletion() {
		let catalog = MaterializedCatalog::new();
		let rb_id = RingBufferId(1);
		let namespace_id = NamespaceId(1);

		// Create and set ringbuffer
		let ringbuffer = create_test_ringbuffer(rb_id, namespace_id, "deletable_rb");
		catalog.set_ringbuffer(rb_id, CommitVersion(1), Some(ringbuffer.clone()));

		// Verify it exists
		assert_eq!(catalog.find_ringbuffer_at(rb_id, CommitVersion(1)), Some(ringbuffer.clone()));
		assert!(catalog.find_ringbuffer_by_name_at(namespace_id, "deletable_rb", CommitVersion(1)).is_some());

		// Delete the ringbuffer
		catalog.set_ringbuffer(rb_id, CommitVersion(2), None);

		// Should not exist at version 2
		assert_eq!(catalog.find_ringbuffer_at(rb_id, CommitVersion(2)), None);
		assert!(catalog.find_ringbuffer_by_name_at(namespace_id, "deletable_rb", CommitVersion(2)).is_none());

		// Should still exist at version 1 (historical)
		assert_eq!(catalog.find_ringbuffer_at(rb_id, CommitVersion(1)), Some(ringbuffer));
	}

	#[test]
	fn test_find_latest_ringbuffer() {
		let catalog = MaterializedCatalog::new();
		let rb_id = RingBufferId(1);
		let namespace_id = NamespaceId(1);

		// Empty catalog should return None
		assert_eq!(catalog.find_ringbuffer(rb_id), None);

		// Create multiple versions
		let rb_v1 = create_test_ringbuffer(rb_id, namespace_id, "rb_v1");
		let mut rb_v2 = rb_v1.clone();
		rb_v2.name = "rb_v2".to_string();

		catalog.set_ringbuffer(rb_id, CommitVersion(10), Some(rb_v1));
		catalog.set_ringbuffer(rb_id, CommitVersion(20), Some(rb_v2.clone()));

		// Should return latest (v2)
		assert_eq!(catalog.find_ringbuffer(rb_id), Some(rb_v2));
	}

	#[test]
	fn test_find_latest_ringbuffer_by_name() {
		let catalog = MaterializedCatalog::new();
		let namespace_id = NamespaceId(1);
		let rb_id = RingBufferId(1);

		// Empty catalog should return None
		assert_eq!(catalog.find_ringbuffer_by_name(namespace_id, "test_rb"), None);

		// Create ringbuffer
		let rb_v1 = create_test_ringbuffer(rb_id, namespace_id, "test_rb");
		let mut rb_v2 = rb_v1.clone();
		rb_v2.name = "renamed_rb".to_string();

		catalog.set_ringbuffer(rb_id, CommitVersion(10), Some(rb_v1));
		catalog.set_ringbuffer(rb_id, CommitVersion(20), Some(rb_v2.clone()));

		// Old name should not be found
		assert_eq!(catalog.find_ringbuffer_by_name(namespace_id, "test_rb"), None);

		// New name should be found with latest version
		assert_eq!(catalog.find_ringbuffer_by_name(namespace_id, "renamed_rb"), Some(rb_v2));
	}
}
