// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{NamespaceId, SinkId},
		sink::Sink,
	},
};

use crate::materialized::{MaterializedCatalog, MultiVersionSink};

impl MaterializedCatalog {
	/// Find a sink by ID at a specific version
	pub fn find_sink_at(&self, sink: SinkId, version: CommitVersion) -> Option<Sink> {
		self.sinks.get(&sink).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	/// Find a sink by name in a namespace at a specific version
	pub fn find_sink_by_name_at(&self, namespace: NamespaceId, name: &str, version: CommitVersion) -> Option<Sink> {
		self.sinks_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let sink_id = *entry.value();
			self.find_sink_at(sink_id, version)
		})
	}

	/// Find a sink by ID (returns latest version)
	pub fn find_sink(&self, sink: SinkId) -> Option<Sink> {
		self.sinks.get(&sink).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	/// Find a sink by name in a namespace (returns latest version)
	pub fn find_sink_by_name(&self, namespace: NamespaceId, name: &str) -> Option<Sink> {
		self.sinks_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let sink_id = *entry.value();
			self.find_sink(sink_id)
		})
	}

	pub fn set_sink(&self, id: SinkId, version: CommitVersion, sink: Option<Sink>) {
		// Look up the current sink to update the index
		if let Some(entry) = self.sinks.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			self.sinks_by_name.remove(&(pre.namespace, pre.name.clone()));
		}

		let multi = self.sinks.get_or_insert_with(id, MultiVersionSink::new);
		if let Some(new) = sink {
			self.sinks_by_name.insert((new.namespace, new.name.clone()), id);
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

	fn create_test_sink(id: SinkId, namespace: NamespaceId, name: &str) -> Sink {
		Sink {
			id,
			namespace,
			name: name.to_string(),
			source_namespace: namespace,
			source_name: "source_table".to_string(),
			connector: "test_connector".to_string(),
			config: vec![],
			status: FlowStatus::Active,
		}
	}

	#[test]
	fn test_set_and_find_sink() {
		let catalog = MaterializedCatalog::new();
		let sink_id = SinkId(1);
		let namespace_id = NamespaceId::SYSTEM;
		let sink = create_test_sink(sink_id, namespace_id, "test_sink");

		// Set sink at version 1
		catalog.set_sink(sink_id, CommitVersion(1), Some(sink.clone()));

		// Find sink at version 1
		let found = catalog.find_sink_at(sink_id, CommitVersion(1));
		assert_eq!(found, Some(sink.clone()));

		// Find sink at later version (should return same sink)
		let found = catalog.find_sink_at(sink_id, CommitVersion(5));
		assert_eq!(found, Some(sink));

		// Sink shouldn't exist at version 0
		let found = catalog.find_sink_at(sink_id, CommitVersion(0));
		assert_eq!(found, None);
	}

	#[test]
	fn test_find_sink_by_name() {
		let catalog = MaterializedCatalog::new();
		let sink_id = SinkId(1);
		let namespace_id = NamespaceId::SYSTEM;
		let sink = create_test_sink(sink_id, namespace_id, "named_sink");

		// Set sink
		catalog.set_sink(sink_id, CommitVersion(1), Some(sink.clone()));

		// Find by name
		let found = catalog.find_sink_by_name_at(namespace_id, "named_sink", CommitVersion(1));
		assert_eq!(found, Some(sink));

		// Shouldn't find with wrong name
		let found = catalog.find_sink_by_name_at(namespace_id, "wrong_name", CommitVersion(1));
		assert_eq!(found, None);

		// Shouldn't find in wrong namespace
		let found = catalog.find_sink_by_name_at(NamespaceId::DEFAULT, "named_sink", CommitVersion(1));
		assert_eq!(found, None);
	}

	#[test]
	fn test_sink_deletion() {
		let catalog = MaterializedCatalog::new();
		let sink_id = SinkId(1);
		let namespace_id = NamespaceId::SYSTEM;

		// Create and set sink
		let sink = create_test_sink(sink_id, namespace_id, "deletable_sink");
		catalog.set_sink(sink_id, CommitVersion(1), Some(sink.clone()));

		// Verify it exists
		assert_eq!(catalog.find_sink_at(sink_id, CommitVersion(1)), Some(sink.clone()));
		assert!(catalog.find_sink_by_name_at(namespace_id, "deletable_sink", CommitVersion(1)).is_some());

		// Delete the sink
		catalog.set_sink(sink_id, CommitVersion(2), None);

		// Should not exist at version 2
		assert_eq!(catalog.find_sink_at(sink_id, CommitVersion(2)), None);
		assert!(catalog.find_sink_by_name_at(namespace_id, "deletable_sink", CommitVersion(2)).is_none());

		// Should still exist at version 1 (historical)
		assert_eq!(catalog.find_sink_at(sink_id, CommitVersion(1)), Some(sink));
	}

	#[test]
	fn test_multiple_sinks_in_namespace() {
		let catalog = MaterializedCatalog::new();
		let namespace_id = NamespaceId::SYSTEM;

		let sink1 = create_test_sink(SinkId(1), namespace_id, "sink1");
		let sink2 = create_test_sink(SinkId(2), namespace_id, "sink2");
		let sink3 = create_test_sink(SinkId(3), namespace_id, "sink3");

		// Set multiple sinks
		catalog.set_sink(SinkId(1), CommitVersion(1), Some(sink1.clone()));
		catalog.set_sink(SinkId(2), CommitVersion(1), Some(sink2.clone()));
		catalog.set_sink(SinkId(3), CommitVersion(1), Some(sink3.clone()));

		// All should be findable
		assert_eq!(catalog.find_sink_by_name_at(namespace_id, "sink1", CommitVersion(1)), Some(sink1));
		assert_eq!(catalog.find_sink_by_name_at(namespace_id, "sink2", CommitVersion(1)), Some(sink2));
		assert_eq!(catalog.find_sink_by_name_at(namespace_id, "sink3", CommitVersion(1)), Some(sink3));
	}

	#[test]
	fn test_sink_versioning() {
		let catalog = MaterializedCatalog::new();
		let sink_id = SinkId(1);
		let namespace_id = NamespaceId::SYSTEM;

		// Create multiple versions
		let sink_v1 = create_test_sink(sink_id, namespace_id, "sink_v1");
		let mut sink_v2 = sink_v1.clone();
		sink_v2.name = "sink_v2".to_string();
		let mut sink_v3 = sink_v2.clone();
		sink_v3.name = "sink_v3".to_string();

		// Set at different versions
		catalog.set_sink(sink_id, CommitVersion(10), Some(sink_v1.clone()));
		catalog.set_sink(sink_id, CommitVersion(20), Some(sink_v2.clone()));
		catalog.set_sink(sink_id, CommitVersion(30), Some(sink_v3.clone()));

		// Query at different versions
		assert_eq!(catalog.find_sink_at(sink_id, CommitVersion(5)), None);
		assert_eq!(catalog.find_sink_at(sink_id, CommitVersion(10)), Some(sink_v1.clone()));
		assert_eq!(catalog.find_sink_at(sink_id, CommitVersion(15)), Some(sink_v1));
		assert_eq!(catalog.find_sink_at(sink_id, CommitVersion(20)), Some(sink_v2.clone()));
		assert_eq!(catalog.find_sink_at(sink_id, CommitVersion(25)), Some(sink_v2));
		assert_eq!(catalog.find_sink_at(sink_id, CommitVersion(30)), Some(sink_v3.clone()));
		assert_eq!(catalog.find_sink_at(sink_id, CommitVersion(100)), Some(sink_v3));
	}
}
