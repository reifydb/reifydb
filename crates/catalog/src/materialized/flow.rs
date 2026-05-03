// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		flow::{Flow, FlowId},
		id::NamespaceId,
	},
};

use crate::materialized::{MaterializedCatalog, MultiVersionFlow};

impl MaterializedCatalog {
	pub fn find_flow_at(&self, flow: FlowId, version: CommitVersion) -> Option<Flow> {
		self.flows.get(&flow).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_flow_by_name_at(&self, namespace: NamespaceId, name: &str, version: CommitVersion) -> Option<Flow> {
		self.flows_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let flow_id = *entry.value();
			self.find_flow_at(flow_id, version)
		})
	}

	pub fn find_flow(&self, flow: FlowId) -> Option<Flow> {
		self.flows.get(&flow).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn find_flow_by_name(&self, namespace: NamespaceId, name: &str) -> Option<Flow> {
		self.flows_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let flow_id = *entry.value();
			self.find_flow(flow_id)
		})
	}

	pub fn set_flow(&self, id: FlowId, version: CommitVersion, flow: Option<Flow>) {
		if let Some(entry) = self.flows.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			self.flows_by_name.remove(&(pre.namespace, pre.name.clone()));
		}

		let multi = self.flows.get_or_insert_with(id, MultiVersionFlow::new);
		if let Some(new) = flow {
			self.flows_by_name.insert((new.namespace, new.name.clone()), id);
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

	fn create_test_flow(id: FlowId, namespace: NamespaceId, name: &str) -> Flow {
		Flow {
			id,
			namespace,
			name: name.to_string(),
			status: FlowStatus::Active,
			tick: None,
		}
	}

	#[test]
	fn test_set_and_find_flow() {
		let catalog = MaterializedCatalog::new();
		let flow_id = FlowId(1);
		let namespace_id = NamespaceId::SYSTEM;
		let flow = create_test_flow(flow_id, namespace_id, "test_flow");

		// Set flow at version 1
		catalog.set_flow(flow_id, CommitVersion(1), Some(flow.clone()));

		// Find flow at version 1
		let found = catalog.find_flow_at(flow_id, CommitVersion(1));
		assert_eq!(found, Some(flow.clone()));

		// Find flow at later version (should return same flow)
		let found = catalog.find_flow_at(flow_id, CommitVersion(5));
		assert_eq!(found, Some(flow));

		// Flow shouldn't exist at version 0
		let found = catalog.find_flow_at(flow_id, CommitVersion(0));
		assert_eq!(found, None);
	}

	#[test]
	fn test_find_flow_by_name() {
		let catalog = MaterializedCatalog::new();
		let flow_id = FlowId(1);
		let namespace_id = NamespaceId::SYSTEM;
		let flow = create_test_flow(flow_id, namespace_id, "named_flow");

		// Set flow
		catalog.set_flow(flow_id, CommitVersion(1), Some(flow.clone()));

		// Find by name
		let found = catalog.find_flow_by_name_at(namespace_id, "named_flow", CommitVersion(1));
		assert_eq!(found, Some(flow));

		// Shouldn't find with wrong name
		let found = catalog.find_flow_by_name_at(namespace_id, "wrong_name", CommitVersion(1));
		assert_eq!(found, None);

		// Shouldn't find in wrong namespace
		let found = catalog.find_flow_by_name_at(NamespaceId::DEFAULT, "named_flow", CommitVersion(1));
		assert_eq!(found, None);
	}

	#[test]
	fn test_flow_rename() {
		let catalog = MaterializedCatalog::new();
		let flow_id = FlowId(1);
		let namespace_id = NamespaceId::SYSTEM;

		// Create and set initial flow
		let flow_v1 = create_test_flow(flow_id, namespace_id, "old_name");
		catalog.set_flow(flow_id, CommitVersion(1), Some(flow_v1.clone()));

		// Verify initial state
		assert!(catalog.find_flow_by_name_at(namespace_id, "old_name", CommitVersion(1)).is_some());
		assert!(catalog.find_flow_by_name_at(namespace_id, "new_name", CommitVersion(1)).is_none());

		// Rename the flow
		let mut flow_v2 = flow_v1.clone();
		flow_v2.name = "new_name".to_string();
		catalog.set_flow(flow_id, CommitVersion(2), Some(flow_v2.clone()));

		// Old name should be gone
		assert!(catalog.find_flow_by_name_at(namespace_id, "old_name", CommitVersion(2)).is_none());

		// New name can be found
		assert_eq!(
			catalog.find_flow_by_name_at(namespace_id, "new_name", CommitVersion(2)),
			Some(flow_v2.clone())
		);

		// Historical query at version 1 should still show old name
		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(1)), Some(flow_v1));

		// Current version should show new name
		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(2)), Some(flow_v2));
	}

	#[test]
	fn test_flow_move_between_namespaces() {
		let catalog = MaterializedCatalog::new();
		let flow_id = FlowId(1);
		let namespace1 = NamespaceId::SYSTEM;
		let namespace2 = NamespaceId::DEFAULT;

		// Create flow in namespace1
		let flow_v1 = create_test_flow(flow_id, namespace1, "movable_flow");
		catalog.set_flow(flow_id, CommitVersion(1), Some(flow_v1.clone()));

		// Verify it's in namespace1
		assert!(catalog.find_flow_by_name_at(namespace1, "movable_flow", CommitVersion(1)).is_some());
		assert!(catalog.find_flow_by_name_at(namespace2, "movable_flow", CommitVersion(1)).is_none());

		// Move to namespace2
		let mut flow_v2 = flow_v1.clone();
		flow_v2.namespace = namespace2;
		catalog.set_flow(flow_id, CommitVersion(2), Some(flow_v2.clone()));

		// Should no longer be in namespace1
		assert!(catalog.find_flow_by_name_at(namespace1, "movable_flow", CommitVersion(2)).is_none());

		// Should now be in namespace2
		assert!(catalog.find_flow_by_name_at(namespace2, "movable_flow", CommitVersion(2)).is_some());
	}

	#[test]
	fn test_flow_deletion() {
		let catalog = MaterializedCatalog::new();
		let flow_id = FlowId(1);
		let namespace_id = NamespaceId::SYSTEM;

		// Create and set flow
		let flow = create_test_flow(flow_id, namespace_id, "deletable_flow");
		catalog.set_flow(flow_id, CommitVersion(1), Some(flow.clone()));

		// Verify it exists
		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(1)), Some(flow.clone()));
		assert!(catalog.find_flow_by_name_at(namespace_id, "deletable_flow", CommitVersion(1)).is_some());

		// Delete the flow
		catalog.set_flow(flow_id, CommitVersion(2), None);

		// Should not exist at version 2
		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(2)), None);
		assert!(catalog.find_flow_by_name_at(namespace_id, "deletable_flow", CommitVersion(2)).is_none());

		// Should still exist at version 1 (historical)
		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(1)), Some(flow));
	}

	#[test]
	fn test_multiple_flows_in_namespace() {
		let catalog = MaterializedCatalog::new();
		let namespace_id = NamespaceId::SYSTEM;

		let flow1 = create_test_flow(FlowId(1), namespace_id, "flow1");
		let flow2 = create_test_flow(FlowId(2), namespace_id, "flow2");
		let flow3 = create_test_flow(FlowId(3), namespace_id, "flow3");

		// Set multiple flows
		catalog.set_flow(FlowId(1), CommitVersion(1), Some(flow1.clone()));
		catalog.set_flow(FlowId(2), CommitVersion(1), Some(flow2.clone()));
		catalog.set_flow(FlowId(3), CommitVersion(1), Some(flow3.clone()));

		// All should be findable
		assert_eq!(catalog.find_flow_by_name_at(namespace_id, "flow1", CommitVersion(1)), Some(flow1));
		assert_eq!(catalog.find_flow_by_name_at(namespace_id, "flow2", CommitVersion(1)), Some(flow2));
		assert_eq!(catalog.find_flow_by_name_at(namespace_id, "flow3", CommitVersion(1)), Some(flow3));
	}

	#[test]
	fn test_flow_versioning() {
		let catalog = MaterializedCatalog::new();
		let flow_id = FlowId(1);
		let namespace_id = NamespaceId::SYSTEM;

		// Create multiple versions
		let flow_v1 = create_test_flow(flow_id, namespace_id, "flow_v1");
		let mut flow_v2 = flow_v1.clone();
		flow_v2.name = "flow_v2".to_string();
		let mut flow_v3 = flow_v2.clone();
		flow_v3.name = "flow_v3".to_string();

		// Set at different versions
		catalog.set_flow(flow_id, CommitVersion(10), Some(flow_v1.clone()));
		catalog.set_flow(flow_id, CommitVersion(20), Some(flow_v2.clone()));
		catalog.set_flow(flow_id, CommitVersion(30), Some(flow_v3.clone()));

		// Query at different versions
		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(5)), None);
		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(10)), Some(flow_v1.clone()));
		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(15)), Some(flow_v1));
		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(20)), Some(flow_v2.clone()));
		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(25)), Some(flow_v2));
		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(30)), Some(flow_v3.clone()));
		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(100)), Some(flow_v3));
	}
}
