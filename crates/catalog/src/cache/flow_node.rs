// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, FlowNode, FlowNodeId},
};

use crate::cache::{CatalogCache, MultiVersionFlowNode};

impl CatalogCache {
	pub fn find_flow_node_at(&self, id: FlowNodeId, version: CommitVersion) -> Option<FlowNode> {
		self.flow_nodes.get(&id).and_then(|entry| entry.value().get(version))
	}

	pub fn find_flow_node(&self, id: FlowNodeId) -> Option<FlowNode> {
		self.flow_nodes.get(&id).and_then(|entry| entry.value().get_latest())
	}

	pub fn list_flow_nodes_by_flow_at(&self, flow: FlowId, version: CommitVersion) -> Option<Vec<FlowNode>> {
		let entry = self.flow_nodes_by_flow.get(&flow)?;
		Some(entry.value().iter().filter_map(|id| self.find_flow_node_at(*id, version)).collect())
	}

	pub fn set_flow_node(&self, id: FlowNodeId, version: CommitVersion, node: Option<FlowNode>) {
		let _guard = self.write_lock.lock();
		let multi = self.flow_nodes.get_or_insert_with(id, MultiVersionFlowNode::new);
		match node {
			Some(new) => {
				let flow = new.flow;
				multi.value().insert(version, new);
				if let Some(entry) = self.flow_nodes_by_flow.get(&flow) {
					let mut ids = entry.value().clone();
					if !ids.contains(&id) {
						ids.push(id);
						drop(entry);
						self.flow_nodes_by_flow.insert(flow, ids);
					}
				} else {
					self.flow_nodes_by_flow.insert(flow, vec![id]);
				}
			}
			None => {
				multi.value().remove(version);
			}
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_value::value::blob::Blob;

	use super::*;

	fn node(id: u64, flow: u64, node_type: u8) -> FlowNode {
		FlowNode {
			id: FlowNodeId(id),
			flow: FlowId(flow),
			node_type,
			data: Blob::from([node_type].as_slice()),
		}
	}

	#[test]
	fn test_set_and_find_flow_node() {
		let cache = CatalogCache::new();
		let n = node(1, 10, 3);

		cache.set_flow_node(FlowNodeId(1), CommitVersion(1), Some(n.clone()));

		assert_eq!(cache.find_flow_node_at(FlowNodeId(1), CommitVersion(1)), Some(n.clone()));
		assert_eq!(cache.find_flow_node_at(FlowNodeId(1), CommitVersion(5)), Some(n));
		assert_eq!(cache.find_flow_node_at(FlowNodeId(1), CommitVersion(0)), None);
	}

	#[test]
	fn test_flow_node_deletion_is_version_scoped() {
		let cache = CatalogCache::new();
		let n = node(1, 10, 3);

		cache.set_flow_node(FlowNodeId(1), CommitVersion(1), Some(n.clone()));
		cache.set_flow_node(FlowNodeId(1), CommitVersion(2), None);

		assert_eq!(cache.find_flow_node_at(FlowNodeId(1), CommitVersion(1)), Some(n));
		assert_eq!(cache.find_flow_node_at(FlowNodeId(1), CommitVersion(2)), None);
	}

	#[test]
	fn list_by_flow_returns_full_set_at_create_and_none_when_uncached() {
		let cache = CatalogCache::new();
		let n1 = node(1, 10, 1);
		let n2 = node(2, 10, 2);

		assert_eq!(cache.list_flow_nodes_by_flow_at(FlowId(10), CommitVersion(1)), None);

		cache.set_flow_node(FlowNodeId(1), CommitVersion(1), Some(n1.clone()));
		cache.set_flow_node(FlowNodeId(2), CommitVersion(1), Some(n2.clone()));

		let mut listed =
			cache.list_flow_nodes_by_flow_at(FlowId(10), CommitVersion(1)).expect("flow is cached");
		listed.sort_by_key(|n| n.id.0);
		assert_eq!(listed, vec![n1, n2]);
	}

	#[test]
	fn list_by_flow_excludes_nodes_deleted_at_or_before_the_read_version() {
		let cache = CatalogCache::new();
		let n1 = node(1, 10, 1);
		let n2 = node(2, 10, 2);

		cache.set_flow_node(FlowNodeId(1), CommitVersion(1), Some(n1.clone()));
		cache.set_flow_node(FlowNodeId(2), CommitVersion(1), Some(n2.clone()));
		cache.set_flow_node(FlowNodeId(1), CommitVersion(2), None);
		cache.set_flow_node(FlowNodeId(2), CommitVersion(2), None);

		let mut at_create =
			cache.list_flow_nodes_by_flow_at(FlowId(10), CommitVersion(1)).expect("flow is cached");
		at_create.sort_by_key(|n| n.id.0);
		assert_eq!(at_create, vec![n1, n2], "the full node set must be visible at the create version");

		assert_eq!(
			cache.list_flow_nodes_by_flow_at(FlowId(10), CommitVersion(2)),
			Some(vec![]),
			"once every node is dropped the set is empty - never a partial set"
		);
	}
}
