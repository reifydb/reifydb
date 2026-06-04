// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowEdge, FlowEdgeId, FlowId},
};

use crate::cache::{CatalogCache, MultiVersionFlowEdge};

impl CatalogCache {
	pub fn find_flow_edge_at(&self, id: FlowEdgeId, version: CommitVersion) -> Option<FlowEdge> {
		self.flow_edges.get(&id).and_then(|entry| entry.value().get(version))
	}

	pub fn find_flow_edge(&self, id: FlowEdgeId) -> Option<FlowEdge> {
		self.flow_edges.get(&id).and_then(|entry| entry.value().get_latest())
	}

	pub fn list_flow_edges_by_flow_at(&self, flow: FlowId, version: CommitVersion) -> Option<Vec<FlowEdge>> {
		let entry = self.flow_edges_by_flow.get(&flow)?;
		let mut edges: Vec<FlowEdge> =
			entry.value().iter().filter_map(|id| self.find_flow_edge_at(*id, version)).collect();
		edges.sort_by_key(|e| e.id.0);
		Some(edges)
	}

	pub fn set_flow_edge(&self, id: FlowEdgeId, version: CommitVersion, edge: Option<FlowEdge>) {
		let multi = self.flow_edges.get_or_insert_with(id, MultiVersionFlowEdge::new);
		match edge {
			Some(new) => {
				let flow = new.flow;
				multi.value().insert(version, new);
				if let Some(entry) = self.flow_edges_by_flow.get(&flow) {
					let mut ids = entry.value().clone();
					if !ids.contains(&id) {
						ids.push(id);
						drop(entry);
						self.flow_edges_by_flow.insert(flow, ids);
					}
				} else {
					self.flow_edges_by_flow.insert(flow, vec![id]);
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
	use super::*;

	fn edge(id: u64, flow: u64, source: u64, target: u64) -> FlowEdge {
		use reifydb_core::interface::catalog::flow::FlowNodeId;
		FlowEdge {
			id: FlowEdgeId(id),
			flow: FlowId(flow),
			source: FlowNodeId(source),
			target: FlowNodeId(target),
		}
	}

	#[test]
	fn test_set_and_find_flow_edge() {
		let cache = CatalogCache::new();
		let e = edge(1, 10, 100, 200);

		cache.set_flow_edge(FlowEdgeId(1), CommitVersion(1), Some(e.clone()));

		assert_eq!(cache.find_flow_edge_at(FlowEdgeId(1), CommitVersion(1)), Some(e.clone()));
		assert_eq!(cache.find_flow_edge_at(FlowEdgeId(1), CommitVersion(5)), Some(e));
		assert_eq!(cache.find_flow_edge_at(FlowEdgeId(1), CommitVersion(0)), None);
	}

	#[test]
	fn list_by_flow_moves_atomically_between_versions() {
		let cache = CatalogCache::new();
		let e1 = edge(1, 10, 100, 200);
		let e2 = edge(2, 10, 200, 300);

		assert_eq!(cache.list_flow_edges_by_flow_at(FlowId(10), CommitVersion(1)), None);

		cache.set_flow_edge(FlowEdgeId(1), CommitVersion(1), Some(e1.clone()));
		cache.set_flow_edge(FlowEdgeId(2), CommitVersion(1), Some(e2.clone()));

		assert_eq!(
			cache.list_flow_edges_by_flow_at(FlowId(10), CommitVersion(1)),
			Some(vec![e1, e2]),
			"both edges visible at the create version"
		);

		cache.set_flow_edge(FlowEdgeId(1), CommitVersion(2), None);
		cache.set_flow_edge(FlowEdgeId(2), CommitVersion(2), None);

		assert_eq!(
			cache.list_flow_edges_by_flow_at(FlowId(10), CommitVersion(2)),
			Some(vec![]),
			"both edges gone at the drop version - never one without the other"
		);
	}
}
