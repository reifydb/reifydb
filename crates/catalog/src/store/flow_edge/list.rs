// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use flow_edge_by_flow::SCHEMA;
use reifydb_core::{
	interface::catalog::flow::{FlowEdgeDef, FlowEdgeId, FlowId, FlowNodeId},
	key::{
		EncodableKey,
		flow_edge::{FlowEdgeByFlowKey, FlowEdgeKey},
	},
};
use reifydb_transaction::transaction::AsTransaction;

use crate::{
	CatalogStore,
	store::flow_edge::schema::{flow_edge, flow_edge_by_flow},
};

impl CatalogStore {
	pub(crate) fn list_flow_edges_by_flow(
		rx: &mut impl AsTransaction,
		flow_id: FlowId,
	) -> crate::Result<Vec<FlowEdgeDef>> {
		let mut txn = rx.as_transaction();

		// Collect edge IDs first to avoid holding stream borrow
		let mut edge_ids = Vec::new();
		{
			let mut stream = txn.range(FlowEdgeByFlowKey::full_scan(flow_id), 1024)?;
			while let Some(entry) = stream.next() {
				let multi = entry?;
				edge_ids.push(FlowEdgeId(SCHEMA.get_u64(&multi.values, flow_edge_by_flow::ID)));
			}
		}

		// Then fetch each edge
		let mut edges = Vec::new();
		for edge_id in edge_ids {
			if let Some(edge) = Self::find_flow_edge(&mut txn, edge_id)? {
				edges.push(edge);
			}
		}

		// Sort by edge_id to ensure consistent ordering (edges are stored in descending order)
		edges.sort_by_key(|e| e.id);

		Ok(edges)
	}

	pub(crate) fn list_flow_edges_all(rx: &mut impl AsTransaction) -> crate::Result<Vec<FlowEdgeDef>> {
		let mut txn = rx.as_transaction();
		let mut result = Vec::new();

		let mut stream = txn.range(FlowEdgeKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let entry = entry?;
			if let Some(flow_edge_key) = FlowEdgeKey::decode(&entry.key) {
				let edge_id = flow_edge_key.edge;
				let flow_id = FlowId(flow_edge::SCHEMA.get_u64(&entry.values, flow_edge::FLOW));
				let source = FlowNodeId(flow_edge::SCHEMA.get_u64(&entry.values, flow_edge::SOURCE));
				let target = FlowNodeId(flow_edge::SCHEMA.get_u64(&entry.values, flow_edge::TARGET));

				let edge_def = FlowEdgeDef {
					id: edge_id,
					flow: flow_id,
					source,
					target,
				};

				result.push(edge_def);
			}
		}

		Ok(result)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_engine::test_utils::create_test_admin_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow, create_flow_edge, create_flow_node, create_namespace, ensure_test_flow},
	};

	#[test]
	fn test_list_flow_edges_by_flow() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]);
		let edge = create_flow_edge(&mut txn, flow.id, node1.id, node2.id);

		let edges = CatalogStore::list_flow_edges_by_flow(&mut txn, flow.id).unwrap();
		assert_eq!(edges.len(), 1);
		assert_eq!(edges[0].id, edge.id);
	}

	#[test]
	fn test_list_flow_edges_by_flow_empty() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let edges = CatalogStore::list_flow_edges_by_flow(&mut txn, flow.id).unwrap();
		assert!(edges.is_empty());
	}

	#[test]
	fn test_list_flow_edges_by_flow_multiple() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]);
		let node3 = create_flow_node(&mut txn, flow.id, 5, &[0x03]);

		let edge1 = create_flow_edge(&mut txn, flow.id, node1.id, node2.id);
		let edge2 = create_flow_edge(&mut txn, flow.id, node2.id, node3.id);

		let edges = CatalogStore::list_flow_edges_by_flow(&mut txn, flow.id).unwrap();
		assert_eq!(edges.len(), 2);

		// Verify all edges are present
		let ids: Vec<_> = edges.iter().map(|e| e.id).collect();
		assert!(ids.contains(&edge1.id));
		assert!(ids.contains(&edge2.id));
	}

	#[test]
	fn test_list_flow_edges_all() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]);

		create_flow_edge(&mut txn, flow.id, node1.id, node2.id);

		let edges = CatalogStore::list_flow_edges_all(&mut txn).unwrap();
		assert_eq!(edges.len(), 1);
	}

	#[test]
	fn test_list_flow_edges_all_empty() {
		let mut txn = create_test_admin_transaction();

		let edges = CatalogStore::list_flow_edges_all(&mut txn).unwrap();
		assert!(edges.is_empty());
	}

	#[test]
	fn test_list_flow_edges_all_multiple_flows() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");

		let flow1 = create_flow(&mut txn, "test_namespace", "flow_one");
		let flow2 = create_flow(&mut txn, "test_namespace", "flow_two");

		let node1a = create_flow_node(&mut txn, flow1.id, 1, &[0x01]);
		let node1b = create_flow_node(&mut txn, flow1.id, 4, &[0x02]);
		let node2a = create_flow_node(&mut txn, flow2.id, 1, &[0x03]);
		let node2b = create_flow_node(&mut txn, flow2.id, 4, &[0x04]);

		create_flow_edge(&mut txn, flow1.id, node1a.id, node1b.id);
		create_flow_edge(&mut txn, flow2.id, node2a.id, node2b.id);

		let all_edges = CatalogStore::list_flow_edges_all(&mut txn).unwrap();
		assert_eq!(all_edges.len(), 2);

		// Verify edges are from correct flows
		let flow1_edges: Vec<_> = all_edges.iter().filter(|e| e.flow == flow1.id).collect();
		let flow2_edges: Vec<_> = all_edges.iter().filter(|e| e.flow == flow2.id).collect();

		assert_eq!(flow1_edges.len(), 1);
		assert_eq!(flow2_edges.len(), 1);
	}
}
