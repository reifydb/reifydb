// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::FlowEdgeDef,
	key::{FlowEdgeByFlowKey, FlowEdgeKey},
};
use reifydb_transaction::StandardCommandTransaction;

use crate::store::flow_edge::layout::{flow_edge, flow_edge_by_flow};

impl crate::CatalogStore {
	pub fn create_flow_edge(txn: &mut StandardCommandTransaction, edge_def: &FlowEdgeDef) -> crate::Result<()> {
		// Write to main flow_edge table
		let mut row = flow_edge::LAYOUT.allocate();
		flow_edge::LAYOUT.set_u64(&mut row, flow_edge::ID, edge_def.id);
		flow_edge::LAYOUT.set_u64(&mut row, flow_edge::FLOW, edge_def.flow);
		flow_edge::LAYOUT.set_u64(&mut row, flow_edge::SOURCE, edge_def.source);
		flow_edge::LAYOUT.set_u64(&mut row, flow_edge::TARGET, edge_def.target);

		txn.set(&FlowEdgeKey::encoded(edge_def.id), row)?;

		// Write to flow_edge_by_flow index
		let mut index_row = flow_edge_by_flow::LAYOUT.allocate();
		flow_edge_by_flow::LAYOUT.set_u64(&mut index_row, flow_edge_by_flow::FLOW, edge_def.flow);
		flow_edge_by_flow::LAYOUT.set_u64(&mut index_row, flow_edge_by_flow::ID, edge_def.id);

		txn.set(&FlowEdgeByFlowKey::encoded(edge_def.flow, edge_def.id), index_row)?;

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow, create_flow_edge, create_flow_node, create_namespace, ensure_test_flow},
	};

	#[test]
	fn test_create_flow_edge() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]);

		let edge = create_flow_edge(&mut txn, flow.id, node1.id, node2.id);

		// Verify edge was created
		let result = CatalogStore::get_flow_edge(&mut txn, edge.id).unwrap();
		assert_eq!(result.id, edge.id);
		assert_eq!(result.flow, flow.id);
		assert_eq!(result.source, node1.id);
		assert_eq!(result.target, node2.id);
	}

	#[test]
	fn test_create_multiple_edges_same_flow() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]);
		let node3 = create_flow_node(&mut txn, flow.id, 5, &[0x03]);

		let edge1 = create_flow_edge(&mut txn, flow.id, node1.id, node2.id);
		let edge2 = create_flow_edge(&mut txn, flow.id, node2.id, node3.id);

		// Verify both edges exist
		let result1 = CatalogStore::get_flow_edge(&mut txn, edge1.id).unwrap();
		let result2 = CatalogStore::get_flow_edge(&mut txn, edge2.id).unwrap();

		assert_eq!(result1.source, node1.id);
		assert_eq!(result1.target, node2.id);
		assert_eq!(result2.source, node2.id);
		assert_eq!(result2.target, node3.id);
	}

	#[test]
	fn test_create_edges_different_flows() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");

		let flow1 = create_flow(&mut txn, "test_namespace", "flow_one");
		let flow2 = create_flow(&mut txn, "test_namespace", "flow_two");

		let node1a = create_flow_node(&mut txn, flow1.id, 1, &[0x01]);
		let node1b = create_flow_node(&mut txn, flow1.id, 4, &[0x02]);
		let node2a = create_flow_node(&mut txn, flow2.id, 1, &[0x03]);
		let node2b = create_flow_node(&mut txn, flow2.id, 4, &[0x04]);

		let edge1 = create_flow_edge(&mut txn, flow1.id, node1a.id, node1b.id);
		let edge2 = create_flow_edge(&mut txn, flow2.id, node2a.id, node2b.id);

		// Verify edges are in correct flows
		let result1 = CatalogStore::get_flow_edge(&mut txn, edge1.id).unwrap();
		let result2 = CatalogStore::get_flow_edge(&mut txn, edge2.id).unwrap();

		assert_eq!(result1.flow, flow1.id);
		assert_eq!(result2.flow, flow2.id);
	}

	#[test]
	fn test_edge_appears_in_index() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]);

		let edge = create_flow_edge(&mut txn, flow.id, node1.id, node2.id);

		// Verify edge appears in flow index by listing edges for flow
		let edges = CatalogStore::list_flow_edges_by_flow(&mut txn, flow.id).unwrap();
		assert_eq!(edges.len(), 1);
		assert_eq!(edges[0].id, edge.id);
	}
}
