// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	key::flow_node::{FlowNodeByFlowKey, FlowNodeKey},
};
use reifydb_transaction::standard::command::StandardCommandTransaction;

use crate::CatalogStore;

impl CatalogStore {
	pub(crate) fn delete_flow_node(txn: &mut StandardCommandTransaction, node_id: FlowNodeId) -> crate::Result<()> {
		// First, get the node to find the flow ID for index deletion
		let node = CatalogStore::find_flow_node(txn, node_id)?;

		if let Some(node_def) = node {
			// Delete from main flow_node table
			txn.remove(&FlowNodeKey::encoded(node_id))?;

			// Delete from flow_node_by_flow index
			txn.remove(&FlowNodeByFlowKey::encoded(node_def.flow, node_id))?;
		}

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::flow::FlowNodeId;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow_node, create_namespace, ensure_test_flow},
	};

	#[test]
	fn test_delete_flow_node() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]);

		// Node should exist
		assert!(CatalogStore::find_flow_node(&mut txn, node.id).unwrap().is_some());

		// Delete node
		CatalogStore::delete_flow_node(&mut txn, node.id).unwrap();

		// Node should no longer exist
		assert!(CatalogStore::find_flow_node(&mut txn, node.id).unwrap().is_none());
	}

	#[test]
	fn test_delete_node_removes_from_index() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]);

		// Node should be in flow index
		let nodes = CatalogStore::list_flow_nodes_by_flow(&mut txn, flow.id).unwrap();
		assert_eq!(nodes.len(), 1);

		// Delete node
		CatalogStore::delete_flow_node(&mut txn, node.id).unwrap();

		// Node should be removed from flow index
		let nodes = CatalogStore::list_flow_nodes_by_flow(&mut txn, flow.id).unwrap();
		assert!(nodes.is_empty());
	}

	#[test]
	fn test_delete_nonexistent_node() {
		let mut txn = create_test_command_transaction();

		// Deleting a non-existent node should succeed silently
		CatalogStore::delete_flow_node(&mut txn, FlowNodeId(999)).unwrap();
	}

	#[test]
	fn test_delete_one_node_keeps_others() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]);

		// Delete first node
		CatalogStore::delete_flow_node(&mut txn, node1.id).unwrap();

		// First node should be gone, second should remain
		assert!(CatalogStore::find_flow_node(&mut txn, node1.id).unwrap().is_none());
		assert!(CatalogStore::find_flow_node(&mut txn, node2.id).unwrap().is_some());

		// List should only have second node
		let nodes = CatalogStore::list_flow_nodes_by_flow(&mut txn, flow.id).unwrap();
		assert_eq!(nodes.len(), 1);
		assert_eq!(nodes[0].id, node2.id);
	}
}
