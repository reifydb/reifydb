// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{CommandTransaction, FlowNodeId},
	key::{FlowNodeByFlowKey, FlowNodeKey},
};

use crate::CatalogStore;

impl CatalogStore {
	pub async fn delete_flow_node(txn: &mut impl CommandTransaction, node_id: FlowNodeId) -> crate::Result<()> {
		// First, get the node to find the flow ID for index deletion
		let node = CatalogStore::find_flow_node(txn, node_id).await?;

		if let Some(node_def) = node {
			// Delete from main flow_node table
			txn.remove(&FlowNodeKey::encoded(node_id)).await?;

			// Delete from flow_node_by_flow index
			txn.remove(&FlowNodeByFlowKey::encoded(node_def.flow, node_id)).await?;
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::FlowNodeId;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow_node, create_namespace, ensure_test_flow},
	};

	#[tokio::test]
	async fn test_delete_flow_node() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace").await;
		let flow = ensure_test_flow(&mut txn).await;

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]).await;

		// Node should exist
		assert!(CatalogStore::find_flow_node(&mut txn, node.id).await.unwrap().is_some());

		// Delete node
		CatalogStore::delete_flow_node(&mut txn, node.id).await.unwrap();

		// Node should no longer exist
		assert!(CatalogStore::find_flow_node(&mut txn, node.id).await.unwrap().is_none());
	}

	#[tokio::test]
	async fn test_delete_node_removes_from_index() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace").await;
		let flow = ensure_test_flow(&mut txn).await;

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]).await;

		// Node should be in flow index
		let nodes = CatalogStore::list_flow_nodes_by_flow(&mut txn, flow.id).await.unwrap();
		assert_eq!(nodes.len(), 1);

		// Delete node
		CatalogStore::delete_flow_node(&mut txn, node.id).await.unwrap();

		// Node should be removed from flow index
		let nodes = CatalogStore::list_flow_nodes_by_flow(&mut txn, flow.id).await.unwrap();
		assert!(nodes.is_empty());
	}

	#[tokio::test]
	async fn test_delete_nonexistent_node() {
		let mut txn = create_test_command_transaction();

		// Deleting a non-existent node should succeed silently
		CatalogStore::delete_flow_node(&mut txn, FlowNodeId(999)).await.unwrap();
	}

	#[tokio::test]
	async fn test_delete_one_node_keeps_others() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace").await;
		let flow = ensure_test_flow(&mut txn).await;

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]).await;
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]).await;

		// Delete first node
		CatalogStore::delete_flow_node(&mut txn, node1.id).await.unwrap();

		// First node should be gone, second should remain
		assert!(CatalogStore::find_flow_node(&mut txn, node1.id).await.unwrap().is_none());
		assert!(CatalogStore::find_flow_node(&mut txn, node2.id).await.unwrap().is_some());

		// List should only have second node
		let nodes = CatalogStore::list_flow_nodes_by_flow(&mut txn, flow.id).await.unwrap();
		assert_eq!(nodes.len(), 1);
		assert_eq!(nodes[0].id, node2.id);
	}
}
