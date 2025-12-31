// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::FlowEdgeId,
	key::{FlowEdgeByFlowKey, FlowEdgeKey},
};
use reifydb_transaction::StandardCommandTransaction;

use crate::CatalogStore;

impl CatalogStore {
	pub async fn delete_flow_edge(txn: &mut StandardCommandTransaction, edge_id: FlowEdgeId) -> crate::Result<()> {
		// First, get the edge to find the flow ID for index deletion
		let edge = CatalogStore::find_flow_edge(txn, edge_id).await?;

		if let Some(edge_def) = edge {
			// Delete from main flow_edge table
			txn.remove(&FlowEdgeKey::encoded(edge_id)).await?;

			// Delete from flow_edge_by_flow index
			txn.remove(&FlowEdgeByFlowKey::encoded(edge_def.flow, edge_id)).await?;
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::FlowEdgeId;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow_edge, create_flow_node, create_namespace, ensure_test_flow},
	};

	#[tokio::test]
	async fn test_delete_flow_edge() {
		let mut txn = create_test_command_transaction().await;
		let _namespace = create_namespace(&mut txn, "test_namespace").await;
		let flow = ensure_test_flow(&mut txn).await;

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]).await;
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]).await;
		let edge = create_flow_edge(&mut txn, flow.id, node1.id, node2.id).await;

		// Edge should exist
		assert!(CatalogStore::find_flow_edge(&mut txn, edge.id).await.unwrap().is_some());

		// Delete edge
		CatalogStore::delete_flow_edge(&mut txn, edge.id).await.unwrap();

		// Edge should no longer exist
		assert!(CatalogStore::find_flow_edge(&mut txn, edge.id).await.unwrap().is_none());
	}

	#[tokio::test]
	async fn test_delete_edge_removes_from_index() {
		let mut txn = create_test_command_transaction().await;
		let _namespace = create_namespace(&mut txn, "test_namespace").await;
		let flow = ensure_test_flow(&mut txn).await;

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]).await;
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]).await;
		let edge = create_flow_edge(&mut txn, flow.id, node1.id, node2.id).await;

		// Edge should be in flow index
		let edges = CatalogStore::list_flow_edges_by_flow(&mut txn, flow.id).await.unwrap();
		assert_eq!(edges.len(), 1);

		// Delete edge
		CatalogStore::delete_flow_edge(&mut txn, edge.id).await.unwrap();

		// Edge should be removed from flow index
		let edges = CatalogStore::list_flow_edges_by_flow(&mut txn, flow.id).await.unwrap();
		assert!(edges.is_empty());
	}

	#[tokio::test]
	async fn test_delete_nonexistent_edge() {
		let mut txn = create_test_command_transaction().await;

		// Deleting a non-existent edge should succeed silently
		CatalogStore::delete_flow_edge(&mut txn, FlowEdgeId(999)).await.unwrap();
	}

	#[tokio::test]
	async fn test_delete_one_edge_keeps_others() {
		let mut txn = create_test_command_transaction().await;
		let _namespace = create_namespace(&mut txn, "test_namespace").await;
		let flow = ensure_test_flow(&mut txn).await;

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]).await;
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]).await;
		let node3 = create_flow_node(&mut txn, flow.id, 5, &[0x03]).await;

		let edge1 = create_flow_edge(&mut txn, flow.id, node1.id, node2.id).await;
		let edge2 = create_flow_edge(&mut txn, flow.id, node2.id, node3.id).await;

		// Delete first edge
		CatalogStore::delete_flow_edge(&mut txn, edge1.id).await.unwrap();

		// First edge should be gone, second should remain
		assert!(CatalogStore::find_flow_edge(&mut txn, edge1.id).await.unwrap().is_none());
		assert!(CatalogStore::find_flow_edge(&mut txn, edge2.id).await.unwrap().is_some());

		// List should only have second edge
		let edges = CatalogStore::list_flow_edges_by_flow(&mut txn, flow.id).await.unwrap();
		assert_eq!(edges.len(), 1);
		assert_eq!(edges[0].id, edge2.id);
	}
}
