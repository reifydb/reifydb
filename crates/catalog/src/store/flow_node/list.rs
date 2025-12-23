// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{EncodableKey, FlowId, FlowNodeDef, FlowNodeId, QueryTransaction},
	key::FlowNodeKey,
};

use crate::{
	CatalogStore,
	store::flow_node::layout::{flow_node, flow_node_by_flow},
};

impl CatalogStore {
	pub async fn list_flow_nodes_by_flow(
		txn: &mut impl QueryTransaction,
		flow_id: FlowId,
	) -> crate::Result<Vec<FlowNodeDef>> {
		// First collect all node IDs
		let batch = txn.range(reifydb_core::key::FlowNodeByFlowKey::full_scan(flow_id)).await?;
		let node_ids: Vec<FlowNodeId> = batch
			.items
			.iter()
			.map(|multi| {
				FlowNodeId(flow_node_by_flow::LAYOUT.get_u64(&multi.values, flow_node_by_flow::ID))
			})
			.collect();

		// Then fetch each node
		let mut nodes = Vec::new();
		for node_id in node_ids {
			if let Some(node) = Self::find_flow_node(txn, node_id).await? {
				nodes.push(node);
			}
		}

		Ok(nodes)
	}

	pub async fn list_flow_nodes_all(txn: &mut impl QueryTransaction) -> crate::Result<Vec<FlowNodeDef>> {
		let mut result = Vec::new();

		let batch = txn.range(FlowNodeKey::full_scan()).await?;
		let entries: Vec<_> = batch.items.into_iter().collect();

		for entry in entries {
			if let Some(flow_node_key) = FlowNodeKey::decode(&entry.key) {
				let node_id = flow_node_key.node;
				let flow_id = FlowId(flow_node::LAYOUT.get_u64(&entry.values, flow_node::FLOW));
				let node_type = flow_node::LAYOUT.get_u8(&entry.values, flow_node::TYPE);
				let data = flow_node::LAYOUT.get_blob(&entry.values, flow_node::DATA).clone();

				let node_def = FlowNodeDef {
					id: node_id,
					flow: flow_id,
					node_type,
					data,
				};

				result.push(node_def);
			}
		}

		Ok(result)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow, create_flow_node, create_namespace, ensure_test_flow},
	};

	#[tokio::test]
	async fn test_list_flow_nodes_by_flow() {
		let mut txn = create_test_command_transaction().await;
		let _namespace = create_namespace(&mut txn, "test_namespace").await;
		let flow = ensure_test_flow(&mut txn).await;

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]).await;

		let nodes = CatalogStore::list_flow_nodes_by_flow(&mut txn, flow.id).await.unwrap();
		assert_eq!(nodes.len(), 1);
		assert_eq!(nodes[0].id, node.id);
	}

	#[tokio::test]
	async fn test_list_flow_nodes_by_flow_empty() {
		let mut txn = create_test_command_transaction().await;
		let _namespace = create_namespace(&mut txn, "test_namespace").await;
		let flow = ensure_test_flow(&mut txn).await;

		let nodes = CatalogStore::list_flow_nodes_by_flow(&mut txn, flow.id).await.unwrap();
		assert!(nodes.is_empty());
	}

	#[tokio::test]
	async fn test_list_flow_nodes_by_flow_multiple() {
		let mut txn = create_test_command_transaction().await;
		let _namespace = create_namespace(&mut txn, "test_namespace").await;
		let flow = ensure_test_flow(&mut txn).await;

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]).await;
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]).await;
		let node3 = create_flow_node(&mut txn, flow.id, 5, &[0x03]).await;

		let nodes = CatalogStore::list_flow_nodes_by_flow(&mut txn, flow.id).await.unwrap();
		assert_eq!(nodes.len(), 3);

		// Verify all nodes are present
		let ids: Vec<_> = nodes.iter().map(|n| n.id).collect();
		assert!(ids.contains(&node1.id));
		assert!(ids.contains(&node2.id));
		assert!(ids.contains(&node3.id));
	}

	#[tokio::test]
	async fn test_list_flow_nodes_all() {
		let mut txn = create_test_command_transaction().await;
		let _namespace = create_namespace(&mut txn, "test_namespace").await;
		let flow = ensure_test_flow(&mut txn).await;

		create_flow_node(&mut txn, flow.id, 1, &[0x01]).await;
		create_flow_node(&mut txn, flow.id, 4, &[0x02]).await;

		let nodes = CatalogStore::list_flow_nodes_all(&mut txn).await.unwrap();
		assert_eq!(nodes.len(), 2);
	}

	#[tokio::test]
	async fn test_list_flow_nodes_all_empty() {
		let mut txn = create_test_command_transaction().await;

		let nodes = CatalogStore::list_flow_nodes_all(&mut txn).await.unwrap();
		assert!(nodes.is_empty());
	}

	#[tokio::test]
	async fn test_list_flow_nodes_all_multiple_flows() {
		let mut txn = create_test_command_transaction().await;
		let _namespace = create_namespace(&mut txn, "test_namespace").await;

		let flow1 = create_flow(&mut txn, "test_namespace", "flow_one").await;
		let flow2 = create_flow(&mut txn, "test_namespace", "flow_two").await;

		create_flow_node(&mut txn, flow1.id, 1, &[0x01]).await;
		create_flow_node(&mut txn, flow1.id, 4, &[0x02]).await;
		create_flow_node(&mut txn, flow2.id, 1, &[0x03]).await;

		let all_nodes = CatalogStore::list_flow_nodes_all(&mut txn).await.unwrap();
		assert_eq!(all_nodes.len(), 3);

		// Verify nodes are from correct flows
		let flow1_nodes: Vec<_> = all_nodes.iter().filter(|n| n.flow == flow1.id).collect();
		let flow2_nodes: Vec<_> = all_nodes.iter().filter(|n| n.flow == flow2.id).collect();

		assert_eq!(flow1_nodes.len(), 2);
		assert_eq!(flow2_nodes.len(), 1);
	}
}
