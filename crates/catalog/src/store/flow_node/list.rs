// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::flow::{FlowId, FlowNodeDef, FlowNodeId},
	key::{EncodableKey, flow_node::FlowNodeKey},
};
use reifydb_transaction::standard::IntoStandardTransaction;

use crate::{
	CatalogStore,
	store::flow_node::schema::{flow_node, flow_node_by_flow},
};

impl CatalogStore {
	pub fn list_flow_nodes_by_flow(
		rx: &mut impl IntoStandardTransaction,
		flow_id: FlowId,
	) -> crate::Result<Vec<FlowNodeDef>> {
		let mut txn = rx.into_standard_transaction();

		// First collect all node IDs to avoid holding stream borrow
		let mut node_ids = Vec::new();
		{
			let mut stream =
				txn.range(reifydb_core::key::flow_node::FlowNodeByFlowKey::full_scan(flow_id), 1024)?;
			while let Some(entry) = stream.next() {
				let multi = entry?;
				node_ids.push(FlowNodeId(
					flow_node_by_flow::SCHEMA.get_u64(&multi.values, flow_node_by_flow::ID),
				));
			}
		}

		// Then fetch each node
		let mut nodes = Vec::new();
		for node_id in node_ids {
			if let Some(node) = Self::find_flow_node(&mut txn, node_id)? {
				nodes.push(node);
			}
		}

		Ok(nodes)
	}

	pub fn list_flow_nodes_all(rx: &mut impl IntoStandardTransaction) -> crate::Result<Vec<FlowNodeDef>> {
		let mut txn = rx.into_standard_transaction();
		let mut result = Vec::new();

		let mut stream = txn.range(FlowNodeKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let entry = entry?;
			if let Some(flow_node_key) = FlowNodeKey::decode(&entry.key) {
				let node_id = flow_node_key.node;
				let flow_id = FlowId(flow_node::SCHEMA.get_u64(&entry.values, flow_node::FLOW));
				let node_type = flow_node::SCHEMA.get_u8(&entry.values, flow_node::TYPE);
				let data = flow_node::SCHEMA.get_blob(&entry.values, flow_node::DATA).clone();

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
pub mod tests {
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow, create_flow_node, create_namespace, ensure_test_flow},
	};

	#[test]
	fn test_list_flow_nodes_by_flow() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]);

		let nodes = CatalogStore::list_flow_nodes_by_flow(&mut txn, flow.id).unwrap();
		assert_eq!(nodes.len(), 1);
		assert_eq!(nodes[0].id, node.id);
	}

	#[test]
	fn test_list_flow_nodes_by_flow_empty() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let nodes = CatalogStore::list_flow_nodes_by_flow(&mut txn, flow.id).unwrap();
		assert!(nodes.is_empty());
	}

	#[test]
	fn test_list_flow_nodes_by_flow_multiple() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]);
		let node3 = create_flow_node(&mut txn, flow.id, 5, &[0x03]);

		let nodes = CatalogStore::list_flow_nodes_by_flow(&mut txn, flow.id).unwrap();
		assert_eq!(nodes.len(), 3);

		// Verify all nodes are present
		let ids: Vec<_> = nodes.iter().map(|n| n.id).collect();
		assert!(ids.contains(&node1.id));
		assert!(ids.contains(&node2.id));
		assert!(ids.contains(&node3.id));
	}

	#[test]
	fn test_list_flow_nodes_all() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		create_flow_node(&mut txn, flow.id, 1, &[0x01]);
		create_flow_node(&mut txn, flow.id, 4, &[0x02]);

		let nodes = CatalogStore::list_flow_nodes_all(&mut txn).unwrap();
		assert_eq!(nodes.len(), 2);
	}

	#[test]
	fn test_list_flow_nodes_all_empty() {
		let mut txn = create_test_command_transaction();

		let nodes = CatalogStore::list_flow_nodes_all(&mut txn).unwrap();
		assert!(nodes.is_empty());
	}

	#[test]
	fn test_list_flow_nodes_all_multiple_flows() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");

		let flow1 = create_flow(&mut txn, "test_namespace", "flow_one");
		let flow2 = create_flow(&mut txn, "test_namespace", "flow_two");

		create_flow_node(&mut txn, flow1.id, 1, &[0x01]);
		create_flow_node(&mut txn, flow1.id, 4, &[0x02]);
		create_flow_node(&mut txn, flow2.id, 1, &[0x03]);

		let all_nodes = CatalogStore::list_flow_nodes_all(&mut txn).unwrap();
		assert_eq!(all_nodes.len(), 3);

		// Verify nodes are from correct flows
		let flow1_nodes: Vec<_> = all_nodes.iter().filter(|n| n.flow == flow1.id).collect();
		let flow2_nodes: Vec<_> = all_nodes.iter().filter(|n| n.flow == flow2.id).collect();

		assert_eq!(flow1_nodes.len(), 2);
		assert_eq!(flow2_nodes.len(), 1);
	}
}
