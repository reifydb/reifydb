// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::flow::FlowNodeDef,
	key::flow_node::{FlowNodeByFlowKey, FlowNodeKey},
};
use reifydb_transaction::transaction::command::CommandTransaction;

use crate::store::flow_node::schema::{flow_node, flow_node_by_flow};

impl crate::CatalogStore {
	pub(crate) fn create_flow_node(txn: &mut CommandTransaction, node_def: &FlowNodeDef) -> crate::Result<()> {
		// Write to main flow_node table
		let mut row = flow_node::SCHEMA.allocate();
		flow_node::SCHEMA.set_u64(&mut row, flow_node::ID, node_def.id);
		flow_node::SCHEMA.set_u64(&mut row, flow_node::FLOW, node_def.flow);
		flow_node::SCHEMA.set_u8(&mut row, flow_node::TYPE, node_def.node_type);
		flow_node::SCHEMA.set_blob(&mut row, flow_node::DATA, &node_def.data);

		txn.set(&FlowNodeKey::encoded(node_def.id), row)?;

		// Write to flow_node_by_flow index
		let mut index_row = flow_node_by_flow::SCHEMA.allocate();
		flow_node_by_flow::SCHEMA.set_u64(&mut index_row, flow_node_by_flow::FLOW, node_def.flow);
		flow_node_by_flow::SCHEMA.set_u64(&mut index_row, flow_node_by_flow::ID, node_def.id);

		txn.set(&FlowNodeByFlowKey::encoded(node_def.flow, node_def.id), index_row)?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::flow::FlowNodeDef;
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::value::blob::Blob;

	use crate::{
		CatalogStore,
		store::sequence::flow::next_flow_node_id,
		test_utils::{create_namespace, ensure_test_flow},
	};

	#[test]
	fn test_create_flow_node() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node_id = next_flow_node_id(&mut txn).unwrap();
		let node_def = FlowNodeDef {
			id: node_id,
			flow: flow.id,
			node_type: 1, // SourceTable
			data: Blob::from([0x01u8, 0x02, 0x03].as_slice()),
		};

		CatalogStore::create_flow_node(&mut txn, &node_def).unwrap();

		// Verify node was created
		let result = CatalogStore::get_flow_node(&mut txn, node_id).unwrap();
		assert_eq!(result.id, node_id);
		assert_eq!(result.flow, flow.id);
		assert_eq!(result.node_type, 1);
		assert_eq!(result.data.as_ref(), &[0x01, 0x02, 0x03]);
	}

	#[test]
	fn test_create_multiple_nodes_same_flow() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		// Create first node
		let node1_id = next_flow_node_id(&mut txn).unwrap();
		let node1 = FlowNodeDef {
			id: node1_id,
			flow: flow.id,
			node_type: 1, // SourceTable
			data: Blob::from([0x01u8].as_slice()),
		};
		CatalogStore::create_flow_node(&mut txn, &node1).unwrap();

		// Create second node
		let node2_id = next_flow_node_id(&mut txn).unwrap();
		let node2 = FlowNodeDef {
			id: node2_id,
			flow: flow.id,
			node_type: 4, // Filter
			data: Blob::from([0x02u8].as_slice()),
		};
		CatalogStore::create_flow_node(&mut txn, &node2).unwrap();

		// Verify both nodes exist
		let result1 = CatalogStore::get_flow_node(&mut txn, node1_id).unwrap();
		let result2 = CatalogStore::get_flow_node(&mut txn, node2_id).unwrap();

		assert_eq!(result1.node_type, 1);
		assert_eq!(result2.node_type, 4);
	}

	#[test]
	fn test_create_nodes_different_flows() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");

		// Create two flows
		let flow1 = crate::test_utils::create_flow(&mut txn, "test_namespace", "flow_one");
		let flow2 = crate::test_utils::create_flow(&mut txn, "test_namespace", "flow_two");

		// Create node in first flow
		let node1_id = next_flow_node_id(&mut txn).unwrap();
		let node1 = FlowNodeDef {
			id: node1_id,
			flow: flow1.id,
			node_type: 1,
			data: Blob::from([0x01u8].as_slice()),
		};
		CatalogStore::create_flow_node(&mut txn, &node1).unwrap();

		// Create node in second flow
		let node2_id = next_flow_node_id(&mut txn).unwrap();
		let node2 = FlowNodeDef {
			id: node2_id,
			flow: flow2.id,
			node_type: 1,
			data: Blob::from([0x02u8].as_slice()),
		};
		CatalogStore::create_flow_node(&mut txn, &node2).unwrap();

		// Verify nodes are in correct flows
		let result1 = CatalogStore::get_flow_node(&mut txn, node1_id).unwrap();
		let result2 = CatalogStore::get_flow_node(&mut txn, node2_id).unwrap();

		assert_eq!(result1.flow, flow1.id);
		assert_eq!(result2.flow, flow2.id);
	}

	#[test]
	fn test_node_appears_in_index() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node_id = next_flow_node_id(&mut txn).unwrap();
		let node_def = FlowNodeDef {
			id: node_id,
			flow: flow.id,
			node_type: 1,
			data: Blob::from([0x01u8].as_slice()),
		};

		CatalogStore::create_flow_node(&mut txn, &node_def).unwrap();

		// Verify node appears in flow index by listing nodes for flow
		let nodes = CatalogStore::list_flow_nodes_by_flow(&mut txn, flow.id).unwrap();
		assert_eq!(nodes.len(), 1);
		assert_eq!(nodes[0].id, node_id);
	}
}
