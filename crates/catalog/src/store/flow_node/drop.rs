// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	key::{
		flow_node::{FlowNodeByFlowKey, FlowNodeKey},
		flow_node_internal_state::FlowNodeInternalStateKey,
		flow_node_state::FlowNodeStateKey,
		retention_policy::OperatorRetentionPolicyKey,
	},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_flow_node(txn: &mut AdminTransaction, node_id: FlowNodeId) -> Result<()> {
		// First, get the node to find the flow ID for index deletion
		let node = CatalogStore::find_flow_node(&mut Transaction::Admin(&mut *txn), node_id)?;

		if let Some(node_def) = node {
			// Clean up flow node state entries
			let state_range = FlowNodeStateKey::node_range(node_id);
			let mut state_stream = txn.range(state_range, 1024)?;
			let mut state_keys = Vec::new();
			while let Some(entry) = state_stream.next() {
				state_keys.push(entry?.key.clone());
			}
			drop(state_stream);
			for key in state_keys {
				txn.remove(&key)?;
			}

			// Clean up flow node internal state entries
			let internal_range = FlowNodeInternalStateKey::node_range(node_id);
			let mut internal_stream = txn.range(internal_range, 1024)?;
			let mut internal_keys = Vec::new();
			while let Some(entry) = internal_stream.next() {
				internal_keys.push(entry?.key.clone());
			}
			drop(internal_stream);
			for key in internal_keys {
				txn.remove(&key)?;
			}

			// Clean up operator retention policy
			txn.remove(&OperatorRetentionPolicyKey::encoded(node_id))?;

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
	use reifydb_core::{
		encoded::encoded::EncodedValues,
		interface::catalog::flow::FlowNodeId,
		key::{flow_node_internal_state::FlowNodeInternalStateKey, flow_node_state::FlowNodeStateKey},
	};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::util::cowvec::CowVec;

	use crate::{
		CatalogStore,
		test_utils::{create_flow_node, create_namespace, ensure_test_flow},
	};

	#[test]
	fn test_drop_flow_node() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]);

		// Node should exist
		assert!(CatalogStore::find_flow_node(&mut Transaction::Admin(&mut txn), node.id).unwrap().is_some());

		// Delete node
		CatalogStore::drop_flow_node(&mut txn, node.id).unwrap();

		// Node should no longer exist
		assert!(CatalogStore::find_flow_node(&mut Transaction::Admin(&mut txn), node.id).unwrap().is_none());
	}

	#[test]
	fn test_drop_node_removes_from_index() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]);

		// Node should be in flow index
		let nodes = CatalogStore::list_flow_nodes_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(nodes.len(), 1);

		// Delete node
		CatalogStore::drop_flow_node(&mut txn, node.id).unwrap();

		// Node should be removed from flow index
		let nodes = CatalogStore::list_flow_nodes_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert!(nodes.is_empty());
	}

	#[test]
	fn test_drop_nonexistent_node() {
		let mut txn = create_test_admin_transaction();

		// Deleting a non-existent node should succeed silently
		CatalogStore::drop_flow_node(&mut txn, FlowNodeId(999)).unwrap();
	}

	#[test]
	fn test_drop_one_node_keeps_others() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]);

		// Delete first node
		CatalogStore::drop_flow_node(&mut txn, node1.id).unwrap();

		// First node should be gone, second should remain
		assert!(CatalogStore::find_flow_node(&mut Transaction::Admin(&mut txn), node1.id).unwrap().is_none());
		assert!(CatalogStore::find_flow_node(&mut Transaction::Admin(&mut txn), node2.id).unwrap().is_some());

		// List should only have second node
		let nodes = CatalogStore::list_flow_nodes_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(nodes.len(), 1);
		assert_eq!(nodes[0].id, node2.id);
	}

	#[test]
	fn test_drop_flow_node_cleans_up_state() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]);

		// Write state entries
		let dummy_value = EncodedValues(CowVec::new(vec![42u8]));
		txn.set(&FlowNodeStateKey::encoded(node.id, vec![1u8]), dummy_value.clone()).unwrap();
		txn.set(&FlowNodeInternalStateKey::encoded(node.id, vec![1u8]), dummy_value.clone()).unwrap();

		// Verify state exists before drop
		assert!(txn.get(&FlowNodeStateKey::encoded(node.id, vec![1u8])).unwrap().is_some());
		assert!(txn.get(&FlowNodeInternalStateKey::encoded(node.id, vec![1u8])).unwrap().is_some());

		// Drop the node
		CatalogStore::drop_flow_node(&mut txn, node.id).unwrap();

		// Verify state is cleaned up
		assert!(txn.get(&FlowNodeStateKey::encoded(node.id, vec![1u8])).unwrap().is_none());
		assert!(txn.get(&FlowNodeInternalStateKey::encoded(node.id, vec![1u8])).unwrap().is_none());

		// Verify node itself is gone
		assert!(CatalogStore::find_flow_node(&mut Transaction::Admin(&mut txn), node.id).unwrap().is_none());
	}
}
