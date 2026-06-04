// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		change::CatalogTrackFlowNodeChangeOperations,
		flow::{FlowId, FlowNodeId},
	},
	key::{
		EncodableKey,
		flow_node::{FlowNodeByFlowKey, FlowNodeKey},
		flow_node_internal_state::FlowNodeInternalStateKey,
		flow_node_state::FlowNodeStateKey,
		retention_strategy::OperatorRetentionStrategyKey,
	},
};
use reifydb_transaction::{
	multi::RangeScope,
	transaction::{Transaction, admin::AdminTransaction},
};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_flow_node(txn: &mut AdminTransaction, node_id: FlowNodeId) -> Result<()> {
		let Some(node_def) = CatalogStore::find_flow_node(&mut Transaction::Admin(&mut *txn), node_id)? else {
			return Ok(());
		};

		Self::delete_node_state(txn, node_id)?;
		Self::delete_internal_state(txn, node_id)?;
		Self::unlink_node(txn, node_id, node_def.flow)?;
		txn.track_flow_node_deleted(node_def)?;
		Ok(())
	}

	#[inline]
	fn delete_node_state(txn: &mut AdminTransaction, node_id: FlowNodeId) -> Result<()> {
		let state_range = FlowNodeStateKey::node_range(node_id);
		let mut state_stream = txn.range(state_range, RangeScope::All, 1024)?;
		let mut state_keys = Vec::new();
		for entry in state_stream.by_ref() {
			state_keys.push(entry?.key.clone());
		}
		drop(state_stream);
		for key in state_keys {
			txn.remove(&key)?;
		}
		Ok(())
	}

	#[inline]
	fn delete_internal_state(txn: &mut AdminTransaction, node_id: FlowNodeId) -> Result<()> {
		let internal_range = FlowNodeInternalStateKey::node_range(node_id);
		let mut internal_stream = txn.range(internal_range, RangeScope::All, 1024)?;
		let mut internal_keys = Vec::new();
		for entry in internal_stream.by_ref() {
			let entry = entry?;

			if let Some(decoded) = FlowNodeInternalStateKey::decode(&entry.key)
				&& (decoded.is_row_number_counter()
					|| decoded.is_row_number_mapping() || decoded.is_window_meta()
					|| decoded.is_gate_visibility())
			{
				continue;
			}
			internal_keys.push(entry.key.clone());
		}
		drop(internal_stream);
		for key in internal_keys {
			txn.remove(&key)?;
		}
		Ok(())
	}

	#[inline]
	fn unlink_node(txn: &mut AdminTransaction, node_id: FlowNodeId, flow: FlowId) -> Result<()> {
		txn.remove(&OperatorRetentionStrategyKey::encoded(node_id))?;
		txn.remove(&FlowNodeKey::encoded(node_id))?;
		txn.remove(&FlowNodeByFlowKey::encoded(flow, node_id))?;
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		encoded::row::EncodedRow,
		interface::catalog::flow::FlowNodeId,
		key::{flow_node_internal_state::FlowNodeInternalStateKey, flow_node_state::FlowNodeStateKey},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::util::cowvec::CowVec;

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
		let dummy_value = EncodedRow(CowVec::new(vec![42u8]));
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

	#[test]
	fn test_drop_flow_node_preserves_row_number_counter() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]);

		let counter_key = FlowNodeInternalStateKey::encoded(
			node.id,
			vec![FlowNodeInternalStateKey::ROW_NUMBER_COUNTER_TAG],
		);
		// An unrelated FlowNodeInternalState entry (not counter, not mapping)
		// to confirm normal cleanup still happens.
		let other_key = FlowNodeInternalStateKey::encoded(node.id, vec![0x42, 0xAB]);

		let dummy = EncodedRow(CowVec::new(vec![42u8]));
		txn.set(&counter_key, dummy.clone()).unwrap();
		txn.set(&other_key, dummy.clone()).unwrap();

		assert!(txn.get(&counter_key).unwrap().is_some());
		assert!(txn.get(&other_key).unwrap().is_some());

		CatalogStore::drop_flow_node(&mut txn, node.id).unwrap();

		// Unrelated entry is cleared (existing contract).
		assert!(txn.get(&other_key).unwrap().is_none(), "unrelated internal state must be cleared on drop");
		// The counter survives - this is the new contract. Row-number
		// state is a monotonic sequence and must never be cleaned.
		assert!(
			txn.get(&counter_key).unwrap().is_some(),
			"drop_flow_node must preserve the RowNumberProvider counter"
		);
	}

	#[test]
	fn test_drop_flow_node_preserves_row_number_mapping() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]);

		// A per-key mapping (b'M' inner-tag family). Re-allocating the same
		// encoded_key to a different row_number after a drop would corrupt
		// any downstream that still holds the old number.
		let mut mapping_inner = vec![FlowNodeInternalStateKey::ROW_NUMBER_MAPPING_TAG];
		mapping_inner.extend_from_slice(b"some_user_key_bytes");
		let mapping_key = FlowNodeInternalStateKey::encoded(node.id, mapping_inner);

		let dummy = EncodedRow(CowVec::new(vec![42u8]));
		txn.set(&mapping_key, dummy.clone()).unwrap();

		assert!(txn.get(&mapping_key).unwrap().is_some());

		CatalogStore::drop_flow_node(&mut txn, node.id).unwrap();

		assert!(
			txn.get(&mapping_key).unwrap().is_some(),
			"drop_flow_node must preserve the RowNumberProvider per-key mapping"
		);
	}

	#[test]
	fn test_drop_flow_node_preserves_window_meta() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]);

		// A windowed-driver per-group meta entry (b'W' inner-tag family).
		// Loss would let late events for closed windows be re-processed,
		// contaminating fresh window slot maps.
		let mut window_meta_inner = vec![FlowNodeInternalStateKey::WINDOW_META_TAG];
		window_meta_inner.extend_from_slice(b"some_group_encoded");
		let window_meta_key = FlowNodeInternalStateKey::encoded(node.id, window_meta_inner);

		let dummy = EncodedRow(CowVec::new(vec![42u8]));
		txn.set(&window_meta_key, dummy.clone()).unwrap();

		assert!(txn.get(&window_meta_key).unwrap().is_some());

		CatalogStore::drop_flow_node(&mut txn, node.id).unwrap();

		assert!(
			txn.get(&window_meta_key).unwrap().is_some(),
			"drop_flow_node must preserve windowed-driver meta entries (high_water etc.)"
		);
	}

	#[test]
	fn test_drop_flow_node_preserves_gate_visibility() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]);

		// A gate-operator visibility marker (b'G' inner-tag family).
		// Loss would let a previously-suppressed row pass the gate again.
		let mut gate_inner = vec![FlowNodeInternalStateKey::GATE_VISIBILITY_TAG];
		gate_inner.extend_from_slice(&42u64.to_be_bytes());
		let gate_key = FlowNodeInternalStateKey::encoded(node.id, gate_inner);

		let dummy = EncodedRow(CowVec::new(vec![1u8]));
		txn.set(&gate_key, dummy.clone()).unwrap();

		assert!(txn.get(&gate_key).unwrap().is_some());

		CatalogStore::drop_flow_node(&mut txn, node.id).unwrap();

		assert!(txn.get(&gate_key).unwrap().is_some(), "drop_flow_node must preserve gate visibility markers");
	}
}
