// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		change::CatalogTrackFlowNodeChangeOperations,
		flow::{FlowId, FlowNodeId},
	},
	key::{
		EncodableKey,
		flow_node::{FlowNodeByFlowKey, FlowNodeKey},
		flow_node_state::FlowNodeStateKey,
		operator_state::{Keyspace, OperatorStateKey},
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
			let entry = entry?;

			if let Some(decoded) = FlowNodeStateKey::decode(&entry.key)
				&& preserved_keyspace(&decoded.key)
			{
				continue;
			}
			state_keys.push(entry.key.clone());
		}
		drop(state_stream);
		for key in state_keys {
			txn.remove(&key)?;
		}
		Ok(())
	}

	#[inline]
	fn unlink_node(txn: &mut AdminTransaction, node_id: FlowNodeId, flow: FlowId) -> Result<()> {
		txn.remove(&FlowNodeKey::encoded(node_id))?;
		txn.remove(&FlowNodeByFlowKey::encoded(flow, node_id))?;
		Ok(())
	}
}

fn preserved_keyspace(inner: &[u8]) -> bool {
	OperatorStateKey::decode_inner(inner).is_some_and(|(_, keyspace, _)| {
		keyspace == Keyspace::ROW_NUMBER_MAPPING
			|| keyspace == Keyspace::NODE_COUNTER
			|| keyspace == Keyspace::WINDOW_META
			|| keyspace == Keyspace::GROUP_DICTIONARY
			|| keyspace == Keyspace::GROUP_RECORD
			|| keyspace == Keyspace::GATE_VISIBILITY
	})
}

#[cfg(test)]
pub mod tests {
	use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
	use reifydb_core::{
		interface::catalog::flow::FlowNodeId,
		key::{
			flow_node_state::FlowNodeStateKey,
			operator_state::{GroupId, Keyspace, OperatorStateKey},
		},
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

		assert!(CatalogStore::find_flow_node(&mut Transaction::Admin(&mut txn), node.id).unwrap().is_some());

		CatalogStore::drop_flow_node(&mut txn, node.id).unwrap();

		assert!(CatalogStore::find_flow_node(&mut Transaction::Admin(&mut txn), node.id).unwrap().is_none());
	}

	#[test]
	fn test_drop_node_removes_from_index() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]);

		let nodes = CatalogStore::list_flow_nodes_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(nodes.len(), 1);

		CatalogStore::drop_flow_node(&mut txn, node.id).unwrap();

		let nodes = CatalogStore::list_flow_nodes_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert!(nodes.is_empty());
	}

	#[test]
	fn test_drop_nonexistent_node() {
		// Dropping a node that never existed is a no-op, not an error.
		let mut txn = create_test_admin_transaction();

		CatalogStore::drop_flow_node(&mut txn, FlowNodeId(999)).unwrap();
	}

	#[test]
	fn test_drop_one_node_keeps_others() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]);

		CatalogStore::drop_flow_node(&mut txn, node1.id).unwrap();

		assert!(CatalogStore::find_flow_node(&mut Transaction::Admin(&mut txn), node1.id).unwrap().is_none());
		assert!(CatalogStore::find_flow_node(&mut Transaction::Admin(&mut txn), node2.id).unwrap().is_some());

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

		let dummy_value = EncodedRow(CowVec::new(vec![42u8]));
		txn.set(&FlowNodeStateKey::encoded(node.id, vec![1u8]), dummy_value.clone()).unwrap();
		txn.set(&FlowNodeStateKey::encoded(node.id, vec![1u8]), dummy_value.clone()).unwrap();

		assert!(txn.get(&FlowNodeStateKey::encoded(node.id, vec![1u8])).unwrap().is_some());
		assert!(txn.get(&FlowNodeStateKey::encoded(node.id, vec![1u8])).unwrap().is_some());

		CatalogStore::drop_flow_node(&mut txn, node.id).unwrap();

		assert!(txn.get(&FlowNodeStateKey::encoded(node.id, vec![1u8])).unwrap().is_none());
		assert!(txn.get(&FlowNodeStateKey::encoded(node.id, vec![1u8])).unwrap().is_none());

		assert!(CatalogStore::find_flow_node(&mut Transaction::Admin(&mut txn), node.id).unwrap().is_none());
	}

	#[test]
	fn test_drop_flow_node_preserves_row_number_counter() {
		// The counter is a monotonic sequence: resetting it on drop lets a re-created node
		// hand out numbers that downstream rows already carry.
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]);

		let counter_key = structured(node.id, GroupId::NODE_SCOPE, Keyspace::NODE_COUNTER, vec![]);
		let other_key = FlowNodeStateKey::encoded(node.id, vec![0x42, 0xAB]);

		let dummy = EncodedRow(CowVec::new(vec![42u8]));
		txn.set(&counter_key, dummy.clone()).unwrap();
		txn.set(&other_key, dummy.clone()).unwrap();

		assert!(txn.get(&counter_key).unwrap().is_some());
		assert!(txn.get(&other_key).unwrap().is_some());

		CatalogStore::drop_flow_node(&mut txn, node.id).unwrap();

		assert!(txn.get(&other_key).unwrap().is_none(), "unrelated internal state must be cleared on drop");
		assert!(
			txn.get(&counter_key).unwrap().is_some(),
			"drop_flow_node must preserve the RowNumberProvider counter"
		);
	}

	#[test]
	fn test_drop_flow_node_preserves_row_number_mapping() {
		// Re-allocating the same encoded key to a different row number after a drop
		// corrupts any downstream that still holds the old number.
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]);

		let mapping_key =
			structured(node.id, GroupId(3), Keyspace::ROW_NUMBER_MAPPING, b"some_user_key_bytes".to_vec());

		let dummy = EncodedRow(CowVec::new(vec![42u8]));
		txn.set(&mapping_key, dummy.clone()).unwrap();

		assert!(txn.get(&mapping_key).unwrap().is_some());

		CatalogStore::drop_flow_node(&mut txn, node.id).unwrap();

		assert!(
			txn.get(&mapping_key).unwrap().is_some(),
			"drop_flow_node must preserve the RowNumberProvider per-key mapping"
		);
	}

	fn structured(node: FlowNodeId, group: GroupId, keyspace: Keyspace, suffix: Vec<u8>) -> EncodedKey {
		FlowNodeStateKey::encoded(
			node,
			OperatorStateKey::inner_encoded(group, keyspace, suffix).as_slice().to_vec(),
		)
	}

	#[test]
	fn drop_preserves_identity_addressed_through_a_structured_key() {
		// Group-scoped operators address identity through structured keys, not raw tag bytes, so a
		// first-byte test misses them. The dictionary and record go with the mapping: erase them and
		// the same logical key re-interns to a fresh group id, renumbering it anyway.
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);
		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]);

		let dummy = EncodedRow(CowVec::new(vec![42u8]));
		let preserved = [
			structured(node.id, GroupId(7), Keyspace::ROW_NUMBER_MAPPING, vec![9]),
			structured(node.id, GroupId::NODE_SCOPE, Keyspace::NODE_COUNTER, vec![0]),
			structured(node.id, GroupId::NODE_SCOPE, Keyspace::WINDOW_META, b"partition".to_vec()),
			structured(node.id, GroupId::NODE_SCOPE, Keyspace::GROUP_DICTIONARY, b"group".to_vec()),
			structured(node.id, GroupId::NODE_SCOPE, Keyspace::GROUP_RECORD, 7u64.to_be_bytes().to_vec()),
		];
		for key in &preserved {
			txn.set(key, dummy.clone()).unwrap();
		}

		CatalogStore::drop_flow_node(&mut txn, node.id).unwrap();

		for key in &preserved {
			assert!(
				txn.get(key).unwrap().is_some(),
				"a structured identity key must survive drop_flow_node exactly as its raw-tag \
				 predecessor did: {key:?}"
			);
		}
	}

	#[test]
	fn drop_still_erases_a_groups_data_under_a_structured_key() {
		// Everything outside the preserved keyspaces must go, or the rule degenerates into
		// "preserve everything structured" and a dropped node leaks its accumulators forever.
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);
		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]);

		let dummy = EncodedRow(CowVec::new(vec![42u8]));
		let accumulator = structured(node.id, GroupId(7), Keyspace::ACCUMULATOR, vec![0, 0, 0, 0, 0, 0, 0, 1]);
		let engine_meta = structured(node.id, GroupId(7), Keyspace::ENGINE_META, vec![]);
		txn.set(&accumulator, dummy.clone()).unwrap();
		txn.set(&engine_meta, dummy).unwrap();

		CatalogStore::drop_flow_node(&mut txn, node.id).unwrap();

		assert!(txn.get(&accumulator).unwrap().is_none(), "a group's accumulator must not outlive its node");
		assert!(txn.get(&engine_meta).unwrap().is_none(), "a group's window meta must not outlive its node");
	}

	#[test]
	fn test_drop_flow_node_preserves_window_meta() {
		// Losing per-partition meta lets late events for closed windows be re-processed,
		// contaminating fresh window slot maps.
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]);

		let window_meta_key =
			structured(node.id, GroupId::NODE_SCOPE, Keyspace::WINDOW_META, b"some_group_encoded".to_vec());

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
		// Losing the marker lets a previously-suppressed row pass the gate again.
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01]);

		let gate_inner = OperatorStateKey::inner_encoded(
			GroupId::NODE_SCOPE,
			Keyspace::GATE_VISIBILITY,
			42u64.to_be_bytes(),
		);
		let gate_key = FlowNodeStateKey::encoded(node.id, gate_inner.as_slice());

		let dummy = EncodedRow(CowVec::new(vec![1u8]));
		txn.set(&gate_key, dummy.clone()).unwrap();

		assert!(txn.get(&gate_key).unwrap().is_some());

		CatalogStore::drop_flow_node(&mut txn, node.id).unwrap();

		assert!(txn.get(&gate_key).unwrap().is_some(), "drop_flow_node must preserve gate visibility markers");
	}
}
