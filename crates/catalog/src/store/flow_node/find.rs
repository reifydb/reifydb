// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use flow_node::SHAPE;
use reifydb_core::{
	interface::catalog::flow::{FlowId, FlowNode, FlowNodeId},
	key::flow_node::FlowNodeKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::flow_node::shape::flow_node};

impl CatalogStore {
	pub(crate) fn find_flow_node(rx: &mut Transaction<'_>, node_id: FlowNodeId) -> Result<Option<FlowNode>> {
		let Some(multi) = rx.get(&FlowNodeKey::encoded(node_id))? else {
			return Ok(None);
		};

		let row = multi.row;
		let id = FlowNodeId(SHAPE.get_u64(&row, flow_node::ID));
		let flow = FlowId(SHAPE.get_u64(&row, flow_node::FLOW));
		let node_type = SHAPE.get_u8(&row, flow_node::TYPE);
		let data = SHAPE.get_blob(&row, flow_node::DATA).clone();

		Ok(Some(FlowNode {
			id,
			flow,
			node_type,
			data,
		}))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::flow::FlowNodeId;
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow_node, create_namespace, ensure_test_flow},
	};

	#[test]
	fn test_find_flow_node_ok() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01, 0x02, 0x03]);

		let result = CatalogStore::find_flow_node(&mut Transaction::Admin(&mut txn), node.id).unwrap();
		assert!(result.is_some());
		let found = result.unwrap();
		assert_eq!(found.id, node.id);
		assert_eq!(found.flow, flow.id);
		assert_eq!(found.node_type, 1);
	}

	#[test]
	fn test_find_flow_node_not_found() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::find_flow_node(&mut Transaction::Admin(&mut txn), FlowNodeId(999)).unwrap();
		assert!(result.is_none());
	}
}
