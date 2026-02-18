// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use flow_node::SCHEMA;
use reifydb_core::{
	interface::catalog::flow::{FlowId, FlowNodeDef, FlowNodeId},
	key::flow_node::FlowNodeKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, store::flow_node::schema::flow_node};

impl CatalogStore {
	pub(crate) fn find_flow_node(
		rx: &mut Transaction<'_>,
		node_id: FlowNodeId,
	) -> crate::Result<Option<FlowNodeDef>> {
		let Some(multi) = rx.get(&FlowNodeKey::encoded(node_id))? else {
			return Ok(None);
		};

		let row = multi.values;
		let id = FlowNodeId(SCHEMA.get_u64(&row, flow_node::ID));
		let flow = FlowId(SCHEMA.get_u64(&row, flow_node::FLOW));
		let node_type = SCHEMA.get_u8(&row, flow_node::TYPE);
		let data = SCHEMA.get_blob(&row, flow_node::DATA).clone();

		Ok(Some(FlowNodeDef {
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
	use reifydb_engine::test_utils::create_test_admin_transaction;
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
