// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use flow_node::LAYOUT;
use reifydb_core::{
	interface::{FlowId, FlowNodeDef, FlowNodeId, QueryTransaction},
	key::FlowNodeKey,
};

use crate::{CatalogStore, store::flow_node::layout::flow_node};

impl CatalogStore {
	pub async fn find_flow_node(
		txn: &mut impl QueryTransaction,
		node_id: FlowNodeId,
	) -> crate::Result<Option<FlowNodeDef>> {
		let Some(multi) = txn.get(&FlowNodeKey::encoded(node_id)).await? else {
			return Ok(None);
		};

		let row = multi.values;
		let id = FlowNodeId(LAYOUT.get_u64(&row, flow_node::ID));
		let flow = FlowId(LAYOUT.get_u64(&row, flow_node::FLOW));
		let node_type = LAYOUT.get_u8(&row, flow_node::TYPE);
		let data = LAYOUT.get_blob(&row, flow_node::DATA).clone();

		Ok(Some(FlowNodeDef {
			id,
			flow,
			node_type,
			data,
		}))
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
	async fn test_find_flow_node_ok() {
		let mut txn = create_test_command_transaction().await;
		let _namespace = create_namespace(&mut txn, "test_namespace").await;
		let flow = ensure_test_flow(&mut txn).await;

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01, 0x02, 0x03]).await;

		let result = CatalogStore::find_flow_node(&mut txn, node.id).await.unwrap();
		assert!(result.is_some());
		let found = result.unwrap();
		assert_eq!(found.id, node.id);
		assert_eq!(found.flow, flow.id);
		assert_eq!(found.node_type, 1);
	}

	#[tokio::test]
	async fn test_find_flow_node_not_found() {
		let mut txn = create_test_command_transaction().await;

		let result = CatalogStore::find_flow_node(&mut txn, FlowNodeId(999)).await.unwrap();
		assert!(result.is_none());
	}
}
