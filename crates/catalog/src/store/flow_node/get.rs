// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Error,
	interface::{FlowNodeDef, FlowNodeId, QueryTransaction},
};
use reifydb_type::internal;

use crate::CatalogStore;

impl CatalogStore {
	pub async fn get_flow_node(txn: &mut impl QueryTransaction, node_id: FlowNodeId) -> crate::Result<FlowNodeDef> {
		CatalogStore::find_flow_node(txn, node_id).await?.ok_or_else(|| {
			Error(internal!(
				"Flow node with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				node_id
			))
		})
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
	async fn test_get_flow_node_ok() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace").await;
		let flow = ensure_test_flow(&mut txn).await;

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01, 0x02, 0x03]).await;

		let result = CatalogStore::get_flow_node(&mut txn, node.id).await.unwrap();
		assert_eq!(result.id, node.id);
		assert_eq!(result.flow, flow.id);
		assert_eq!(result.node_type, 1);
		assert_eq!(result.data.as_ref(), &[0x01, 0x02, 0x03]);
	}

	#[tokio::test]
	async fn test_get_flow_node_not_found() {
		let mut txn = create_test_command_transaction();

		let err = CatalogStore::get_flow_node(&mut txn, FlowNodeId(999)).await.unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("FlowNodeId(999)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
