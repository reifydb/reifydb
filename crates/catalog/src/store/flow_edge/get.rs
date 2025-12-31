// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	Error,
	interface::{FlowEdgeDef, FlowEdgeId, QueryTransaction},
};
use reifydb_type::internal;

use crate::CatalogStore;

impl CatalogStore {
	pub async fn get_flow_edge(txn: &mut impl QueryTransaction, edge_id: FlowEdgeId) -> crate::Result<FlowEdgeDef> {
		CatalogStore::find_flow_edge(txn, edge_id).await?.ok_or_else(|| {
			Error(internal!(
				"Flow edge with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				edge_id
			))
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::FlowEdgeId;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow_edge, create_flow_node, create_namespace, ensure_test_flow},
	};

	#[tokio::test]
	async fn test_get_flow_edge_ok() {
		let mut txn = create_test_command_transaction().await;
		let _namespace = create_namespace(&mut txn, "test_namespace").await;
		let flow = ensure_test_flow(&mut txn).await;

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]).await;
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]).await;
		let edge = create_flow_edge(&mut txn, flow.id, node1.id, node2.id).await;

		let result = CatalogStore::get_flow_edge(&mut txn, edge.id).await.unwrap();
		assert_eq!(result.id, edge.id);
		assert_eq!(result.flow, flow.id);
		assert_eq!(result.source, node1.id);
		assert_eq!(result.target, node2.id);
	}

	#[tokio::test]
	async fn test_get_flow_edge_not_found() {
		let mut txn = create_test_command_transaction().await;

		let err = CatalogStore::get_flow_edge(&mut txn, FlowEdgeId(999)).await.unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("FlowEdgeId(999)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
