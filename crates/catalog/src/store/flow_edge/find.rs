// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use flow_edge::LAYOUT;
use reifydb_core::{
	interface::{FlowEdgeDef, FlowEdgeId, FlowId, FlowNodeId, QueryTransaction},
	key::FlowEdgeKey,
};

use crate::{CatalogStore, store::flow_edge::layout::flow_edge};

impl CatalogStore {
	pub async fn find_flow_edge(
		txn: &mut impl QueryTransaction,
		edge: FlowEdgeId,
	) -> crate::Result<Option<FlowEdgeDef>> {
		let Some(multi) = txn.get(&FlowEdgeKey::encoded(edge)).await? else {
			return Ok(None);
		};

		let row = multi.values;
		let id = FlowEdgeId(LAYOUT.get_u64(&row, flow_edge::ID));
		let flow = FlowId(LAYOUT.get_u64(&row, flow_edge::FLOW));
		let source = FlowNodeId(LAYOUT.get_u64(&row, flow_edge::SOURCE));
		let target = FlowNodeId(LAYOUT.get_u64(&row, flow_edge::TARGET));

		Ok(Some(FlowEdgeDef {
			id,
			flow,
			source,
			target,
		}))
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
	async fn test_find_flow_edge_ok() {
		let mut txn = create_test_command_transaction().await;
		let _namespace = create_namespace(&mut txn, "test_namespace").await;
		let flow = ensure_test_flow(&mut txn).await;

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]).await;
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]).await;
		let edge = create_flow_edge(&mut txn, flow.id, node1.id, node2.id).await;

		let result = CatalogStore::find_flow_edge(&mut txn, edge.id).await.unwrap();
		assert!(result.is_some());
		let found = result.unwrap();
		assert_eq!(found.id, edge.id);
		assert_eq!(found.flow, flow.id);
		assert_eq!(found.source, node1.id);
		assert_eq!(found.target, node2.id);
	}

	#[tokio::test]
	async fn test_find_flow_edge_not_found() {
		let mut txn = create_test_command_transaction().await;

		let result = CatalogStore::find_flow_edge(&mut txn, FlowEdgeId(999)).await.unwrap();
		assert!(result.is_none());
	}
}
