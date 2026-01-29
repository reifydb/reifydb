// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use flow_edge::SCHEMA;
use reifydb_core::{
	interface::catalog::flow::{FlowEdgeDef, FlowEdgeId, FlowId, FlowNodeId},
	key::flow_edge::FlowEdgeKey,
};
use reifydb_transaction::transaction::AsTransaction;

use crate::{CatalogStore, store::flow_edge::schema::flow_edge};

impl CatalogStore {
	pub(crate) fn find_flow_edge(
		rx: &mut impl AsTransaction,
		edge: FlowEdgeId,
	) -> crate::Result<Option<FlowEdgeDef>> {
		let mut txn = rx.as_transaction();
		let Some(multi) = txn.get(&FlowEdgeKey::encoded(edge))? else {
			return Ok(None);
		};

		let row = multi.values;
		let id = FlowEdgeId(SCHEMA.get_u64(&row, flow_edge::ID));
		let flow = FlowId(SCHEMA.get_u64(&row, flow_edge::FLOW));
		let source = FlowNodeId(SCHEMA.get_u64(&row, flow_edge::SOURCE));
		let target = FlowNodeId(SCHEMA.get_u64(&row, flow_edge::TARGET));

		Ok(Some(FlowEdgeDef {
			id,
			flow,
			source,
			target,
		}))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::flow::FlowEdgeId;
	use reifydb_engine::test_utils::create_test_admin_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow_edge, create_flow_node, create_namespace, ensure_test_flow},
	};

	#[test]
	fn test_find_flow_edge_ok() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]);
		let edge = create_flow_edge(&mut txn, flow.id, node1.id, node2.id);

		let result = CatalogStore::find_flow_edge(&mut txn, edge.id).unwrap();
		assert!(result.is_some());
		let found = result.unwrap();
		assert_eq!(found.id, edge.id);
		assert_eq!(found.flow, flow.id);
		assert_eq!(found.source, node1.id);
		assert_eq!(found.target, node2.id);
	}

	#[test]
	fn test_find_flow_edge_not_found() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::find_flow_edge(&mut txn, FlowEdgeId(999)).unwrap();
		assert!(result.is_none());
	}
}
