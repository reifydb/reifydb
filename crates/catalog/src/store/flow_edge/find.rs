// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use flow_edge::SHAPE;
use reifydb_core::{
	interface::catalog::flow::{FlowEdge, FlowEdgeId, FlowId, OperatorId},
	key::flow_edge::FlowEdgeKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::flow_edge::shape::flow_edge};

impl CatalogStore {
	pub(crate) fn find_flow_edge(rx: &mut Transaction<'_>, edge: FlowEdgeId) -> Result<Option<FlowEdge>> {
		let Some(multi) = rx.get(&FlowEdgeKey::encoded(edge))? else {
			return Ok(None);
		};

		let bytes = multi.bytes;
		let id = FlowEdgeId(SHAPE.get::<u64>(&bytes, flow_edge::ID));
		let flow = FlowId(SHAPE.get::<u64>(&bytes, flow_edge::FLOW));
		let source = OperatorId(SHAPE.get::<u64>(&bytes, flow_edge::SOURCE));
		let target = OperatorId(SHAPE.get::<u64>(&bytes, flow_edge::TARGET));

		Ok(Some(FlowEdge {
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
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow_edge, create_namespace, create_operator, ensure_test_flow},
	};

	#[test]
	fn test_find_flow_edge_ok() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_operator(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_operator(&mut txn, flow.id, 4, &[0x02]);
		let edge = create_flow_edge(&mut txn, flow.id, node1.id, node2.id);

		let result = CatalogStore::find_flow_edge(&mut Transaction::Admin(&mut txn), edge.id).unwrap();
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

		let result = CatalogStore::find_flow_edge(&mut Transaction::Admin(&mut txn), FlowEdgeId(999)).unwrap();
		assert!(result.is_none());
	}
}
