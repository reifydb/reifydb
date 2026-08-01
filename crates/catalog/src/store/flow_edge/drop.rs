// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{change::CatalogTrackFlowEdgeChangeOperations, flow::FlowEdgeId},
	key::flow_edge::{FlowEdgeByFlowKey, FlowEdgeKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_flow_edge(txn: &mut AdminTransaction, edge_id: FlowEdgeId) -> Result<()> {
		let edge = CatalogStore::find_flow_edge(&mut Transaction::Admin(&mut *txn), edge_id)?;

		if let Some(edge_def) = edge {
			txn.remove(&FlowEdgeKey::encoded(edge_id))?;

			txn.remove(&FlowEdgeByFlowKey::encoded(edge_def.flow, edge_id))?;

			txn.track_flow_edge_deleted(edge_def)?;
		}

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::flow::FlowEdgeId;
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow_edge, create_flow_node, create_namespace, ensure_test_flow},
	};

	#[test]
	fn test_drop_flow_edge() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]);
		let edge = create_flow_edge(&mut txn, flow.id, node1.id, node2.id);

		assert!(CatalogStore::find_flow_edge(&mut Transaction::Admin(&mut txn), edge.id).unwrap().is_some());

		CatalogStore::drop_flow_edge(&mut txn, edge.id).unwrap();

		assert!(CatalogStore::find_flow_edge(&mut Transaction::Admin(&mut txn), edge.id).unwrap().is_none());
	}

	#[test]
	fn test_drop_edge_removes_from_index() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]);
		let edge = create_flow_edge(&mut txn, flow.id, node1.id, node2.id);

		let edges = CatalogStore::list_flow_edges_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(edges.len(), 1);

		CatalogStore::drop_flow_edge(&mut txn, edge.id).unwrap();

		let edges = CatalogStore::list_flow_edges_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert!(edges.is_empty());
	}

	#[test]
	fn test_drop_nonexistent_edge() {
		// Dropping an edge that never existed is a no-op, not an error.
		let mut txn = create_test_admin_transaction();

		CatalogStore::drop_flow_edge(&mut txn, FlowEdgeId(999)).unwrap();
	}

	#[test]
	fn test_drop_one_edge_keeps_others() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]);
		let node3 = create_flow_node(&mut txn, flow.id, 5, &[0x03]);

		let edge1 = create_flow_edge(&mut txn, flow.id, node1.id, node2.id);
		let edge2 = create_flow_edge(&mut txn, flow.id, node2.id, node3.id);

		CatalogStore::drop_flow_edge(&mut txn, edge1.id).unwrap();

		assert!(CatalogStore::find_flow_edge(&mut Transaction::Admin(&mut txn), edge1.id).unwrap().is_none());
		assert!(CatalogStore::find_flow_edge(&mut Transaction::Admin(&mut txn), edge2.id).unwrap().is_some());

		let edges = CatalogStore::list_flow_edges_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(edges.len(), 1);
		assert_eq!(edges[0].id, edge2.id);
	}
}
