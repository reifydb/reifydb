// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{flow::FlowId, id::NamespaceId},
		cdc::CdcConsumerId,
	},
	key::{
		cdc_consumer::CdcConsumerKey, flow::FlowKey, flow_version::FlowVersionKey,
		namespace_flow::NamespaceFlowKey,
	},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	/// Drop a flow by its name within a namespace.
	///
	/// This is useful for cleaning up flows associated with subscriptions,
	/// where the flow name is derived from the subscription ID.
	pub(crate) fn drop_flow_by_name(txn: &mut AdminTransaction, namespace: NamespaceId, name: &str) -> Result<()> {
		// Find the flow by name
		if let Some(flow) =
			CatalogStore::find_flow_by_name(&mut Transaction::Admin(&mut *txn), namespace, name)?
		{
			CatalogStore::drop_flow(txn, flow.id)?;
		}
		Ok(())
	}

	pub(crate) fn drop_flow(txn: &mut AdminTransaction, flow_id: FlowId) -> Result<()> {
		// Get the flow to find namespace for index deletion
		let flow_def = CatalogStore::find_flow(&mut Transaction::Admin(&mut *txn), flow_id)?;

		if let Some(flow) = flow_def {
			// Step 1: Drop all nodes for this flow
			let nodes = CatalogStore::list_flow_nodes_by_flow(&mut Transaction::Admin(&mut *txn), flow_id)?;
			for node in nodes {
				CatalogStore::drop_flow_node(txn, node.id)?;
			}

			// Step 2: Drop all edges for this flow
			let edges = CatalogStore::list_flow_edges_by_flow(&mut Transaction::Admin(&mut *txn), flow_id)?;
			for edge in edges {
				CatalogStore::drop_flow_edge(txn, edge.id)?;
			}

			// Step 3: Clean up flow version and CDC consumer
			txn.remove(&FlowVersionKey::encoded(flow_id))?;
			txn.remove(&CdcConsumerKey::encoded(CdcConsumerId::new(format!("flow:{}", flow_id.0))))?;

			// Step 4: Delete from namespace index
			txn.remove(&NamespaceFlowKey::encoded(flow.namespace, flow_id))?;

			// Step 5: Delete from main flow table
			txn.remove(&FlowKey::encoded(flow_id))?;
		}

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::flow::FlowId;
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow, create_flow_edge, create_flow_node, create_namespace},
	};

	#[test]
	fn test_drop_flow() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = create_flow(&mut txn, "test_namespace", "drop_test_flow");

		// Create nodes and edges
		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]);
		let edge = create_flow_edge(&mut txn, flow.id, node1.id, node2.id);

		// Verify flow, nodes, and edges exist
		assert!(CatalogStore::find_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap().is_some());
		assert!(CatalogStore::find_flow_node(&mut Transaction::Admin(&mut txn), node1.id).unwrap().is_some());
		assert!(CatalogStore::find_flow_node(&mut Transaction::Admin(&mut txn), node2.id).unwrap().is_some());
		assert!(CatalogStore::find_flow_edge(&mut Transaction::Admin(&mut txn), edge.id).unwrap().is_some());

		// Drop the flow
		CatalogStore::drop_flow(&mut txn, flow.id).unwrap();

		// Verify flow is gone
		assert!(CatalogStore::find_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap().is_none());

		// Verify all nodes are gone
		assert!(CatalogStore::find_flow_node(&mut Transaction::Admin(&mut txn), node1.id).unwrap().is_none());
		assert!(CatalogStore::find_flow_node(&mut Transaction::Admin(&mut txn), node2.id).unwrap().is_none());

		// Verify all edges are gone
		assert!(CatalogStore::find_flow_edge(&mut Transaction::Admin(&mut txn), edge.id).unwrap().is_none());
	}

	#[test]
	fn test_drop_nonexistent_flow() {
		let mut txn = create_test_admin_transaction();

		// Dropping a non-existent flow should succeed silently
		CatalogStore::drop_flow(&mut txn, FlowId(999)).unwrap();
	}

	#[test]
	fn test_drop_flow_by_name() {
		let mut txn = create_test_admin_transaction();
		let ns = create_namespace(&mut txn, "test_namespace");
		let _flow = create_flow(&mut txn, "test_namespace", "named_flow");

		// Verify flow exists
		assert!(CatalogStore::find_flow_by_name(&mut Transaction::Admin(&mut txn), ns.id, "named_flow")
			.unwrap()
			.is_some());

		// Drop by name
		CatalogStore::drop_flow_by_name(&mut txn, ns.id, "named_flow").unwrap();

		// Verify flow is gone
		assert!(CatalogStore::find_flow_by_name(&mut Transaction::Admin(&mut txn), ns.id, "named_flow")
			.unwrap()
			.is_none());
	}
}
