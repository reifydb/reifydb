// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{flow::FlowId, id::NamespaceId},
	key::{flow::FlowKey, namespace_flow::NamespaceFlowKey},
};
use reifydb_transaction::transaction::command::CommandTransaction;

use crate::CatalogStore;

impl CatalogStore {
	/// Delete a flow by its name within a namespace.
	///
	/// This is useful for cleaning up flows associated with subscriptions,
	/// where the flow name is derived from the subscription ID.
	pub(crate) fn delete_flow_by_name(
		txn: &mut CommandTransaction,
		namespace: NamespaceId,
		name: &str,
	) -> crate::Result<()> {
		// Find the flow by name
		if let Some(flow) = CatalogStore::find_flow_by_name(txn, namespace, name)? {
			// Delete it using the existing delete_flow function
			CatalogStore::delete_flow(txn, flow.id)?;
		}
		Ok(())
	}

	pub(crate) fn delete_flow(txn: &mut CommandTransaction, flow_id: FlowId) -> crate::Result<()> {
		// Get the flow to find namespace for index deletion
		let flow_def = CatalogStore::find_flow(txn, flow_id)?;

		if let Some(flow) = flow_def {
			// Step 1: Delete all nodes for this flow
			let nodes = CatalogStore::list_flow_nodes_by_flow(txn, flow_id)?;
			for node in nodes {
				CatalogStore::delete_flow_node(txn, node.id)?;
			}

			// Step 2: Delete all edges for this flow
			let edges = CatalogStore::list_flow_edges_by_flow(txn, flow_id)?;
			for edge in edges {
				CatalogStore::delete_flow_edge(txn, edge.id)?;
			}

			// Step 3: Delete from namespace index
			txn.remove(&NamespaceFlowKey::encoded(flow.namespace, flow_id))?;

			// Step 4: Delete from main flow table
			txn.remove(&FlowKey::encoded(flow_id))?;
		}

		Ok(())
	}
}
