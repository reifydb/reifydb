// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::flow::FlowId,
	key::{flow::FlowKey, namespace_flow::NamespaceFlowKey},
};
use reifydb_transaction::standard::command::StandardCommandTransaction;

use crate::CatalogStore;

impl CatalogStore {
	pub fn delete_flow(txn: &mut StandardCommandTransaction, flow_id: FlowId) -> crate::Result<()> {
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
