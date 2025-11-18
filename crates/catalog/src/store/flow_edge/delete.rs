// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{CommandTransaction, EncodableKey, FlowEdgeId},
	key::{FlowEdgeByFlowKey, FlowEdgeKey},
};

use crate::CatalogStore;

impl CatalogStore {
	pub fn delete_flow_edge(txn: &mut impl CommandTransaction, edge_id: FlowEdgeId) -> crate::Result<()> {
		// First, get the edge to find the flow ID for index deletion
		let edge = CatalogStore::find_flow_edge(txn, edge_id)?;

		if let Some(edge_def) = edge {
			// Delete from main flow_edge table
			txn.remove(&FlowEdgeKey {
				edge: edge_id,
			}
			.encode())?;

			// Delete from flow_edge_by_flow index
			txn.remove(&FlowEdgeByFlowKey {
				flow: edge_def.flow,
				edge: edge_id,
			}
			.encode())?;
		}

		Ok(())
	}
}
