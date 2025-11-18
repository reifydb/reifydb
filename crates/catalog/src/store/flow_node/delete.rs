// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{CommandTransaction, EncodableKey, FlowNodeId},
	key::{FlowNodeByFlowKey, FlowNodeKey},
};

use crate::CatalogStore;

impl CatalogStore {
	pub fn delete_flow_node(txn: &mut impl CommandTransaction, node_id: FlowNodeId) -> crate::Result<()> {
		// First, get the node to find the flow ID for index deletion
		let node = CatalogStore::find_flow_node(txn, node_id)?;

		if let Some(node_def) = node {
			// Delete from main flow_node table
			txn.remove(&FlowNodeKey {
				node: node_id,
			}
			.encode())?;

			// Delete from flow_node_by_flow index
			txn.remove(&FlowNodeByFlowKey {
				flow: node_def.flow,
				node: node_id,
			}
			.encode())?;
		}

		Ok(())
	}
}
