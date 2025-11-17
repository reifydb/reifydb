// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{FlowId, FlowNodeDef, FlowNodeId, QueryTransaction};

use crate::{CatalogStore, store::flow_node::layout::flow_node_by_flow};

impl CatalogStore {
	pub fn list_flow_nodes_by_flow(
		txn: &mut impl QueryTransaction,
		flow_id: FlowId,
	) -> crate::Result<Vec<FlowNodeDef>> {
		// First collect all node IDs
		let node_ids: Vec<FlowNodeId> = txn
			.range(reifydb_core::key::FlowNodeByFlowKey::full_scan(flow_id))?
			.map(|multi| {
				FlowNodeId(flow_node_by_flow::LAYOUT.get_u64(&multi.values, flow_node_by_flow::ID))
			})
			.collect();

		// Then fetch each node
		let mut nodes = Vec::new();
		for node_id in node_ids {
			if let Some(node) = Self::find_flow_node(txn, node_id)? {
				nodes.push(node);
			}
		}

		Ok(nodes)
	}
}
