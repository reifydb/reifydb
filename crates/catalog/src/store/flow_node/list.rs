// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{EncodableKey, FlowId, FlowNodeDef, FlowNodeId, QueryTransaction},
	key::FlowNodeKey,
};

use crate::{
	CatalogStore,
	store::flow_node::layout::{flow_node, flow_node_by_flow},
};

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

	pub fn list_flow_nodes_all(txn: &mut impl QueryTransaction) -> crate::Result<Vec<FlowNodeDef>> {
		let mut result = Vec::new();

		let entries: Vec<_> = txn.range(FlowNodeKey::full_scan())?.into_iter().collect();

		for entry in entries {
			if let Some(flow_node_key) = FlowNodeKey::decode(&entry.key) {
				let node_id = flow_node_key.node;
				let flow_id = FlowId(flow_node::LAYOUT.get_u64(&entry.values, flow_node::FLOW));
				let node_type = flow_node::LAYOUT.get_u8(&entry.values, flow_node::TYPE);
				let data = flow_node::LAYOUT.get_blob(&entry.values, flow_node::DATA).clone();

				let node_def = FlowNodeDef {
					id: node_id,
					flow: flow_id,
					node_type,
					data,
				};

				result.push(node_def);
			}
		}

		Ok(result)
	}
}
