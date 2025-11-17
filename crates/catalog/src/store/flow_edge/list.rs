// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use flow_edge_by_flow::LAYOUT;
use reifydb_core::{
	interface::{EncodableKey, FlowEdgeDef, FlowEdgeId, FlowId, FlowNodeId, QueryTransaction},
	key::{FlowEdgeByFlowKey, FlowEdgeKey},
};

use crate::{
	CatalogStore,
	store::flow_edge::layout::{flow_edge, flow_edge_by_flow},
};

impl CatalogStore {
	pub fn list_flow_edges_by_flow(
		txn: &mut impl QueryTransaction,
		flow_id: FlowId,
	) -> crate::Result<Vec<FlowEdgeDef>> {
		let edge_ids: Vec<FlowEdgeId> = txn
			.range(FlowEdgeByFlowKey::full_scan(flow_id))?
			.map(|multi| FlowEdgeId(LAYOUT.get_u64(&multi.values, flow_edge_by_flow::ID)))
			.collect();

		// Then fetch each edge
		let mut edges = Vec::new();
		for edge_id in edge_ids {
			if let Some(edge) = Self::find_flow_edge(txn, edge_id)? {
				edges.push(edge);
			}
		}

		Ok(edges)
	}

	pub fn list_flow_edges_all(txn: &mut impl QueryTransaction) -> crate::Result<Vec<FlowEdgeDef>> {
		let mut result = Vec::new();

		let entries: Vec<_> = txn.range(FlowEdgeKey::full_scan())?.into_iter().collect();

		for entry in entries {
			if let Some(flow_edge_key) = FlowEdgeKey::decode(&entry.key) {
				let edge_id = flow_edge_key.edge;
				let flow_id = FlowId(flow_edge::LAYOUT.get_u64(&entry.values, flow_edge::FLOW));
				let source = FlowNodeId(flow_edge::LAYOUT.get_u64(&entry.values, flow_edge::SOURCE));
				let target = FlowNodeId(flow_edge::LAYOUT.get_u64(&entry.values, flow_edge::TARGET));

				let edge_def = FlowEdgeDef {
					id: edge_id,
					flow: flow_id,
					source,
					target,
				};

				result.push(edge_def);
			}
		}

		Ok(result)
	}
}
