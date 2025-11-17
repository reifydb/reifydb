// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use flow_edge_by_flow::LAYOUT;
use reifydb_core::{
	interface::{FlowEdgeDef, FlowEdgeId, FlowId, QueryTransaction},
	key::FlowEdgeByFlowKey,
};

use crate::{CatalogStore, store::flow_edge::layout::flow_edge_by_flow};

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
}
