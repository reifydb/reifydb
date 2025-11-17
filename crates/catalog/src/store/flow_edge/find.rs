// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use flow_edge::LAYOUT;
use reifydb_core::{
	interface::{EncodableKey, FlowEdgeDef, FlowEdgeId, FlowId, FlowNodeId, QueryTransaction},
	key::FlowEdgeKey,
};

use crate::{CatalogStore, store::flow_edge::layout::flow_edge};

impl CatalogStore {
	pub fn find_flow_edge(txn: &mut impl QueryTransaction, edge: FlowEdgeId) -> crate::Result<Option<FlowEdgeDef>> {
		let Some(multi) = txn.get(&FlowEdgeKey {
			edge,
		}
		.encode())?
		else {
			return Ok(None);
		};

		let row = multi.values;
		let id = FlowEdgeId(LAYOUT.get_u64(&row, flow_edge::ID));
		let flow = FlowId(LAYOUT.get_u64(&row, flow_edge::FLOW));
		let source = FlowNodeId(LAYOUT.get_u64(&row, flow_edge::SOURCE));
		let target = FlowNodeId(LAYOUT.get_u64(&row, flow_edge::TARGET));

		Ok(Some(FlowEdgeDef {
			id,
			flow,
			source,
			target,
		}))
	}
}
