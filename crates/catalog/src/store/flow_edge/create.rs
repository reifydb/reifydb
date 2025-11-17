// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{CommandTransaction, EncodableKey, FlowEdgeDef};

use crate::store::flow_edge::layout::{flow_edge, flow_edge_by_flow};

impl crate::CatalogStore {
	pub fn create_flow_edge(txn: &mut impl CommandTransaction, edge_def: &FlowEdgeDef) -> crate::Result<()> {
		// Write to main flow_edge table
		let mut row = flow_edge::LAYOUT.allocate();
		flow_edge::LAYOUT.set_u64(&mut row, flow_edge::ID, edge_def.id);
		flow_edge::LAYOUT.set_u64(&mut row, flow_edge::FLOW, edge_def.flow);
		flow_edge::LAYOUT.set_u64(&mut row, flow_edge::SOURCE, edge_def.source);
		flow_edge::LAYOUT.set_u64(&mut row, flow_edge::TARGET, edge_def.target);

		txn.set(
			&reifydb_core::key::FlowEdgeKey {
				edge: edge_def.id,
			}
			.encode(),
			row,
		)?;

		// Write to flow_edge_by_flow index
		let mut index_row = flow_edge_by_flow::LAYOUT.allocate();
		flow_edge_by_flow::LAYOUT.set_u64(&mut index_row, flow_edge_by_flow::FLOW, edge_def.flow);
		flow_edge_by_flow::LAYOUT.set_u64(&mut index_row, flow_edge_by_flow::ID, edge_def.id);

		txn.set(
			&reifydb_core::key::FlowEdgeByFlowKey {
				flow: edge_def.flow,
				edge: edge_def.id,
			}
			.encode(),
			index_row,
		)?;

		Ok(())
	}
}
