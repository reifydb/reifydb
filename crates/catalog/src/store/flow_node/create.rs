// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{CommandTransaction, EncodableKey, FlowNodeDef},
	key::FlowNodeKey,
};

use crate::store::flow_node::layout::{flow_node, flow_node_by_flow};

impl crate::CatalogStore {
	pub fn create_flow_node(txn: &mut impl CommandTransaction, node_def: &FlowNodeDef) -> crate::Result<()> {
		// Write to main flow_node table
		let mut row = flow_node::LAYOUT.allocate();
		flow_node::LAYOUT.set_u64(&mut row, flow_node::ID, node_def.id);
		flow_node::LAYOUT.set_u64(&mut row, flow_node::FLOW, node_def.flow);
		flow_node::LAYOUT.set_u8(&mut row, flow_node::TYPE, node_def.node_type);
		flow_node::LAYOUT.set_blob(&mut row, flow_node::DATA, &node_def.data);

		txn.set(
			&FlowNodeKey {
				node: node_def.id,
			}
			.encode(),
			row,
		)?;

		// Write to flow_node_by_flow index
		let mut index_row = flow_node_by_flow::LAYOUT.allocate();
		flow_node_by_flow::LAYOUT.set_u64(&mut index_row, flow_node_by_flow::FLOW, node_def.flow);
		flow_node_by_flow::LAYOUT.set_u64(&mut index_row, flow_node_by_flow::ID, node_def.id);

		txn.set(
			&reifydb_core::key::FlowNodeByFlowKey {
				flow: node_def.flow,
				node: node_def.id,
			}
			.encode(),
			index_row,
		)?;

		Ok(())
	}
}
