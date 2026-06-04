// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use postcard::from_bytes;
use reifydb_catalog::CatalogStore;
use reifydb_core::{interface::catalog::flow::FlowId, internal};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::error::Error;

use crate::{
	Result,
	flow::{
		flow::FlowDag,
		node::{FlowEdge, FlowNode, FlowNodeType},
	},
};

pub fn load_flow_dag(txn: &mut Transaction<'_>, flow_id: FlowId) -> Result<FlowDag> {
	let node_defs = CatalogStore::list_flow_nodes_by_flow(txn, flow_id)?;
	let edge_defs = CatalogStore::list_flow_edges_by_flow(txn, flow_id)?;

	let mut builder = FlowDag::builder(flow_id);

	for node_def in node_defs {
		let node_type: FlowNodeType = from_bytes(node_def.data.as_ref())
			.map_err(|e| Error(Box::new(internal!("Failed to deserialize FlowNodeType: {}", e))))?;

		let node = FlowNode::new(node_def.id, node_type);
		builder.add_node(node);
	}

	for edge_def in edge_defs {
		let edge = FlowEdge::new(edge_def.id, edge_def.source, edge_def.target);
		builder.add_edge(edge)?;
	}

	let flow = builder.build();
	Ok(flow)
}
