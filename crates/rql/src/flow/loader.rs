// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Loader module for reconstructing Flows from catalog nodes and edges

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{interface::catalog::flow::FlowId, internal};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::error::Error;

use crate::{
	Result,
	flow::{
		flow::FlowDag,
		node::{FlowEdge, FlowNode, FlowNodeType},
	},
};

/// Loads a Flow from the catalog by reconstructing it from nodes and edges
pub fn load_flow_dag(catalog: &Catalog, txn: &mut Transaction<'_>, flow_id: FlowId) -> Result<FlowDag> {
	let node_defs = catalog.list_flow_nodes_by_flow(txn, flow_id)?;
	let edge_defs = catalog.list_flow_edges_by_flow(txn, flow_id)?;

	let mut builder = FlowDag::builder(flow_id);

	// Deserialize and add all nodes
	for node_def in node_defs {
		let node_type: FlowNodeType = postcard::from_bytes(node_def.data.as_ref())
			.map_err(|e| Error(internal!("Failed to deserialize FlowNodeType: {}", e)))?;

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
