// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Loader module for reconstructing Flows from catalog nodes and edges

use std::time::Duration;

use postcard::from_bytes;
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

	// Look up tick duration from Flow
	let tick = catalog
		.find_flow(txn, flow_id)?
		.and_then(|def| def.tick)
		.map(|d| Duration::from_nanos(d.get_nanos() as u64));

	let mut builder = FlowDag::builder(flow_id).tick(tick);

	// Deserialize and add all nodes
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
