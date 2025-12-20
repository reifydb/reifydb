// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Loader module for reconstructing Flows from catalog nodes and edges

use bincode::{
	config::{Configuration, standard},
	serde::decode_from_slice,
};
use reifydb_catalog::CatalogStore;
use reifydb_core::{
	Error,
	interface::{FlowId, QueryTransaction},
};
use reifydb_type::internal;

use super::{Flow, FlowEdge, FlowNode, FlowNodeType};

/// Loads a Flow from the catalog by reconstructing it from nodes and edges
pub async fn load_flow(txn: &mut impl QueryTransaction, flow_id: FlowId) -> crate::Result<Flow> {
	// Load all nodes for this flow
	let node_defs = CatalogStore::list_flow_nodes_by_flow(txn, flow_id).await?;

	// Load all edges for this flow
	let edge_defs = CatalogStore::list_flow_edges_by_flow(txn, flow_id).await?;

	// Create a new FlowBuilder
	let mut builder = Flow::builder(flow_id);

	// Deserialize and add all nodes
	for node_def in node_defs {
		// Deserialize the FlowNodeType from the blob
		let (node_type, _) =
			decode_from_slice::<FlowNodeType, Configuration>(node_def.data.as_ref(), standard())
				.map_err(|e| Error(internal!("Failed to deserialize FlowNodeType: {}", e)))?;

		// Create and add the FlowNode
		let node = FlowNode::new(node_def.id, node_type);
		builder.add_node(node);
	}

	// Add all edges
	for edge_def in edge_defs {
		let edge = FlowEdge::new(edge_def.id, edge_def.source, edge_def.target);
		builder.add_edge(edge)?;
	}

	// Build the immutable Flow
	Ok(builder.build())
}
