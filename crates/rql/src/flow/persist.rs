// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Persistence module for compiled flow plans.
//!
//! This module handles Phase 2 of the 2-phase flow compilation:
//! - Allocates real catalog IDs for nodes and edges
//! - Persists nodes and edges to the catalog
//! - Builds the in-memory Flow representation

use std::collections::HashMap;

use reifydb_catalog::{CatalogStore, store::sequence::flow::{next_flow_edge_id, next_flow_node_id}};
use reifydb_core::interface::FlowId;
use reifydb_core::interface::{MultiVersionCommandTransaction, FlowEdgeDef, FlowNodeDef};
use reifydb_type::Blob;

use super::{
	Flow, FlowEdge, FlowNode,
	plan::{CompiledFlowPlan, LocalNodeId},
};

/// Persists a compiled flow plan to the catalog and returns the Flow.
///
/// This is Phase 2 of the 2-phase compilation:
/// - Phase 1: `compile_flow_plan()` - pure compilation, no catalog access
/// - Phase 2: `persist_flow()` - allocates real node/edge IDs and persists to catalog
///
/// # Arguments
/// * `txn` - The command transaction for catalog access
/// * `plan` - The compiled flow plan from Phase 1
/// * `flow_id` - The FlowId from the already-created FlowDef
///
/// # Returns
/// The persisted `Flow` with real catalog IDs
///
/// # Note
/// The FlowDef must already be created via `CatalogStore::create_flow()` before
/// calling this function. This function only persists nodes and edges.
pub fn persist_flow(
	txn: &mut impl MultiVersionCommandTransaction,
	plan: CompiledFlowPlan,
	flow_id: FlowId,
) -> crate::Result<Flow> {

	// Map local IDs to real catalog IDs
	let mut node_map: HashMap<LocalNodeId, reifydb_core::interface::FlowNodeId> = HashMap::new();

	// Create a FlowBuilder for the in-memory Flow
	let mut builder = Flow::builder(flow_id);

	// Phase 2a: Persist all nodes and build ID mapping
	for compiled_node in &plan.nodes {
		let real_node_id = next_flow_node_id(txn)?;
		node_map.insert(compiled_node.local_id, real_node_id);

		// Serialize the node type
		let data = postcard::to_stdvec(&compiled_node.node_type).map_err(|e| {
			reifydb_core::Error(reifydb_type::internal!("Failed to serialize FlowNodeType: {}", e))
		})?;

		// Create and persist the catalog entry
		let node_def = FlowNodeDef {
			id: real_node_id,
			flow: flow_id,
			node_type: compiled_node.node_type.discriminator(),
			data: Blob::from(data),
		};
		CatalogStore::create_flow_node(txn, &node_def)?;

		// Add to in-memory builder
		builder.add_node(FlowNode::new(real_node_id, compiled_node.node_type.clone()));
	}

	// Phase 2b: Persist all edges (now we can resolve local IDs to real IDs)
	for compiled_edge in &plan.edges {
		let real_edge_id = next_flow_edge_id(txn)?;
		let real_source = *node_map
			.get(&compiled_edge.source)
			.expect("Source node must exist in node map");
		let real_target = *node_map
			.get(&compiled_edge.target)
			.expect("Target node must exist in node map");

		// Create and persist the catalog entry
		let edge_def = FlowEdgeDef {
			id: real_edge_id,
			flow: flow_id,
			source: real_source,
			target: real_target,
		};
		CatalogStore::create_flow_edge(txn, &edge_def)?;

		// Add to in-memory builder
		builder.add_edge(FlowEdge::new(real_edge_id, real_source, real_target))?;
	}

	Ok(builder.build())
}
