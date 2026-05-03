// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use postcard::to_stdvec;
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	interface::catalog::flow::{FlowEdge, FlowId, FlowNode, FlowNodeId},
	internal,
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::{error::Error, value::blob::Blob};

use super::plan::{CompiledFlowPlan, LocalNodeId};
use crate::{
	Result,
	flow::{flow::FlowDag, node},
};

pub fn persist_flow(
	catalog: &Catalog,
	txn: &mut AdminTransaction,
	plan: CompiledFlowPlan,
	flow_id: FlowId,
) -> Result<FlowDag> {
	let mut node_map: HashMap<LocalNodeId, FlowNodeId> = HashMap::new();

	let mut builder = FlowDag::builder(flow_id);

	for compiled_node in &plan.nodes {
		let real_node_id = catalog.next_flow_node_id(txn)?;
		node_map.insert(compiled_node.local_id, real_node_id);

		let data = to_stdvec(&compiled_node.node_type)
			.map_err(|e| Error(Box::new(internal!("Failed to serialize FlowNodeType: {}", e))))?;

		let node_def = FlowNode {
			id: real_node_id,
			flow: flow_id,
			node_type: compiled_node.node_type.discriminator(),
			data: Blob::from(data),
		};
		catalog.create_flow_node(txn, &node_def)?;

		builder.add_node(node::FlowNode::new(real_node_id, compiled_node.node_type.clone()));
	}

	for compiled_edge in &plan.edges {
		let real_edge_id = catalog.next_flow_edge_id(txn)?;
		let real_source = *node_map.get(&compiled_edge.source).expect("Source node must exist in node map");
		let real_target = *node_map.get(&compiled_edge.target).expect("Target node must exist in node map");

		let edge_def = FlowEdge {
			id: real_edge_id,
			flow: flow_id,
			source: real_source,
			target: real_target,
		};
		catalog.create_flow_edge(txn, &edge_def)?;

		builder.add_edge(node::FlowEdge::new(real_edge_id, real_source, real_target))?;
	}

	Ok(builder.build())
}
