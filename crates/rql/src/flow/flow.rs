// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{ops::Deref, sync::Arc, time::Duration};

use reifydb_core::{
	interface::catalog::flow::{FlowId, FlowNodeId},
	internal,
};
use reifydb_type::{Result, error::Error};

use super::{
	graph::DirectedGraph,
	node::{FlowEdge, FlowNode, FlowNodeType},
};

#[derive(Debug, Clone)]
pub struct FlowDag {
	inner: Arc<Inner>,
}

#[derive(Debug)]
pub struct Inner {
	pub id: FlowId,
	pub graph: DirectedGraph<FlowNode>,
	pub tick: Option<Duration>,
}

impl Deref for FlowDag {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

#[derive(Debug)]
pub struct FlowBuilder {
	id: FlowId,
	graph: DirectedGraph<FlowNode>,
	tick: Option<Duration>,
}

impl FlowBuilder {
	/// Create a new FlowBuilder
	pub fn new(id: impl Into<FlowId>) -> Self {
		Self {
			id: id.into(),
			graph: DirectedGraph::new(),
			tick: None,
		}
	}

	/// Set the tick duration for this flow
	pub fn tick(mut self, tick: Option<Duration>) -> Self {
		self.tick = tick;
		self
	}

	/// Get the flow ID
	pub fn id(&self) -> FlowId {
		self.id
	}

	/// Add a node to the flow during construction
	pub fn add_node(&mut self, node: FlowNode) -> FlowNodeId {
		let node_id = node.id;
		self.graph.add_node(node_id, node);
		node_id
	}

	/// Add an edge to the flow during construction
	pub fn add_edge(&mut self, edge: FlowEdge) -> Result<()> {
		let source = edge.source;
		let target = edge.target;

		if self.graph.get_node(&source).is_none() {
			return Err(Error(Box::new(internal!(
				"Flow edge references missing source node {:?}",
				source
			))));
		}
		if self.graph.get_node(&target).is_none() {
			return Err(Error(Box::new(internal!(
				"Flow edge references missing target node {:?}",
				target
			))));
		}

		self.graph.add_edge(edge);

		// Update operator input/output lists
		if let Some(from_node) = self.graph.get_node_mut(&source) {
			from_node.outputs.push(target);
		}

		if let Some(to_node) = self.graph.get_node_mut(&target) {
			to_node.inputs.push(source);
		}

		Ok(())
	}

	/// Build the immutable Flow from this builder
	pub fn build(self) -> FlowDag {
		FlowDag {
			inner: Arc::new(Inner {
				id: self.id,
				graph: self.graph,
				tick: self.tick,
			}),
		}
	}
}

impl FlowDag {
	/// Create a new FlowBuilder for constructing a Flow
	pub fn builder(id: impl Into<FlowId>) -> FlowBuilder {
		FlowBuilder::new(id)
	}

	/// Get the flow ID
	pub fn id(&self) -> FlowId {
		self.inner.id
	}

	/// Get the topological order of nodes in the flow
	pub fn topological_order(&self) -> Result<Vec<FlowNodeId>> {
		Ok(self.inner.graph.topological_sort())
	}

	/// Get a node by its ID
	pub fn get_node(&self, node_id: &FlowNodeId) -> Option<&FlowNode> {
		self.inner.graph.get_node(node_id)
	}

	/// Get all node IDs in the flow
	pub fn get_node_ids(&self) -> impl Iterator<Item = FlowNodeId> + '_ {
		self.inner.graph.nodes().map(|e| *e.0)
	}

	/// Get the number of nodes in the flow
	pub fn node_count(&self) -> usize {
		self.inner.graph.node_count()
	}

	/// Get the number of edges in the flow
	pub fn edge_count(&self) -> usize {
		self.inner.graph.edge_count()
	}

	/// Get the tick duration for this flow, if configured.
	pub fn tick(&self) -> Option<Duration> {
		self.inner.tick
	}

	/// Check whether this flow has a subscription sink.
	pub fn is_subscription(&self) -> bool {
		self.get_node_ids().any(|id| {
			self.get_node(&id).is_some_and(|n| matches!(n.ty, FlowNodeType::SinkSubscription { .. }))
		})
	}
}
