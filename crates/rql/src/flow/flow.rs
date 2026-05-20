// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{ops::Deref, sync::Arc};

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
}

impl FlowBuilder {
	pub fn new(id: impl Into<FlowId>) -> Self {
		Self {
			id: id.into(),
			graph: DirectedGraph::new(),
		}
	}

	pub fn id(&self) -> FlowId {
		self.id
	}

	pub fn add_node(&mut self, node: FlowNode) -> FlowNodeId {
		let node_id = node.id;
		self.graph.add_node(node_id, node);
		node_id
	}

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

		if let Some(from_node) = self.graph.get_node_mut(&source) {
			from_node.outputs.push(target);
		}

		if let Some(to_node) = self.graph.get_node_mut(&target) {
			to_node.inputs.push(source);
		}

		Ok(())
	}

	pub fn build(self) -> FlowDag {
		FlowDag {
			inner: Arc::new(Inner {
				id: self.id,
				graph: self.graph,
			}),
		}
	}
}

impl FlowDag {
	pub fn builder(id: impl Into<FlowId>) -> FlowBuilder {
		FlowBuilder::new(id)
	}

	pub fn id(&self) -> FlowId {
		self.inner.id
	}

	pub fn topological_order(&self) -> Result<Vec<FlowNodeId>> {
		Ok(self.inner.graph.topological_sort())
	}

	pub fn get_node(&self, node_id: &FlowNodeId) -> Option<&FlowNode> {
		self.inner.graph.get_node(node_id)
	}

	pub fn get_node_ids(&self) -> impl Iterator<Item = FlowNodeId> + '_ {
		self.inner.graph.nodes().map(|e| *e.0)
	}

	pub fn node_count(&self) -> usize {
		self.inner.graph.node_count()
	}

	pub fn edge_count(&self) -> usize {
		self.inner.graph.edge_count()
	}

	pub fn is_subscription(&self) -> bool {
		self.get_node_ids().any(|id| {
			self.get_node(&id).is_some_and(|n| matches!(n.ty, FlowNodeType::SinkSubscription { .. }))
		})
	}

	pub fn ticks(&self) -> bool {
		self.get_node_ids().any(|id| self.get_node(&id).is_some_and(|n| n.ty.ticks()))
	}
}
