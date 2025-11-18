use std::{ops::Deref, sync::Arc};

use reifydb_core::interface::{FlowId, FlowNodeId};
use reifydb_type::Result;

use super::{
	graph::DirectedGraph,
	node::{FlowEdge, FlowNode},
};

#[derive(Debug, Clone)]
pub struct Flow {
	inner: Arc<FlowInner>,
}

#[derive(Debug)]
pub struct FlowInner {
	pub id: FlowId,
	pub graph: DirectedGraph<FlowNode>,
}

impl Deref for Flow {
	type Target = FlowInner;

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
	/// Create a new FlowBuilder
	pub fn new(id: impl Into<FlowId>) -> Self {
		Self {
			id: id.into(),
			graph: DirectedGraph::new(),
		}
	}

	/// Get the flow ID
	pub fn id(&self) -> FlowId {
		self.id
	}

	/// Add a node to the flow during construction
	pub fn add_node(&mut self, node: FlowNode) -> FlowNodeId {
		let node_id = node.id.clone();
		self.graph.add_node(node_id.clone(), node);
		node_id
	}

	/// Add an edge to the flow during construction
	pub fn add_edge(&mut self, edge: FlowEdge) -> Result<()> {
		let source = edge.source;
		let target = edge.target;

		self.graph.add_edge(edge);

		// Update operator input/output lists
		if let Some(from_node) = self.graph.get_node_mut(&source) {
			from_node.outputs.push(target.clone());
		}

		if let Some(to_node) = self.graph.get_node_mut(&target) {
			to_node.inputs.push(source.clone());
		}

		Ok(())
	}

	/// Build the immutable Flow from this builder
	pub fn build(self) -> Flow {
		Flow {
			inner: Arc::new(FlowInner {
				id: self.id,
				graph: self.graph,
			}),
		}
	}
}

impl Flow {
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
}
