use reifydb_type::Result;
use serde::{Deserialize, Serialize};

use super::{
	graph::DirectedGraph,
	node::{FlowEdge, FlowNode},
};
use crate::interface::{FlowId, FlowNodeId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Flow {
	pub id: FlowId,
	pub graph: DirectedGraph<FlowNode>,
}

impl Flow {
	pub fn new(id: impl Into<FlowId>) -> Self {
		Self {
			id: id.into(),
			graph: DirectedGraph::new(),
		}
	}

	pub fn add_node(&mut self, node: FlowNode) -> FlowNodeId {
		let node_id = node.id.clone();
		self.graph.add_node(node_id.clone(), node);
		node_id
	}

	pub fn add_edge(&mut self, edge: FlowEdge) -> Result<()> {
		let source = edge.source;
		let target = edge.target;

		self.graph.add_edge(edge);

		// Update node input/output lists
		if let Some(from_node) = self.graph.get_node_mut(&source) {
			from_node.outputs.push(target.clone());
		}

		if let Some(to_node) = self.graph.get_node_mut(&target) {
			to_node.inputs.push(source.clone());
		}

		Ok(())
	}

	pub fn topological_order(&self) -> Result<Vec<FlowNodeId>> {
		Ok(self.graph.topological_sort())
	}

	pub fn get_node(&self, node_id: &FlowNodeId) -> Option<&FlowNode> {
		self.graph.get_node(node_id)
	}

	pub fn get_node_mut(&mut self, node_id: &FlowNodeId) -> Option<&mut FlowNode> {
		self.graph.get_node_mut(node_id)
	}

	pub fn get_node_ids(&self) -> impl Iterator<Item = FlowNodeId> + '_ {
		self.graph.nodes().map(|e| *e.0)
	}
}
