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

	pub fn get_node_mut(
		&mut self,
		node_id: &FlowNodeId,
	) -> Option<&mut FlowNode> {
		self.graph.get_node_mut(node_id)
	}

	pub fn get_node_ids(&self) -> impl Iterator<Item = FlowNodeId> + '_ {
		self.graph.nodes().map(|e| *e.0)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{
		flow::{FlowNodeSchema, FlowNodeType, OperatorType},
		interface::{TableId, ViewId},
	};

	#[test]
	fn test_dataflow_graph_basic_operations() {
		let mut graph = Flow::new(1);

		// Create some test nodes
		let table1 = graph.add_node(FlowNode::new(
			1,
			FlowNodeType::SourceTable {
				name: "test_table".to_string(),
				table: TableId(1),
				schema: FlowNodeSchema::empty(),
			},
		));

		let operator = graph.add_node(FlowNode::new(
			2,
			FlowNodeType::Operator {
				operator: OperatorType::Map {
					expressions: vec![],
				},
				input_schemas: vec![],
				output_schema: FlowNodeSchema::empty(),
			},
		));

		let view = graph.add_node(FlowNode::new(
			3,
			FlowNodeType::SinkView {
				name: "test_view".to_string(),
				view: ViewId(2),
			},
		));

		// Add edges
		assert!(graph
			.add_edge(FlowEdge::new(1, &table1, &operator))
			.is_ok());

		assert!(graph
			.add_edge(FlowEdge::new(2, &operator, &view))
			.is_ok());

		// Check that nodes exist
		assert!(graph.get_node(&table1).is_some());
		assert!(graph.get_node(&operator).is_some());
		assert!(graph.get_node(&view).is_some());

		// Check topological order
		let order = graph.topological_order().unwrap();
		assert_eq!(order.len(), 3);

		// table1 should come before operator, operator before view
		let table1_pos =
			order.iter().position(|id| *id == table1).unwrap();
		let operator_pos =
			order.iter().position(|id| *id == operator).unwrap();
		let view_pos = order.iter().position(|id| *id == view).unwrap();

		assert!(table1_pos < operator_pos);
		assert!(operator_pos < view_pos);
	}
}
