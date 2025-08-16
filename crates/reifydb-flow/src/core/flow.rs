use std::collections::HashMap;

use reifydb_core::interface::FlowNodeId;
use serde::{Deserialize, Serialize};

use super::{
	change::Change,
	graph::DirectedGraph,
	node::{FlowEdge, FlowNode, FlowNodeType},
};
use crate::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Flow {
	graph: DirectedGraph<FlowNode>,
	node_map: HashMap<FlowNodeId, FlowNodeId>,
}

impl Default for Flow {
	fn default() -> Self {
		Self {
			graph: DirectedGraph::default(),
			node_map: HashMap::default(),
		}
	}
}

impl Flow {
	pub fn new() -> Self {
		Self {
			graph: DirectedGraph::new(),
			node_map: HashMap::new(),
		}
	}

	pub fn add_node(&mut self, node: FlowNode) -> FlowNodeId {
		let node_id = node.id.clone();
		self.graph.add_node(node_id.clone(), node);
		self.node_map.insert(node_id.clone(), node_id.clone());
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

	pub fn propagate_update(
		&mut self,
		node_id: &FlowNodeId,
		change: Change,
	) -> Result<()> {
		// This is a placeholder for update propagation logic
		// In a full implementation, this would:
		// 1. Process the update at the given node
		// 2. Generate output changes
		// 3. Propagate to downstream nodes

		if let Some(node) = self.graph.get_node(node_id) {
			// Debug: Propagating update to node with change

			// Process update based on node type
			match &node.ty {
				FlowNodeType::SourceTable {
					name,
					..
				} => {
					// Debug: Base table received update
					let _ = name; // Avoid unused variable warning
				}
				FlowNodeType::Operator {
					operator,
				} => {
					// Debug: Operator processing update
					let _ = operator; // Avoid unused variable warning
				}
				FlowNodeType::SinkView {
					name,
					..
				} => {
					// Debug: View storing update
					let _ = name; // Avoid unused variable warning
				}
			}
		}

		let _ = change; // Avoid unused variable warning

		Ok(())
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

	pub fn get_all_nodes(&self) -> impl Iterator<Item = FlowNodeId> + '_ {
		self.node_map.keys().cloned()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{TableId, ViewId};

	use super::*;
	use crate::{FlowEdge, core::OperatorType};

	#[test]
	fn test_dataflow_graph_basic_operations() {
		let mut graph = Flow::new();

		// Create some test nodes
		let table1 = graph.add_node(FlowNode::new(
			1,
			FlowNodeType::SourceTable {
				name: "test_table".to_string(),
				table: TableId(1),
			},
		));

		let operator = graph.add_node(FlowNode::new(
			2,
			FlowNodeType::Operator {
				operator: OperatorType::Map {
					expressions: vec![],
				},
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
