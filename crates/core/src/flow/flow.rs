use super::change::Change;
use super::graph::DirectedGraph;
use super::node::{Node, NodeId, NodeType};
use crate::Result;
use std::collections::HashMap;

#[derive(Clone)]
pub struct FlowGraph {
    graph: DirectedGraph<Node>,
    node_map: HashMap<NodeId, NodeId>,
    next_node_id: u64,
}

impl FlowGraph {
    pub fn new() -> Self {
        Self { graph: DirectedGraph::new(), node_map: HashMap::new(), next_node_id: 0 }
    }

    pub fn add_node(&mut self, node_type: NodeType) -> NodeId {
        let node_id = NodeId(self.next_node_id);
        self.next_node_id += 1;

        let node = Node { id: node_id.clone(), node_type, inputs: Vec::new(), outputs: Vec::new() };

        self.graph.add_node(node_id.clone(), node);
        self.node_map.insert(node_id.clone(), node_id.clone());

        node_id
    }

    pub fn add_edge(&mut self, from: &NodeId, to: &NodeId) -> Result<()> {
        self.graph.add_edge(from, to);

        // Update node input/output lists
        if let Some(from_node) = self.graph.get_node_mut(from) {
            from_node.outputs.push(to.clone());
        }

        if let Some(to_node) = self.graph.get_node_mut(to) {
            to_node.inputs.push(from.clone());
        }

        Ok(())
    }

    pub fn topological_order(&self) -> Result<Vec<NodeId>> {
        Ok(self.graph.topological_sort())
    }

    pub fn propagate_update(&mut self, node_id: &NodeId, change: Change) -> Result<()> {
        // This is a placeholder for update propagation logic
        // In a full implementation, this would:
        // 1. Process the update at the given node
        // 2. Generate output changes
        // 3. Propagate to downstream nodes

        if let Some(node) = self.graph.get_node(node_id) {
            // Debug: Propagating update to node with change

            // Process update based on node type
            match &node.node_type {
                NodeType::Table { name, .. } => {
                    // Debug: Base table received update
                    let _ = name; // Avoid unused variable warning
                }
                NodeType::Operator { operator } => {
                    // Debug: Operator processing update
                    let _ = operator; // Avoid unused variable warning
                }
                NodeType::View { name, .. } => {
                    // Debug: View storing update
                    let _ = name; // Avoid unused variable warning
                }
            }
        }

        let _ = change; // Avoid unused variable warning

        Ok(())
    }

    pub fn get_node(&self, node_id: &NodeId) -> Option<&Node> {
        self.graph.get_node(node_id)
    }

    pub fn get_node_mut(&mut self, node_id: &NodeId) -> Option<&mut Node> {
        self.graph.get_node_mut(node_id)
    }
}

impl Default for FlowGraph {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flow::node::OperatorType;
    use crate::interface::{SchemaId, Table, TableId};

    fn create_test_table(id: u64, name: &str) -> Table {
        Table { id: TableId(id), schema: SchemaId(1), name: name.to_string(), columns: vec![] }
    }

    #[test]
    fn test_dataflow_graph_basic_operations() {
        let mut graph = FlowGraph::new();

        // Create some test nodes
        let table1 = graph.add_node(NodeType::Table {
            name: "test_table".to_string(),
            table: create_test_table(1, "test_table"),
        });

        let operator = graph
            .add_node(NodeType::Operator { operator: OperatorType::Map { expressions: vec![] } });

        let view = graph.add_node(NodeType::View {
            name: "test_view".to_string(),
            table: create_test_table(2, "test_view"),
        });

        // Add edges
        assert!(graph.add_edge(&table1, &operator).is_ok());
        assert!(graph.add_edge(&operator, &view).is_ok());

        // Check that nodes exist
        assert!(graph.get_node(&table1).is_some());
        assert!(graph.get_node(&operator).is_some());
        assert!(graph.get_node(&view).is_some());

        // Check topological order
        let order = graph.topological_order().unwrap();
        assert_eq!(order.len(), 3);

        // table1 should come before operator, operator before view
        let table1_pos = order.iter().position(|id| *id == table1).unwrap();
        let operator_pos = order.iter().position(|id| *id == operator).unwrap();
        let view_pos = order.iter().position(|id| *id == view).unwrap();

        assert!(table1_pos < operator_pos);
        assert!(operator_pos < view_pos);
    }
}
