use super::change::{Change, Diff};
use super::flow::FlowGraph;
use super::node::{NodeId, NodeType, OperatorType};
use super::operators::{FilterOperator, MapOperator, Operator, OperatorContext};
use super::state::StateStore;
use crate::Result;
use crate::flow::row::Row;
use std::collections::HashMap;

pub struct FlowEngine {
    graph: FlowGraph,
    operators: HashMap<NodeId, Box<dyn Operator>>,
    contexts: HashMap<NodeId, OperatorContext>,
    node_states: HashMap<NodeId, StateStore>,
}

impl FlowEngine {
    pub fn new(graph: FlowGraph) -> Self {
        Self {
            graph,
            operators: HashMap::new(),
            contexts: HashMap::new(),
            node_states: HashMap::new(),
        }
    }

    pub fn initialize(&mut self) -> Result<()> {
        // Initialize operators and state for all nodes
        let node_ids: Vec<NodeId> = self.graph.get_all_nodes().collect();

        for node_id in node_ids {
            if let Some(node) = self.graph.get_node(&node_id) {
                match &node.node_type {
                    NodeType::Table { .. } => {
                        // Base tables need state storage
                        self.node_states.insert(node_id.clone(), StateStore::new());
                    }
                    NodeType::Operator { operator } => {
                        // Create operator and context
                        let op = self.create_operator(operator)?;
                        self.operators.insert(node_id.clone(), op);
                        self.contexts.insert(node_id.clone(), OperatorContext::new());
                    }
                    NodeType::View { .. } => {
                        // Views need state storage for materialized results
                        self.node_states.insert(node_id.clone(), StateStore::new());
                    }
                }
            }
        }

        Ok(())
    }

    pub fn process_change(&mut self, node_id: &NodeId, change: Diff) -> Result<()> {
        if let Some(node) = self.graph.get_node(node_id) {
            let output_change = match &node.node_type {
                NodeType::Table { .. } => {
                    // Store in table state and pass through
                    if let Some(state) = self.node_states.get_mut(node_id) {
                        Self::apply_change_to_state(state, &change)?;
                    }
                    change
                }
                NodeType::Operator { .. } => {
                    // Process through operator
                    if let (Some(operator), Some(context)) =
                        (self.operators.get_mut(node_id), self.contexts.get_mut(node_id))
                    {
                        operator.apply(context, change)?
                    } else {
                        panic!("Operator or context not found");
                    }
                }
                NodeType::View { .. } => {
                    // Store in view state
                    if let Some(state) = self.node_states.get_mut(node_id) {
                        Self::apply_change_to_state(state, &change)?;
                    }
                    change
                }
            };

            // Propagate to downstream nodes
            let output_nodes = node.outputs.clone();
            for output_id in output_nodes {
                self.process_change(&output_id, output_change.clone())?;
            }
        }

        Ok(())
    }

    fn create_operator(&self, operator_type: &OperatorType) -> Result<Box<dyn Operator>> {
        match operator_type {
            OperatorType::Filter { predicate } => {
                Ok(Box::new(FilterOperator::new(predicate.clone())))
            }
            OperatorType::Map { expressions } => {
                Ok(Box::new(MapOperator::new(expressions.clone())))
            }
            _ => {
                panic!("Operator type {:?} not implemented yet", operator_type)
            }
        }
    }

    fn apply_change_to_state(state: &mut StateStore, diff: &Diff) -> Result<()> {
        for change in &diff.changes {
            match change {
                Change::Insert { row, .. } => {
                    state.insert(row.clone())?;
                }
                Change::Update { old, new } => {
                    todo!()
                }
                Change::Remove { row } => {
                    todo!()
                }
            }
        }
        Ok(())
    }

    pub fn get_view_data(&self, view_name: &str) -> Result<Vec<&Row>> {
        // Find view node and return its state
        for (node_id, _) in &self.node_states {
            if let Some(node) = self.graph.get_node(node_id) {
                if let NodeType::View { name, .. } = &node.node_type {
                    if name == view_name {
                        if let Some(state) = self.node_states.get(node_id) {
                            return Ok(state.all_rows().collect());
                        }
                    }
                }
            }
        }
        panic!("View {} not found", view_name)
    }

    pub fn get_graph(&self) -> &FlowGraph {
        &self.graph
    }
}
