use super::change::Change;
use super::flow::FlowGraph;
use super::node::{NodeId, NodeType, OperatorType};
use super::operators::{Operator, OperatorContext, FilterOperator, MapOperator};
use super::state::StateStore;
use crate::Result;
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
    
    pub fn process_change(&mut self, node_id: &NodeId, change: Change) -> Result<()> {
        if let Some(node) = self.graph.get_node(node_id) {
            let output_change = match &node.node_type {
                NodeType::Table { .. } => {
                    // Store in table state and pass through
                    if let Some(state) = self.node_states.get_mut(node_id) {
                        self.apply_change_to_state(state, &change)?;
                    }
                    change
                }
                NodeType::Operator { .. } => {
                    // Process through operator
                    if let (Some(operator), Some(context)) = (
                        self.operators.get_mut(node_id),
                        self.contexts.get_mut(node_id)
                    ) {
                        operator.apply(change, context)?
                    } else {
                        return Err(crate::Error::Runtime("Operator or context not found".to_string()));
                    }
                }
                NodeType::View { .. } => {
                    // Store in view state
                    if let Some(state) = self.node_states.get_mut(node_id) {
                        self.apply_change_to_state(state, &change)?;
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
                Err(crate::Error::Runtime(format!("Operator type {:?} not implemented yet", operator_type)))
            }
        }
    }
    
    fn apply_change_to_state(&self, state: &mut StateStore, change: &Change) -> Result<()> {
        for delta in &change.deltas {
            match delta {
                crate::delta::Delta::Insert { row, .. } => {
                    state.insert(row.clone())?;
                }
                crate::delta::Delta::Update { row, .. } => {
                    // For updates, we'd need to find and update the existing row
                    // This is simplified - in practice we'd use the key
                    state.insert(row.clone())?;
                }
                crate::delta::Delta::Upsert { row, .. } => {
                    state.insert(row.clone())?;
                }
                crate::delta::Delta::Remove { key } => {
                    // Would need to implement removal by key in StateStore
                    // For now, skip
                }
            }
        }
        Ok(())
    }
    
    pub fn get_view_data(&self, view_name: &str) -> Result<Vec<&crate::row::EncodedRow>> {
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
        Err(crate::Error::Runtime(format!("View {} not found", view_name)))
    }
    
    pub fn get_graph(&self) -> &FlowGraph {
        &self.graph
    }
}