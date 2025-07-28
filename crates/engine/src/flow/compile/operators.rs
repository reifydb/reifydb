// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of operator logical plans to FlowGraph nodes

use super::FlowCompiler;
use crate::flow::flow::FlowGraph;
use crate::flow::node::{NodeId, NodeType, OperatorType};
use crate::Result;
use reifydb_core::JoinType;
use reifydb_rql::plan::logical::{
    AggregateNode, FilterNode, JoinInnerNode, JoinLeftNode, MapNode, OrderNode, TakeNode,
};

impl FlowCompiler {
    /// Compiles a Filter logical plan into a Filter operator
    pub(super) fn compile_filter(&mut self, flow_graph: &mut FlowGraph, filter: FilterNode) -> Result<NodeId> {
        let node_id = flow_graph.add_node(NodeType::Operator {
            operator: OperatorType::Filter {
                predicate: filter.condition,
            },
        });
        
        Ok(node_id)
    }
    
    /// Compiles a Map logical plan into a Map operator
    pub(super) fn compile_map(&mut self, flow_graph: &mut FlowGraph, map: MapNode) -> Result<NodeId> {
        let node_id = flow_graph.add_node(NodeType::Operator {
            operator: OperatorType::Map {
                expressions: map.map,
            },
        });
        
        Ok(node_id)
    }
    
    /// Compiles an Aggregate logical plan into an Aggregate operator
    pub(super) fn compile_aggregate(&mut self, flow_graph: &mut FlowGraph, aggregate: AggregateNode) -> Result<NodeId> {
        let node_id = flow_graph.add_node(NodeType::Operator {
            operator: OperatorType::Aggregate {
                by: aggregate.by,
                map: aggregate.map,
            },
        });
        
        Ok(node_id)
    }
    
    /// Compiles a JoinInner logical plan into a Join operator
    pub(super) fn compile_join_inner(&mut self, flow_graph: &mut FlowGraph, join: JoinInnerNode) -> Result<NodeId> {
        // For joins, we need to handle multiple inputs
        // This is a simplified implementation - proper join compilation requires
        // handling the subplans in join.with and connecting them properly
        
        let node_id = flow_graph.add_node(NodeType::Operator {
            operator: OperatorType::Join {
                join_type: JoinType::Inner,
                left: join.on.clone(),  // Simplified - need proper key extraction
                right: join.on,         // Simplified - need proper key extraction
            },
        });
        
        // TODO: Compile and connect the subplans in join.with
        // This requires more sophisticated graph building logic
        
        Ok(node_id)
    }
    
    /// Compiles a JoinLeft logical plan into a Join operator
    pub(super) fn compile_join_left(&mut self, flow_graph: &mut FlowGraph, join: JoinLeftNode) -> Result<NodeId> {
        let node_id = flow_graph.add_node(NodeType::Operator {
            operator: OperatorType::Join {
                join_type: JoinType::Left,
                left: join.on.clone(),  // Simplified - need proper key extraction
                right: join.on,         // Simplified - need proper key extraction
            },
        });
        
        // TODO: Compile and connect the subplans in join.with
        
        Ok(node_id)
    }
    
    /// Compiles a Take logical plan into a TopK operator
    pub(super) fn compile_take(&mut self, flow_graph: &mut FlowGraph, take: TakeNode) -> Result<NodeId> {
        let node_id = flow_graph.add_node(NodeType::Operator {
            operator: OperatorType::TopK {
                k: take.take,
                sort: vec![], // No sorting specified, just limit
            },
        });
        
        Ok(node_id)
    }
    
    /// Compiles an Order logical plan into a TopK operator with sorting
    pub(super) fn compile_order(&mut self, flow_graph: &mut FlowGraph, order: OrderNode) -> Result<NodeId> {
        // Order without limit becomes a TopK with a very large K
        // In practice, this might need special handling or a dedicated Sort operator
        let node_id = flow_graph.add_node(NodeType::Operator {
            operator: OperatorType::TopK {
                k: usize::MAX, // Sort all results
                sort: order.by,
            },
        });
        
        Ok(node_id)
    }
}