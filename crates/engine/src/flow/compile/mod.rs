// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation module for converting RQL logical plans into FlowGraphs
//!
//! This module bridges the gap between ReifyDB's SQL query processing and the streaming
//! dataflow engine, enabling automatic incremental computation for SQL queries.

mod operators;
mod sources;

use crate::Result;
use crate::flow::flow::Flow;
use crate::flow::node::{NodeId, NodeType};
use reifydb_core::interface::{SchemaId, Table, TableId};
use reifydb_core::result::error::diagnostic::flow::flow_error;
use reifydb_rql::plan::logical::{CreateComputedViewNode, LogicalPlan};
use std::collections::HashMap;

/// Compiler for converting RQL logical plans into executable FlowGraphs
pub struct FlowCompiler {
    /// Counter for generating unique table IDs
    next_table_id: u64,
    /// Current schema context for table resolution
    schema_context: Option<SchemaId>,
    /// Map from logical plan node references to FlowGraph NodeIds
    node_mapping: HashMap<usize, NodeId>,
}

impl FlowCompiler {
    /// Creates a new FlowCompiler instance
    pub fn new() -> Self {
        Self { next_table_id: 1, schema_context: None, node_mapping: HashMap::new() }
    }

    /// Compiles a vector of logical plans into a single FlowGraph
    pub fn compile(&mut self, plans: Vec<LogicalPlan>) -> Result<Flow> {
        let mut result = Flow::new();

        // Process logical plans in order, building the dataflow graph
        let mut last_node_id: Option<NodeId> = None;

        for (index, plan) in plans.into_iter().enumerate() {
            let node_id = self.compile_plan(&mut result, plan, index)?;
            // Connect nodes in sequence (for simple linear plans)
            if let Some(prev_id) = last_node_id {
                result.add_edge(&prev_id, &node_id)?;
            }
            last_node_id = Some(node_id);
        }

        dbg!(&result);

        Ok(result)
    }

    /// Compiles a single logical plan node into the FlowGraph
    fn compile_plan(
        &mut self,
        flow_graph: &mut Flow,
        plan: LogicalPlan,
        index: usize,
    ) -> Result<NodeId> {
        let node_id = match plan {
            // Data Sources -> Source Nodes
            LogicalPlan::TableScan(table_scan) => {
                self.compile_table_scan(flow_graph, table_scan)?
            }
            LogicalPlan::InlineData(inline_data) => {
                self.compile_inline_data(flow_graph, inline_data)?
            }

            // Query Operations -> Operators
            LogicalPlan::Filter(filter) => self.compile_filter(flow_graph, filter)?,
            LogicalPlan::Map(map) => self.compile_map(flow_graph, map)?,
            LogicalPlan::Aggregate(aggregate) => self.compile_aggregate(flow_graph, aggregate)?,
            LogicalPlan::JoinInner(join) => self.compile_join_inner(flow_graph, join)?,
            LogicalPlan::JoinLeft(join) => self.compile_join_left(flow_graph, join)?,
            LogicalPlan::Take(take) => self.compile_take(flow_graph, take)?,
            LogicalPlan::Order(order) => self.compile_order(flow_graph, order)?,

            // DDL operations that cannot be compiled to dataflow
            LogicalPlan::CreateSchema(_)
            | LogicalPlan::CreateTable(_)
            | LogicalPlan::CreateSequence(_)
            | LogicalPlan::CreateIndex(_) => {
                return Err(reifydb_core::Error(flow_error(
                    "DDL operations cannot be compiled to dataflow".to_string(),
                )));
            }

            // CREATE COMPUTED VIEW can be compiled to dataflow
            LogicalPlan::CreateComputedView(computed_view) => {
                self.compile_create_computed_view(flow_graph, computed_view)?
            }

            // Mutate operations are handled by transaction layer
            LogicalPlan::Insert(_) | LogicalPlan::Update(_) | LogicalPlan::Delete(_) => {
                return Err(reifydb_core::Error(flow_error(
                    "DML operations cannot be compiled to dataflow".to_string(),
                )));
            }

            // Not yet implemented
            LogicalPlan::JoinNatural(_) => {
                return Err(reifydb_core::Error(flow_error(
                    "Natural joins not yet implemented in dataflow".to_string(),
                )));
            }

            LogicalPlan::AlterSequence(_) => unreachable!(),
        };

        // Store the mapping for this plan node
        self.node_mapping.insert(index, node_id.clone());

        Ok(node_id)
    }

    /// Generates the next available TableId
    fn next_table_id(&mut self) -> TableId {
        let id = TableId(self.next_table_id);
        self.next_table_id += 1;
        id
    }

    /// Compiles a CREATE COMPUTED VIEW logical plan into a FlowGraph with a Sink node
    fn compile_create_computed_view(
        &mut self,
        flow_graph: &mut Flow,
        computed_view: CreateComputedViewNode,
    ) -> Result<NodeId> {
        // If there's no WITH clause, this is just a view definition without a query
        let query_plans = match computed_view.with {
            Some(plans) => plans,
            None => {
                return Err(reifydb_core::Error(flow_error(
                    "CREATE COMPUTED VIEW requires a WITH clause containing the query definition"
                        .to_string(),
                )));
            }
        };

        // Compile the query plans to build the dataflow graph
        let mut last_node_id: Option<NodeId> = None;

        for (index, plan) in query_plans.into_iter().enumerate() {
            let node_id = self.compile_plan(flow_graph, plan, index)?;

            // Connect nodes in sequence (for simple linear plans)
            if let Some(prev_id) = last_node_id {
                flow_graph.add_edge(&prev_id, &node_id)?;
            }

            last_node_id = Some(node_id);
        }

        // Create the computed view as a Sink node at the end
        let view_name = computed_view.view.fragment.clone();
        let view_table = Table {
            id: self.next_table_id(),
            schema: SchemaId(1), // TODO: Parse schema from computed_view.schema
            name: view_name.clone(),
            columns: vec![], // TODO: Create columns from computed_view.columns
        };

        let sink_node_id =
            flow_graph.add_node(NodeType::Sink { name: view_name, table: view_table });

        // Connect the last query node to the sink
        if let Some(last_id) = last_node_id {
            flow_graph.add_edge(&last_id, &sink_node_id)?;
        }

        Ok(sink_node_id)
    }
}

/// Public API for compiling logical plans to FlowGraphs
pub fn compile_to_flow(plans: Vec<LogicalPlan>) -> Result<Flow> {
    let mut compiler = FlowCompiler::new();
    compiler.compile(plans)
}
