// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation module for converting RQL logical plans into FlowGraphs
//!
//! This module bridges the gap between ReifyDB's SQL query processing and the
//! streaming dataflow engine, enabling automatic incremental computation for
//! SQL queries.

mod operators;
mod sources;

use reifydb_catalog::sequence::flow::{next_flow_edge_id, next_flow_node_id};
use reifydb_core::{
	interface::{
		ActiveCommandTransaction, FlowNodeId, Transaction, ViewDef,
	},
	result::error::diagnostic::flow::flow_error,
};
use reifydb_rql::plan::physical::{
	AggregateNode, FilterNode, JoinInnerNode, JoinLeftNode, MapNode,
	PhysicalPlan, SortNode, TakeNode,
};

use crate::{Flow, FlowEdge, FlowNode, FlowNodeType};

/// Public API for compiling logical plans to FlowGraphs
pub fn compile_flow<T: Transaction>(
	txn: &mut ActiveCommandTransaction<T>,
	plan: PhysicalPlan,
	sink: &ViewDef,
) -> crate::Result<Flow> {
	let mut compiler = FlowCompiler::new();
	compiler.compile(txn, plan, sink)
}

/// Compiler for converting RQL plans into executable Flows
pub(crate) struct FlowCompiler {
	/// The flow graph being built
	flow: Flow,
}

impl FlowCompiler {
	/// Creates a new FlowCompiler instance
	pub fn new() -> Self {
		Self {
			flow: Flow::new(),
		}
	}

	/// Compiles a physical plan into a FlowGraph
	pub(crate) fn compile<T: Transaction>(
		&mut self,
		txn: &mut ActiveCommandTransaction<T>,
		plan: PhysicalPlan,
		sink: &ViewDef,
	) -> crate::Result<Flow> {
		// Reset the flow for this compilation
		self.flow = Flow::new();

		// Compile the physical plan tree into the dataflow graph
		let root_node_id = self.compile_plan(txn, plan)?;

		// Create the sink node for the view
		let result_node = self.flow.add_node(FlowNode::new(
			next_flow_node_id(txn)?,
			FlowNodeType::SinkView {
				name: sink.name.clone(),
				view: sink.id,
			},
		));
		self.flow.add_edge(FlowEdge::new(
			next_flow_edge_id(txn)?,
			&root_node_id,
			&result_node,
		))?;

		// Return the flow, replacing it with a new one
		let result = self.flow.clone();
		self.flow = Flow::new();
		Ok(result)
	}

	/// Compiles a physical plan node into the FlowGraph
	fn compile_plan<T: Transaction>(
		&mut self,
		txn: &mut ActiveCommandTransaction<T>,
		plan: PhysicalPlan,
	) -> crate::Result<FlowNodeId> {
		match plan {
			// Leaf nodes (data sources)
			PhysicalPlan::TableScan(table_scan) => {
				self.compile_table_scan(txn, table_scan)
			}
			PhysicalPlan::InlineData(inline_data) => {
				self.compile_inline_data(inline_data)
			}

			// Unary operators (single input)
			PhysicalPlan::Filter(filter) => {
				let FilterNode {
					input,
					conditions,
				} = filter;
				let input_node =
					self.compile_plan(txn, *input)?;
				self.compile_filter(txn, conditions, input_node)
			}
			PhysicalPlan::Map(map) => {
				let MapNode {
					input,
					map: expressions,
				} = map;
				let input_node = if let Some(input) = input {
					Some(self.compile_plan(txn, *input)?)
				} else {
					None
				};
				self.compile_map(txn, expressions, input_node)
			}
			PhysicalPlan::Aggregate(aggregate) => {
				let AggregateNode {
					input,
					by,
					map,
				} = aggregate;
				let input_node =
					self.compile_plan(txn, *input)?;
				self.compile_aggregate(txn, by, map, input_node)
			}
			PhysicalPlan::Take(take) => {
				let TakeNode {
					input,
					take: limit,
				} = take;
				let input_node =
					self.compile_plan(txn, *input)?;
				self.compile_take(txn, limit, input_node)
			}
			PhysicalPlan::Sort(sort) => {
				let SortNode {
					input,
					by,
				} = sort;
				let input_node =
					self.compile_plan(txn, *input)?;
				self.compile_sort(txn, by, input_node)
			}

			// Binary operators (two inputs)
			PhysicalPlan::JoinInner(join) => {
				let JoinInnerNode {
					left,
					right,
					on,
				} = join;
				let left_node =
					self.compile_plan(txn, *left)?;
				let right_node =
					self.compile_plan(txn, *right)?;
				self.compile_join_inner(
					txn, on, left_node, right_node,
				)
			}
			PhysicalPlan::JoinLeft(join) => {
				let JoinLeftNode {
					left,
					right,
					on,
				} = join;
				let left_node =
					self.compile_plan(txn, *left)?;
				let right_node =
					self.compile_plan(txn, *right)?;
				self.compile_join_left(
					txn, on, left_node, right_node,
				)
			}
			PhysicalPlan::JoinNatural(_) => {
				return Err(reifydb_core::Error(flow_error(
					"Natural joins not yet implemented in dataflow".to_string(),
				)));
			}

			// DDL operations
			PhysicalPlan::CreateSchema(_)
			| PhysicalPlan::CreateTable(_)
			| PhysicalPlan::AlterSequence(_) => {
				return Err(reifydb_core::Error(flow_error(
					"DDL operations cannot be compiled to dataflow".to_string(),
				)));
			}

			// Computed view is handled specially
			PhysicalPlan::CreateComputedView(_) => {
				return Err(reifydb_core::Error(flow_error(
					"CREATE COMPUTED VIEW should be handled at a higher level".to_string(),
				)));
			}

			// DML operations
			PhysicalPlan::Insert(_)
			| PhysicalPlan::Update(_)
			| PhysicalPlan::Delete(_) => {
				return Err(reifydb_core::Error(flow_error(
					"DML operations cannot be compiled to dataflow".to_string(),
				)));
			}
		}
	}
}
