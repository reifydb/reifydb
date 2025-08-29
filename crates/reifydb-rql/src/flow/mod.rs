// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation module for converting RQL plans into Flows
//!
//! This module bridges the gap between ReifyDB's RQL query processing and the
//! streaming dataflow engine, enabling automatic incremental computation for
//! RQL queries.

mod builder;
mod operator;
mod source;

use reifydb_catalog::sequence::flow::{
	next_flow_edge_id, next_flow_id, next_flow_node_id,
};
use reifydb_core::{
	flow::{Flow, FlowEdge, FlowNode, FlowNodeType},
	interface::{CommandTransaction, FlowEdgeId, FlowNodeId, ViewDef},
};

use self::{
	operator::{
		aggregate::AggregateCompiler, distinct::DistinctCompiler,
		extend::ExtendCompiler, filter::FilterCompiler,
		join::JoinCompiler, map::MapCompiler, sort::SortCompiler,
		take::TakeCompiler,
	},
	source::{
		inline_data::InlineDataCompiler, table_scan::TableScanCompiler,
	},
};
use crate::plan::physical::PhysicalPlan;

/// Public API for compiling logical plans to Flows
pub fn compile_flow(
	txn: &mut impl CommandTransaction,
	plan: PhysicalPlan,
	sink: &ViewDef,
) -> crate::Result<Flow> {
	let compiler = FlowCompiler::new(txn)?;
	compiler.compile(plan, sink)
}

/// Compiler for converting RQL plans into executable Flows
pub(crate) struct FlowCompiler<'a, T: CommandTransaction> {
	/// The flow graph being built
	flow: Flow,
	/// Transaction for accessing catalog and sequences
	txn: &'a mut T,
}

impl<'a, T: CommandTransaction> FlowCompiler<'a, T> {
	/// Creates a new FlowCompiler instance
	pub fn new(txn: &'a mut T) -> crate::Result<Self> {
		Ok(Self {
			flow: Flow::new(next_flow_id(txn)?),
			txn,
		})
	}

	/// Gets the next available node ID
	fn next_node_id(&mut self) -> crate::Result<FlowNodeId> {
		next_flow_node_id(self.txn)
	}

	/// Gets the next available edge ID
	fn next_edge_id(&mut self) -> crate::Result<FlowEdgeId> {
		next_flow_edge_id(self.txn)
	}

	/// Adds an edge between two nodes
	fn add_edge(
		&mut self,
		from: &FlowNodeId,
		to: &FlowNodeId,
	) -> crate::Result<()> {
		let edge_id = self.next_edge_id()?;
		self.flow.add_edge(FlowEdge::new(edge_id, from, to))
	}

	/// Adds a node to the flow graph
	fn add_node(
		&mut self,
		node_type: FlowNodeType,
	) -> crate::Result<FlowNodeId> {
		let node_id = self.next_node_id()?;
		let flow_node_id =
			self.flow.add_node(FlowNode::new(node_id, node_type));
		Ok(flow_node_id)
	}

	/// Compiles a physical plan into a FlowGraph
	pub(crate) fn compile(
		mut self,
		plan: PhysicalPlan,
		sink: &ViewDef,
	) -> crate::Result<Flow> {
		// Check if the root plan is a Map node that should be terminal
		let root_node_id = match &plan {
			PhysicalPlan::Map(map_node) => {
				// This is a terminal map node - compile it with
				// view info
				self.compile_terminal_map(
					map_node.clone(),
					sink,
				)?
			}
			_ => {
				// Not a map or not terminal - compile normally
				self.compile_plan(plan)?
			}
		};

		let result_node = self.add_node(FlowNodeType::SinkView {
			name: sink.name.clone(),
			view: sink.id,
		})?;

		self.add_edge(&root_node_id, &result_node)?;

		Ok(self.flow)
	}

	/// Compiles a physical plan node into the FlowGraph
	pub(crate) fn compile_plan(
		&mut self,
		plan: PhysicalPlan,
	) -> crate::Result<FlowNodeId> {
		match plan {
			PhysicalPlan::TableScan(table_scan) => {
				TableScanCompiler::from(table_scan)
					.compile(self)
			}
			PhysicalPlan::ViewScan(_view_scan) => {
				// TODO: Implement ViewScanCompiler
				// For now, return a placeholder
				unimplemented!(
					"ViewScan compilation not yet implemented"
				)
			}
			PhysicalPlan::InlineData(inline_data) => {
				InlineDataCompiler::from(inline_data)
					.compile(self)
			}
			PhysicalPlan::Filter(filter) => {
				FilterCompiler::from(filter).compile(self)
			}
			PhysicalPlan::Map(map) => {
				MapCompiler::from(map).compile(self)
			}
			PhysicalPlan::Extend(extend) => {
				ExtendCompiler::from(extend).compile(self)
			}
			PhysicalPlan::Aggregate(aggregate) => {
				AggregateCompiler::from(aggregate).compile(self)
			}
			PhysicalPlan::Distinct(distinct) => {
				DistinctCompiler::from(distinct).compile(self)
			}
			PhysicalPlan::Take(take) => {
				TakeCompiler::from(take).compile(self)
			}
			PhysicalPlan::Sort(sort) => {
				SortCompiler::from(sort).compile(self)
			}
			PhysicalPlan::JoinInner(join) => {
				JoinCompiler::from(join).compile(self)
			}
			PhysicalPlan::JoinLeft(join) => {
				JoinCompiler::from(join).compile(self)
			}
			PhysicalPlan::JoinNatural(_) => {
				unimplemented!()
			}

			PhysicalPlan::CreateSchema(_)
			| PhysicalPlan::CreateTable(_)
			| PhysicalPlan::AlterSequence(_)
			| PhysicalPlan::CreateDeferredView(_)
			| PhysicalPlan::CreateTransactionalView(_)
			| PhysicalPlan::Insert(_)
			| PhysicalPlan::Update(_)
			| PhysicalPlan::Delete(_) => {
				unreachable!()
			}
		}
	}

	/// Compiles a terminal Map node with view information
	pub(crate) fn compile_terminal_map(
		&mut self,
		map_node: crate::plan::physical::MapNode,
		sink: &ViewDef,
	) -> crate::Result<FlowNodeId> {
		// First compile the input if it exists
		let input_node = if let Some(input) = map_node.input {
			Some(self.compile_plan(*input)?)
		} else {
			None
		};

		// Create a MapTerminal operator with the view ID
		let mut builder = self.build_node(FlowNodeType::Operator {
			operator:
				reifydb_core::flow::OperatorType::MapTerminal {
					expressions: map_node.map,
					view_id: sink.id,
				},
		});

		if let Some(input) = input_node {
			builder = builder.with_input(input);
		}

		builder.build()
	}
}

/// Trait for compiling operator from physical plans to flow nodes
pub(crate) trait CompileOperator<T: CommandTransaction> {
	/// Compiles this operator into a flow node
	fn compile(
		self,
		compiler: &mut FlowCompiler<T>,
	) -> crate::Result<FlowNodeId>;
}
