// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation module for converting RQL plans into Flows
//!
//! This module bridges the gap between ReifyDB's RQL query processing and the
//! streaming dataflow engine, enabling automatic incremental computation for
//! RQL queries.

mod builder;
mod conversion;
mod operator;
mod source;

use reifydb_catalog::sequence::flow::{next_flow_edge_id, next_flow_id, next_flow_node_id};
use reifydb_core::{
	flow::{Flow, FlowEdge, FlowNode, FlowNodeDef, FlowNodeType},
	interface::{CommandTransaction, FlowEdgeId, FlowNodeId, ViewDef},
};

use self::{
	conversion::to_owned_physical_plan,
	operator::{
		aggregate::AggregateCompiler, apply::ApplyCompiler, distinct::DistinctCompiler, extend::ExtendCompiler,
		filter::FilterCompiler, join::JoinCompiler, map::MapCompiler, sort::SortCompiler, take::TakeCompiler,
	},
	source::{inline_data::InlineDataCompiler, table_scan::TableScanCompiler, view_scan::ViewScanCompiler},
};
use crate::plan::physical::PhysicalPlan;

/// Public API for compiling logical plans to Flows
pub fn compile_flow(txn: &mut impl CommandTransaction, plan: PhysicalPlan, sink: &ViewDef) -> crate::Result<Flow> {
	// Convert PhysicalPlan<'_> to PhysicalPlan<'static> at the boundary
	let owned_plan = to_owned_physical_plan(plan);
	let compiler = FlowCompiler::new(txn)?;
	compiler.compile(owned_plan, sink)
}

/// Compiler for converting RQL plans into executable Flows
pub(crate) struct FlowCompiler<T: CommandTransaction> {
	/// The flow graph being built
	flow: Flow,
	/// Reference to transaction for ID generation
	pub(crate) txn: *mut T,
	/// The sink view schema (for terminal nodes)
	pub(crate) sink: Option<ViewDef>,
}

impl<T: CommandTransaction> FlowCompiler<T> {
	/// Creates a new FlowCompiler instance
	pub fn new(txn: &mut T) -> crate::Result<Self> {
		Ok(Self {
			flow: Flow::new(next_flow_id(txn)?),
			txn: txn as *mut T,
			sink: None,
		})
	}

	/// Gets the next available node ID
	fn next_node_id(&mut self) -> crate::Result<FlowNodeId> {
		unsafe { next_flow_node_id(&mut *self.txn) }
	}

	/// Gets the next available edge ID
	fn next_edge_id(&mut self) -> crate::Result<FlowEdgeId> {
		unsafe { next_flow_edge_id(&mut *self.txn) }
	}

	/// Adds an edge between two nodes
	fn add_edge(&mut self, from: &FlowNodeId, to: &FlowNodeId) -> crate::Result<()> {
		let edge_id = self.next_edge_id()?;
		self.flow.add_edge(FlowEdge::new(edge_id, from, to))
	}

	/// Adds a node to the flow graph
	fn add_node(&mut self, node_type: FlowNodeType) -> crate::Result<FlowNodeId> {
		let node_id = self.next_node_id()?;
		let flow_node_id = self.flow.add_node(FlowNode::new(node_id, node_type));
		Ok(flow_node_id)
	}

	/// Compiles a physical plan into a FlowGraph
	pub(crate) fn compile(mut self, plan: PhysicalPlan, sink: &ViewDef) -> crate::Result<Flow> {
		// Store sink view for terminal nodes
		self.sink = Some(sink.clone());
		let root_node_id = self.compile_plan(plan)?;

		let result_node = self.add_node(FlowNodeType::SinkView {
			name: sink.name.clone(),
			view: sink.id,
		})?;

		self.add_edge(&root_node_id, &result_node)?;

		Ok(self.flow)
	}

	/// Compiles a physical plan node into the FlowGraph
	pub(crate) fn compile_plan(&mut self, plan: PhysicalPlan) -> crate::Result<FlowNodeId> {
		match plan {
			PhysicalPlan::IndexScan(_index_scan) => {
				// TODO: Implement IndexScanCompiler for flow
				unimplemented!("IndexScan compilation not yet implemented for flow")
			}
			PhysicalPlan::TableScan(table_scan) => TableScanCompiler::from(table_scan).compile(self),
			PhysicalPlan::ViewScan(view_scan) => ViewScanCompiler::from(view_scan).compile(self),
			PhysicalPlan::InlineData(inline_data) => InlineDataCompiler::from(inline_data).compile(self),
			PhysicalPlan::Filter(filter) => FilterCompiler::from(filter).compile(self),
			PhysicalPlan::Map(map) => MapCompiler::from(map).compile(self),
			PhysicalPlan::Extend(extend) => ExtendCompiler::from(extend).compile(self),
			PhysicalPlan::Apply(apply) => ApplyCompiler::from(apply).compile(self),
			PhysicalPlan::Aggregate(aggregate) => AggregateCompiler::from(aggregate).compile(self),
			PhysicalPlan::Distinct(distinct) => DistinctCompiler::from(distinct).compile(self),
			PhysicalPlan::Take(take) => TakeCompiler::from(take).compile(self),
			PhysicalPlan::Sort(sort) => SortCompiler::from(sort).compile(self),
			PhysicalPlan::JoinInner(join) => JoinCompiler::from(join).compile(self),
			PhysicalPlan::JoinLeft(join) => JoinCompiler::from(join).compile(self),
			PhysicalPlan::JoinNatural(_) => {
				unimplemented!()
			}

			PhysicalPlan::CreateNamespace(_)
			| PhysicalPlan::CreateTable(_)
			| PhysicalPlan::CreateRingBuffer(_)
			| PhysicalPlan::AlterSequence(_)
			| PhysicalPlan::AlterTable(_)
			| PhysicalPlan::AlterView(_)
			| PhysicalPlan::CreateDeferredView(_)
			| PhysicalPlan::CreateTransactionalView(_)
			| PhysicalPlan::InsertTable(_)
			| PhysicalPlan::InsertRingBuffer(_)
			| PhysicalPlan::Update(_)
			| PhysicalPlan::UpdateRingBuffer(_)
			| PhysicalPlan::Delete(_)
			| PhysicalPlan::DeleteRingBuffer(_) => {
				unreachable!()
			}
			PhysicalPlan::TableVirtualScan(_scan) => {
				// TODO: Implement VirtualScanCompiler
				// For now, return a placeholder
				unimplemented!("VirtualScan compilation not yet implemented")
			}
			PhysicalPlan::RingBufferScan(_scan) => {
				// TODO: Implement RingBufferScanCompiler for flow
				unimplemented!("RingBufferScan compilation not yet implemented for flow")
			}
		}
	}

	/// Compiles a physical plan and returns both the node ID and its output
	/// namespace
	pub(crate) fn compile_plan_with_definition(
		&mut self,
		plan: PhysicalPlan,
	) -> crate::Result<(FlowNodeId, FlowNodeDef)> {
		match plan {
			PhysicalPlan::TableScan(table_scan) => {
				// The table_scan already has the table
				// definition
				let table = table_scan.source.def();
				let namespace_def = table_scan.source.namespace().def();

				let namespace = FlowNodeDef::new(
					table.columns.clone(),
					Some(namespace_def.name.clone()),
					Some(table.name.clone()),
				);

				let node_id = TableScanCompiler::from(table_scan).compile(self)?;
				Ok((node_id, namespace))
			}
			PhysicalPlan::ViewScan(view_scan) => {
				// The view_scan already has the view definition
				let view = view_scan.source.def();
				let namespace_def = view_scan.source.namespace().def();

				let namespace = FlowNodeDef::new(
					view.columns.clone(),
					Some(namespace_def.name.clone()),
					Some(view.name.clone()),
				);

				let node_id = ViewScanCompiler::from(view_scan).compile(self)?;
				Ok((node_id, namespace))
			}
			PhysicalPlan::JoinInner(join) => {
				// The JoinCompiler will handle compilation with
				// namespace tracking
				let node_id = JoinCompiler::from(join).compile(self)?;
				// For now return empty namespace - we could
				// extract it from the flow node if needed
				Ok((node_id, FlowNodeDef::empty()))
			}
			PhysicalPlan::JoinLeft(join) => {
				// The JoinCompiler will handle compilation with
				// namespace tracking
				let node_id = JoinCompiler::from(join).compile(self)?;
				// For now return empty namespace - we could
				// extract it from the flow node if needed
				Ok((node_id, FlowNodeDef::empty()))
			}
			PhysicalPlan::Map(map) => {
				// Map needs special handling to compute output
				// namespace
				let map_compiler = MapCompiler::from(map);

				// Get input namespace if there's an input
				let input_schema = if let Some(ref input) = map_compiler.input {
					// We need to compile the input first to
					// get its namespace Clone the input
					// since we need to use it twice
					let input_plan = to_owned_physical_plan(*input.clone());
					let (_, namespace) = self.compile_plan_with_definition(input_plan)?;
					namespace
				} else {
					FlowNodeDef::empty()
				};

				// Compute the output namespace
				// Pass the sink view schema for type inference
				let output_schema =
					map_compiler.compute_output_schema(&input_schema, self.sink.as_ref());

				// Now compile the Map node
				let node_id = map_compiler.compile(self)?;

				Ok((node_id, output_schema))
			}
			// For other operators, compile normally and return
			// empty namespace for now
			_ => {
				let node_id = self.compile_plan(plan)?;
				Ok((node_id, FlowNodeDef::empty()))
			}
		}
	}
}

/// Trait for compiling operator from physical plans to flow nodes
pub(crate) trait CompileOperator<T: CommandTransaction> {
	/// Compiles this operator into a flow node
	fn compile(self, compiler: &mut FlowCompiler<T>) -> crate::Result<FlowNodeId>;
}
