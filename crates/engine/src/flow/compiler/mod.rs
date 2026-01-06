// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Flow compilation - compiles RQL physical plans into Flows
//!
//! This module uses StandardCommandTransaction directly instead of being generic
//! over MultiVersionCommandTransaction to avoid lifetime issues with async recursion.

use reifydb_catalog::{
	CatalogStore,
	store::sequence::flow::{next_flow_edge_id, next_flow_node_id},
};
use reifydb_core::{
	Result,
	interface::{FlowEdgeDef, FlowEdgeId, FlowId, FlowNodeDef, FlowNodeId, SubscriptionDef, ViewDef},
};
use reifydb_rql::{
	flow::{Flow, FlowBuilder, FlowEdge, FlowNode, FlowNodeType},
	plan::physical::PhysicalPlan,
};
use reifydb_type::Blob;

use crate::StandardCommandTransaction;

mod operator;
mod primitive;

use operator::{
	AggregateCompiler, ApplyCompiler, DistinctCompiler, ExtendCompiler, FilterCompiler, JoinCompiler, MapCompiler,
	MergeCompiler, SortCompiler, TakeCompiler, WindowCompiler,
};
use primitive::{FlowScanCompiler, InlineDataCompiler, TableScanCompiler, ViewScanCompiler};

/// Public API for compiling logical plans to Flows with an existing flow ID.
pub fn compile_flow(
	txn: &mut StandardCommandTransaction,
	plan: PhysicalPlan,
	sink: Option<&ViewDef>,
	flow_id: FlowId,
) -> Result<Flow> {
	let compiler = FlowCompiler::new(flow_id);
	compiler.compile(txn, plan, sink)
}

pub fn compile_subscription_flow(
	txn: &mut StandardCommandTransaction,
	plan: PhysicalPlan,
	subscription: &SubscriptionDef,
	flow_id: FlowId,
) -> Result<Flow> {
	let compiler = FlowCompiler::new(flow_id);
	compiler.compile_with_subscription(txn, plan, subscription)
}

/// Compiler for converting RQL plans into executable Flows
pub(crate) struct FlowCompiler {
	/// The flow builder being used for construction
	builder: FlowBuilder,
	/// The sink view schema (for terminal nodes)
	pub(crate) sink: Option<ViewDef>,
}

impl FlowCompiler {
	/// Creates a new FlowCompiler instance with an existing flow ID
	pub fn new(flow_id: FlowId) -> Self {
		Self {
			builder: Flow::builder(flow_id),
			sink: None,
		}
	}

	/// Gets the next available operator ID
	fn next_node_id(&mut self, txn: &mut StandardCommandTransaction) -> Result<FlowNodeId> {
		next_flow_node_id(txn).map_err(Into::into)
	}

	/// Gets the next available edge ID
	fn next_edge_id(&mut self, txn: &mut StandardCommandTransaction) -> Result<FlowEdgeId> {
		next_flow_edge_id(txn).map_err(Into::into)
	}

	/// Adds an edge between two nodes
	pub(crate) fn add_edge(
		&mut self,
		txn: &mut StandardCommandTransaction,
		from: &FlowNodeId,
		to: &FlowNodeId,
	) -> Result<()> {
		let edge_id = self.next_edge_id(txn)?;
		let flow_id = self.builder.id();

		// Create the catalog entry
		let edge_def = FlowEdgeDef {
			id: edge_id,
			flow: flow_id,
			source: *from,
			target: *to,
		};

		// Persist to catalog
		CatalogStore::create_flow_edge(txn, &edge_def)?;

		// Add to in-memory builder
		self.builder.add_edge(FlowEdge::new(edge_id, *from, *to))?;
		Ok(())
	}

	/// Adds a operator to the flow graph
	pub(crate) fn add_node(
		&mut self,
		txn: &mut StandardCommandTransaction,
		node_type: FlowNodeType,
	) -> Result<FlowNodeId> {
		let node_id = self.next_node_id(txn)?;
		let flow_id = self.builder.id();

		// Serialize the node type to blob
		let data = postcard::to_stdvec(&node_type).map_err(|e| {
			reifydb_core::Error(reifydb_type::internal!("Failed to serialize FlowNodeType: {}", e))
		})?;

		// Create the catalog entry
		let node_def = FlowNodeDef {
			id: node_id,
			flow: flow_id,
			node_type: node_type.discriminator(),
			data: Blob::from(data),
		};

		// Persist to catalog
		CatalogStore::create_flow_node(txn, &node_def)?;

		// Add to in-memory builder
		self.builder.add_node(FlowNode::new(node_id, node_type));
		Ok(node_id)
	}

	/// Compiles a physical plan into a FlowGraph
	pub(crate) fn compile(
		mut self,
		txn: &mut StandardCommandTransaction,
		plan: PhysicalPlan,
		sink: Option<&ViewDef>,
	) -> Result<Flow> {
		// Store sink view for terminal nodes (if provided)
		self.sink = sink.cloned();
		let root_node_id = self.compile_plan(txn, plan)?;

		// Only add SinkView node if sink is provided
		if let Some(sink_view) = sink {
			let result_node = self.add_node(
				txn,
				FlowNodeType::SinkView {
					view: sink_view.id,
				},
			)?;

			self.add_edge(txn, &root_node_id, &result_node)?;
		}

		Ok(self.builder.build())
	}

	/// Compiles a physical plan into a FlowGraph with a subscription sink
	pub(crate) fn compile_with_subscription(
		mut self,
		txn: &mut StandardCommandTransaction,
		plan: PhysicalPlan,
		subscription: &SubscriptionDef,
	) -> Result<Flow> {
		let root_node_id = self.compile_plan(txn, plan)?;

		// Add SinkSubscription node
		let result_node = self.add_node(
			txn,
			FlowNodeType::SinkSubscription {
				subscription: subscription.id,
			},
		)?;

		self.add_edge(txn, &root_node_id, &result_node)?;

		Ok(self.builder.build())
	}

	/// Compiles a physical plan operator into the FlowGraph
	///
	/// Uses async_recursion to handle the recursive async calls.
	/// With the concrete StandardCommandTransaction type, the future is Send.

	pub(crate) fn compile_plan(
		&mut self,
		txn: &mut StandardCommandTransaction,
		plan: PhysicalPlan,
	) -> Result<FlowNodeId> {
		match plan {
			PhysicalPlan::IndexScan(_index_scan) => {
				// TODO: Implement IndexScanCompiler for flow
				unimplemented!("IndexScan compilation not yet implemented for flow")
			}
			PhysicalPlan::TableScan(table_scan) => TableScanCompiler::from(table_scan).compile(self, txn),
			PhysicalPlan::ViewScan(view_scan) => ViewScanCompiler::from(view_scan).compile(self, txn),
			PhysicalPlan::InlineData(inline_data) => {
				InlineDataCompiler::from(inline_data).compile(self, txn)
			}
			PhysicalPlan::Filter(filter) => FilterCompiler::from(filter).compile(self, txn),
			PhysicalPlan::Map(map) => MapCompiler::from(map).compile(self, txn),
			PhysicalPlan::Extend(extend) => ExtendCompiler::from(extend).compile(self, txn),
			PhysicalPlan::Apply(apply) => ApplyCompiler::from(apply).compile(self, txn),
			PhysicalPlan::Aggregate(aggregate) => AggregateCompiler::from(aggregate).compile(self, txn),
			PhysicalPlan::Distinct(distinct) => DistinctCompiler::from(distinct).compile(self, txn),
			PhysicalPlan::Take(take) => TakeCompiler::from(take).compile(self, txn),
			PhysicalPlan::Sort(sort) => SortCompiler::from(sort).compile(self, txn),
			PhysicalPlan::JoinInner(join) => JoinCompiler::from(join).compile(self, txn),
			PhysicalPlan::JoinLeft(join) => JoinCompiler::from(join).compile(self, txn),
			PhysicalPlan::JoinNatural(_) => {
				unimplemented!()
			}
			PhysicalPlan::Merge(merge) => MergeCompiler::from(merge).compile(self, txn),

			PhysicalPlan::CreateNamespace(_)
			| PhysicalPlan::CreateTable(_)
			| PhysicalPlan::CreateRingBuffer(_)
			| PhysicalPlan::CreateFlow(_)
			| PhysicalPlan::CreateDictionary(_)
			| PhysicalPlan::CreateSubscription(_)
			| PhysicalPlan::AlterSequence(_)
			| PhysicalPlan::AlterTable(_)
			| PhysicalPlan::AlterView(_)
			| PhysicalPlan::AlterFlow(_)
			| PhysicalPlan::CreateDeferredView(_)
			| PhysicalPlan::CreateTransactionalView(_)
			| PhysicalPlan::InsertTable(_)
			| PhysicalPlan::InsertRingBuffer(_)
			| PhysicalPlan::InsertDictionary(_)
			| PhysicalPlan::Update(_)
			| PhysicalPlan::UpdateRingBuffer(_)
			| PhysicalPlan::Delete(_)
			| PhysicalPlan::DeleteRingBuffer(_) => {
				unreachable!()
			}
			PhysicalPlan::FlowScan(flow_scan) => FlowScanCompiler::from(flow_scan).compile(self, txn),
			PhysicalPlan::TableVirtualScan(_scan) => {
				// TODO: Implement VirtualScanCompiler
				// For now, return a placeholder
				unimplemented!("VirtualScan compilation not yet implemented")
			}
			PhysicalPlan::RingBufferScan(_scan) => {
				// TODO: Implement RingBufferScanCompiler for flow
				unimplemented!("RingBufferScan compilation not yet implemented for flow")
			}
			PhysicalPlan::Generator(_generator) => {
				// TODO: Implement GeneratorCompiler for flow
				unimplemented!("Generator compilation not yet implemented for flow")
			}
			PhysicalPlan::Window(window) => WindowCompiler::from(window).compile(self, txn),
			PhysicalPlan::Declare(_) => {
				panic!("Declare statements are not supported in flow graphs");
			}

			PhysicalPlan::Assign(_) => {
				panic!("Assign statements are not supported in flow graphs");
			}

			PhysicalPlan::Conditional(_) => {
				panic!("Conditional statements are not supported in flow graphs");
			}

			PhysicalPlan::Variable(_) => {
				panic!("Variable references are not supported in flow graphs");
			}

			PhysicalPlan::Scalarize(_) => {
				panic!("Scalarize operations are not supported in flow graphs");
			}

			PhysicalPlan::Environment(_) => {
				panic!("Environment operations are not supported in flow graphs");
			}

			PhysicalPlan::RowPointLookup(_) => {
				// TODO: Implement optimized row point lookup for flow graphs
				unimplemented!("RowPointLookup compilation not yet implemented for flow")
			}

			PhysicalPlan::RowListLookup(_) => {
				// TODO: Implement optimized row list lookup for flow graphs
				unimplemented!("RowListLookup compilation not yet implemented for flow")
			}

			PhysicalPlan::RowRangeScan(_) => {
				// TODO: Implement optimized row range scan for flow graphs
				unimplemented!("RowRangeScan compilation not yet implemented for flow")
			}

			PhysicalPlan::DictionaryScan(_) => {
				// TODO: Implement DictionaryScan for flow graphs
				unimplemented!("DictionaryScan compilation not yet implemented for flow")
			}
		}
	}
}

/// Trait for compiling operator from physical plans to flow nodes
pub(crate) trait CompileOperator {
	/// Compiles this operator into a flow operator
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut StandardCommandTransaction) -> Result<FlowNodeId>;
}
