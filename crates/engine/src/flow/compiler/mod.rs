// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Flow compilation - compiles RQL physical plans into Flows
//!
//! This module uses AdminTransaction directly instead of being generic
//! over MultiVersionAdminTransaction to avoid lifetime issues with async recursion.

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	interface::catalog::{
		flow::{FlowEdgeDef, FlowEdgeId, FlowId, FlowNodeDef, FlowNodeId},
		subscription::SubscriptionDef,
		view::ViewDef,
	},
	internal,
};
use reifydb_rql::{
	flow::{
		flow::{FlowBuilder, FlowDag},
		node::{FlowEdge, FlowNode, FlowNodeType},
	},
	query::QueryPlan,
};
use reifydb_type::{Result, error::Error, value::blob::Blob};

pub mod operator;
pub mod primitive;

use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::flow::compiler::{
	operator::{
		aggregate::AggregateCompiler, append::AppendCompiler, apply::ApplyCompiler, distinct::DistinctCompiler,
		extend::ExtendCompiler, filter::FilterCompiler, join::JoinCompiler, map::MapCompiler,
		sort::SortCompiler, take::TakeCompiler, window::WindowCompiler,
	},
	primitive::{
		inline_data::InlineDataCompiler, ringbuffer_scan::RingBufferScanCompiler,
		series_scan::SeriesScanCompiler, table_scan::TableScanCompiler, view_scan::ViewScanCompiler,
	},
};

/// Public API for compiling logical plans to Flows with an existing flow ID.
pub fn compile_flow(
	catalog: &Catalog,
	txn: &mut AdminTransaction,
	plan: QueryPlan,
	sink: Option<&ViewDef>,
	flow_id: FlowId,
) -> Result<FlowDag> {
	let compiler = FlowCompiler::new(catalog.clone(), flow_id);
	compiler.compile(txn, plan, sink)
}

pub fn compile_subscription_flow(
	catalog: &Catalog,
	txn: &mut AdminTransaction,
	plan: QueryPlan,
	subscription: &SubscriptionDef,
	flow_id: FlowId,
) -> Result<FlowDag> {
	let compiler = FlowCompiler::new(catalog.clone(), flow_id);
	compiler.compile_with_subscription(txn, plan, subscription)
}

/// Compiler for converting RQL plans into executable Flows
pub(crate) struct FlowCompiler {
	/// The catalog for persisting flow nodes and edges
	pub(crate) catalog: Catalog,
	/// The flow builder being used for construction
	builder: FlowBuilder,
	/// The sink view schema (for terminal nodes)
	pub(crate) sink: Option<ViewDef>,
}

impl FlowCompiler {
	/// Creates a new FlowCompiler instance with an existing flow ID
	pub fn new(catalog: Catalog, flow_id: FlowId) -> Self {
		Self {
			catalog,
			builder: FlowDag::builder(flow_id),
			sink: None,
		}
	}

	/// Gets the next available operator ID
	fn next_node_id(&mut self, txn: &mut AdminTransaction) -> Result<FlowNodeId> {
		self.catalog.next_flow_node_id(txn).map_err(Into::into)
	}

	/// Gets the next available edge ID
	fn next_edge_id(&mut self, txn: &mut AdminTransaction) -> Result<FlowEdgeId> {
		self.catalog.next_flow_edge_id(txn).map_err(Into::into)
	}

	/// Adds an edge between two nodes
	pub(crate) fn add_edge(
		&mut self,
		txn: &mut AdminTransaction,
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
		self.catalog.create_flow_edge(txn, &edge_def)?;

		// Add to in-memory builder
		self.builder.add_edge(FlowEdge::new(edge_id, *from, *to))?;
		Ok(())
	}

	/// Adds a operator to the flow graph
	pub(crate) fn add_node(&mut self, txn: &mut AdminTransaction, node_type: FlowNodeType) -> Result<FlowNodeId> {
		let node_id = self.next_node_id(txn)?;
		let flow_id = self.builder.id();

		// Serialize the node type to blob
		let data = postcard::to_stdvec(&node_type)
			.map_err(|e| Error(internal!("Failed to serialize FlowNodeType: {}", e)))?;

		// Create the catalog entry
		let node_def = FlowNodeDef {
			id: node_id,
			flow: flow_id,
			node_type: node_type.discriminator(),
			data: Blob::from(data),
		};

		// Persist to catalog
		self.catalog.create_flow_node(txn, &node_def)?;

		// Add to in-memory builder
		self.builder.add_node(FlowNode::new(node_id, node_type));
		Ok(node_id)
	}

	/// Compiles a query plan into a FlowGraph
	pub(crate) fn compile(
		mut self,
		txn: &mut AdminTransaction,
		plan: QueryPlan,
		sink: Option<&ViewDef>,
	) -> Result<FlowDag> {
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

	/// Compiles a query plan into a FlowGraph with a subscription sink
	pub(crate) fn compile_with_subscription(
		mut self,
		txn: &mut AdminTransaction,
		plan: QueryPlan,
		subscription: &SubscriptionDef,
	) -> Result<FlowDag> {
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

	/// Compiles a query plan operator into the FlowGraph
	pub(crate) fn compile_plan(&mut self, txn: &mut AdminTransaction, plan: QueryPlan) -> Result<FlowNodeId> {
		match plan {
			QueryPlan::IndexScan(_index_scan) => {
				// TODO: Implement IndexScanCompiler for flow
				unimplemented!("IndexScan compilation not yet implemented for flow")
			}
			QueryPlan::TableScan(table_scan) => TableScanCompiler::from(table_scan).compile(self, txn),
			QueryPlan::ViewScan(view_scan) => ViewScanCompiler::from(view_scan).compile(self, txn),
			QueryPlan::InlineData(inline_data) => InlineDataCompiler::from(inline_data).compile(self, txn),
			QueryPlan::Filter(filter) => FilterCompiler::from(filter).compile(self, txn),
			QueryPlan::Map(map) => MapCompiler::from(map).compile(self, txn),
			QueryPlan::Extend(extend) => ExtendCompiler::from(extend).compile(self, txn),
			QueryPlan::Apply(apply) => ApplyCompiler::from(apply).compile(self, txn),
			QueryPlan::Aggregate(aggregate) => AggregateCompiler::from(aggregate).compile(self, txn),
			QueryPlan::Distinct(distinct) => DistinctCompiler::from(distinct).compile(self, txn),
			QueryPlan::Take(take) => TakeCompiler::from(take).compile(self, txn),
			QueryPlan::Sort(sort) => SortCompiler::from(sort).compile(self, txn),
			QueryPlan::JoinInner(join) => JoinCompiler::from(join).compile(self, txn),
			QueryPlan::JoinLeft(join) => JoinCompiler::from(join).compile(self, txn),
			QueryPlan::JoinNatural(_) => {
				unimplemented!()
			}
			QueryPlan::Append(append) => AppendCompiler::from(append).compile(self, txn),
			QueryPlan::Patch(_) => {
				unimplemented!("Patch compilation not yet implemented for flow")
			}
			QueryPlan::TableVirtualScan(_scan) => {
				// TODO: Implement VirtualScanCompiler
				unimplemented!("VirtualScan compilation not yet implemented")
			}
			QueryPlan::RingBufferScan(scan) => RingBufferScanCompiler::from(scan).compile(self, txn),
			QueryPlan::Generator(_generator) => {
				// TODO: Implement GeneratorCompiler for flow
				unimplemented!("Generator compilation not yet implemented for flow")
			}
			QueryPlan::Window(window) => WindowCompiler::from(window).compile(self, txn),
			QueryPlan::Variable(_) => {
				panic!("Variable references are not supported in flow graphs");
			}
			QueryPlan::Scalarize(_) => {
				panic!("Scalarize operations are not supported in flow graphs");
			}
			QueryPlan::Environment(_) => {
				panic!("Environment operations are not supported in flow graphs");
			}
			QueryPlan::RowPointLookup(_) => {
				// TODO: Implement optimized row point lookup for flow graphs
				unimplemented!("RowPointLookup compilation not yet implemented for flow")
			}
			QueryPlan::RowListLookup(_) => {
				// TODO: Implement optimized row list lookup for flow graphs
				unimplemented!("RowListLookup compilation not yet implemented for flow")
			}
			QueryPlan::RowRangeScan(_) => {
				// TODO: Implement optimized row range scan for flow graphs
				unimplemented!("RowRangeScan compilation not yet implemented for flow")
			}
			QueryPlan::DictionaryScan(_) => {
				// TODO: Implement DictionaryScan for flow graphs
				unimplemented!("DictionaryScan compilation not yet implemented for flow")
			}
			QueryPlan::Assert(_) => {
				unimplemented!("Assert compilation not yet implemented for flow")
			}
			QueryPlan::SeriesScan(series_scan) => SeriesScanCompiler::from(series_scan).compile(self, txn),
		}
	}
}

/// Trait for compiling operator from physical plans to flow nodes
pub(crate) trait CompileOperator {
	/// Compiles this operator into a flow operator
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut AdminTransaction) -> Result<FlowNodeId>;
}
