// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	error::diagnostic::flow::{
		flow_ephemeral_id_capacity_exceeded, flow_remote_source_unsupported, flow_source_required,
	},
	interface::catalog::{
		flow::{FlowEdge, FlowEdgeId, FlowId, FlowNode, FlowNodeId},
		id::SubscriptionId,
		view::View,
	},
	internal,
};
use reifydb_rql::{
	flow::{
		flow::{FlowBuilder, FlowDag},
		node::{self, FlowNodeType},
	},
	query::QueryPlan,
};
use reifydb_type::{Result, error::Error, value::blob::Blob};

pub mod operator;
pub mod primitive;

use postcard::to_stdvec;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::flow::compiler::{
	operator::{
		aggregate::AggregateCompiler, append::AppendCompiler, apply::ApplyCompiler, distinct::DistinctCompiler,
		extend::ExtendCompiler, filter::FilterCompiler, gate::GateCompiler, join::JoinCompiler,
		map::MapCompiler, sort::SortCompiler, take::TakeCompiler, window::WindowCompiler,
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
	sink: Option<&View>,
	flow_id: FlowId,
) -> Result<FlowDag> {
	let compiler = FlowCompiler::new(catalog.clone(), flow_id);
	compiler.compile(&mut Transaction::Admin(txn), plan, sink)
}

/// Compile a subscription flow without persisting to the catalog.
///
/// Uses local ID counters and skips catalog writes. The resulting FlowDag
/// is used for ephemeral in-memory subscription flow registration.
pub fn compile_subscription_flow_ephemeral(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	plan: QueryPlan,
	subscription_id: SubscriptionId,
	flow_id: FlowId,
) -> Result<FlowDag> {
	let compiler = FlowCompiler::new_ephemeral(catalog.clone(), flow_id);
	compiler.compile_with_subscription_id(txn, plan, subscription_id)
}

/// Compiler for converting RQL plans into executable Flows
pub(crate) struct FlowCompiler {
	/// The catalog for persisting flow nodes and edges
	pub(crate) catalog: Catalog,
	/// The flow builder being used for construction
	builder: FlowBuilder,
	/// The sink view shape (for terminal nodes)
	pub(crate) sink: Option<View>,
	/// When true, skip catalog persistence and use local ID counters.
	ephemeral: bool,
	/// Local node ID counter for ephemeral mode.
	local_node_counter: u64,
	/// Local edge ID counter for ephemeral mode.
	local_edge_counter: u64,
	/// Maximum ID value before overflow in ephemeral mode.
	local_id_limit: u64,
}

impl FlowCompiler {
	/// Creates a new FlowCompiler instance with an existing flow ID
	pub fn new(catalog: Catalog, flow_id: FlowId) -> Self {
		Self {
			catalog,
			builder: FlowDag::builder(flow_id),
			sink: None,
			ephemeral: false,
			local_node_counter: 0,
			local_edge_counter: 0,
			local_id_limit: 0,
		}
	}

	/// Creates a new ephemeral FlowCompiler that builds in-memory only.
	///
	/// Does not persist nodes/edges to the catalog and uses local ID counters.
	/// Node/edge IDs are offset by `flow_id * 100` to avoid collisions when
	/// multiple ephemeral flows share the same FlowEngine. Each flow is limited to 99 IDs.
	pub fn new_ephemeral(catalog: Catalog, flow_id: FlowId) -> Self {
		let base = flow_id.0 * 100;
		Self {
			catalog,
			builder: FlowDag::builder(flow_id),
			sink: None,
			ephemeral: true,
			local_node_counter: base,
			local_edge_counter: base,
			local_id_limit: base + 99,
		}
	}

	/// Gets the next available operator ID
	fn next_node_id(&mut self, txn: &mut Transaction<'_>) -> Result<FlowNodeId> {
		if self.ephemeral {
			if self.local_node_counter >= self.local_id_limit {
				return Err(Error(Box::new(flow_ephemeral_id_capacity_exceeded(self.builder.id().0))));
			}
			self.local_node_counter += 1;
			Ok(FlowNodeId(self.local_node_counter))
		} else {
			self.catalog.next_flow_node_id(txn.admin_mut())
		}
	}

	/// Gets the next available edge ID
	fn next_edge_id(&mut self, txn: &mut Transaction<'_>) -> Result<FlowEdgeId> {
		if self.ephemeral {
			if self.local_edge_counter >= self.local_id_limit {
				return Err(Error(Box::new(flow_ephemeral_id_capacity_exceeded(self.builder.id().0))));
			}
			self.local_edge_counter += 1;
			Ok(FlowEdgeId(self.local_edge_counter))
		} else {
			self.catalog.next_flow_edge_id(txn.admin_mut())
		}
	}

	/// Adds an edge between two nodes
	pub(crate) fn add_edge(&mut self, txn: &mut Transaction<'_>, from: &FlowNodeId, to: &FlowNodeId) -> Result<()> {
		let edge_id = self.next_edge_id(txn)?;
		let flow_id = self.builder.id();

		if !self.ephemeral {
			// Create the catalog entry
			let edge_def = FlowEdge {
				id: edge_id,
				flow: flow_id,
				source: *from,
				target: *to,
			};

			// Persist to catalog
			self.catalog.create_flow_edge(txn.admin_mut(), &edge_def)?;
		}

		// Add to in-memory builder
		self.builder.add_edge(node::FlowEdge::new(edge_id, *from, *to))?;
		Ok(())
	}

	/// Adds a operator to the flow graph
	pub(crate) fn add_node(&mut self, txn: &mut Transaction<'_>, node_type: FlowNodeType) -> Result<FlowNodeId> {
		let node_id = self.next_node_id(txn)?;
		let flow_id = self.builder.id();

		if !self.ephemeral {
			// Serialize the node type to blob
			let data = to_stdvec(&node_type)
				.map_err(|e| Error(Box::new(internal!("Failed to serialize FlowNodeType: {}", e))))?;

			// Create the catalog entry
			let node_def = FlowNode {
				id: node_id,
				flow: flow_id,
				node_type: node_type.discriminator(),
				data: Blob::from(data),
			};

			// Persist to catalog
			self.catalog.create_flow_node(txn.admin_mut(), &node_def)?;
		}

		// Add to in-memory builder
		self.builder.add_node(node::FlowNode::new(node_id, node_type));
		Ok(node_id)
	}

	/// Compiles a query plan into a FlowGraph
	pub(crate) fn compile(
		mut self,
		txn: &mut Transaction<'_>,
		plan: QueryPlan,
		sink: Option<&View>,
	) -> Result<FlowDag> {
		// Store sink view for terminal nodes (if provided)
		self.sink = sink.cloned();
		let root_node_id = self.compile_plan(txn, plan)?;

		if let Some(sink_view) = sink {
			let node_type = match sink_view {
				View::Table(t) => FlowNodeType::SinkTableView {
					view: sink_view.id(),
					table: t.underlying,
				},
				View::RingBuffer(rb) => FlowNodeType::SinkRingBufferView {
					view: sink_view.id(),
					ringbuffer: rb.underlying,
					capacity: rb.capacity,
					propagate_evictions: rb.propagate_evictions,
				},
				View::Series(s) => FlowNodeType::SinkSeriesView {
					view: sink_view.id(),
					series: s.underlying,
					key: s.key.clone(),
				},
			};
			let result_node = self.add_node(txn, node_type)?;
			self.add_edge(txn, &root_node_id, &result_node)?;
		}

		let flow = self.builder.build();

		if !has_real_source(&flow) {
			return Err(Error(Box::new(flow_source_required())));
		}

		Ok(flow)
	}

	pub(crate) fn compile_with_subscription_id(
		mut self,
		txn: &mut Transaction<'_>,
		plan: QueryPlan,
		subscription_id: SubscriptionId,
	) -> Result<FlowDag> {
		let root_node_id = self.compile_plan(txn, plan)?;

		// Add SinkSubscription node
		let result_node = self.add_node(
			txn,
			FlowNodeType::SinkSubscription {
				subscription: subscription_id,
			},
		)?;

		self.add_edge(txn, &root_node_id, &result_node)?;

		let flow = self.builder.build();

		if !has_real_source(&flow) {
			return Err(Error(Box::new(flow_source_required())));
		}

		Ok(flow)
	}

	/// Compiles a query plan operator into the FlowGraph
	pub(crate) fn compile_plan(&mut self, txn: &mut Transaction<'_>, plan: QueryPlan) -> Result<FlowNodeId> {
		match plan {
			QueryPlan::IndexScan(_index_scan) => {
				// TODO: Implement IndexScanCompiler for flow
				unimplemented!("IndexScan compilation not yet implemented for flow")
			}
			QueryPlan::TableScan(table_scan) => TableScanCompiler::from(table_scan).compile(self, txn),
			QueryPlan::ViewScan(view_scan) => ViewScanCompiler::from(view_scan).compile(self, txn),
			QueryPlan::InlineData(inline_data) => InlineDataCompiler::from(inline_data).compile(self, txn),
			QueryPlan::Filter(filter) => FilterCompiler::from(filter).compile(self, txn),
			QueryPlan::Gate(gate) => GateCompiler::from(gate).compile(self, txn),
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
			QueryPlan::RemoteScan(_) => Err(Error(Box::new(flow_remote_source_unsupported()))),
			QueryPlan::RunTests(_) => {
				panic!("RunTests is not supported in flow graphs");
			}
			QueryPlan::CallFunction(_) => {
				panic!("CallFunction is not supported in flow graphs");
			}
		}
	}
}

/// Returns true if the flow contains at least one real source node
/// (i.e., not just inline data).
fn has_real_source(flow: &FlowDag) -> bool {
	flow.get_node_ids().any(|node_id| {
		if let Some(node) = flow.get_node(&node_id) {
			matches!(
				node.ty,
				FlowNodeType::SourceTable { .. }
					| FlowNodeType::SourceView { .. } | FlowNodeType::SourceFlow { .. }
					| FlowNodeType::SourceRingBuffer { .. }
					| FlowNodeType::SourceSeries { .. }
			)
		} else {
			false
		}
	})
}

/// Trait for compiling operator from physical plans to flow nodes
pub(crate) trait CompileOperator {
	/// Compiles this operator into a flow operator
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<FlowNodeId>;
}
