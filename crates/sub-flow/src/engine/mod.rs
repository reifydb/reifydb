// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod eval;
pub mod process;
pub mod register;

use std::{
	collections::{HashMap, HashSet},
	rc::Rc,
};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::catalog::{
		flow::{FlowId, FlowNodeId},
		id::{TableId, ViewId},
		primitive::PrimitiveId,
	},
	internal,
};
use reifydb_engine::{evaluate::column::StandardColumnEvaluator, execute::Executor};
use reifydb_runtime::clock::Clock;
use reifydb_rql::flow::{
	analyzer::{FlowDependencyGraph, FlowGraphAnalyzer},
	flow::FlowDag,
};
use reifydb_type::{error::Error, value::Value};
use tracing::instrument;

#[cfg(reifydb_target = "native")]
use crate::ffi::loader::ffi_operator_loader;
use crate::operator::{BoxedOperator, Operators};

pub struct FlowEngine {
	pub(crate) catalog: Catalog,
	pub(crate) evaluator: StandardColumnEvaluator,
	pub(crate) executor: Executor,
	pub(crate) operators: HashMap<FlowNodeId, Rc<Operators>>,
	pub(crate) flows: HashMap<FlowId, FlowDag>,
	pub(crate) sources: HashMap<PrimitiveId, Vec<(FlowId, FlowNodeId)>>,
	pub(crate) sinks: HashMap<PrimitiveId, Vec<(FlowId, FlowNodeId)>>,
	pub(crate) analyzer: FlowGraphAnalyzer,
	#[allow(dead_code)]
	pub(crate) event_bus: EventBus,
	pub(crate) flow_creation_versions: HashMap<FlowId, CommitVersion>,
	pub(crate) clock: Clock,
}

impl FlowEngine {
	#[instrument(name = "flow::engine::new", level = "debug", skip(catalog, evaluator, executor, event_bus, clock))]
	pub fn new(
		catalog: Catalog,
		evaluator: StandardColumnEvaluator,
		executor: Executor,
		event_bus: EventBus,
		clock: Clock,
	) -> Self {
		Self {
			catalog,
			evaluator,
			executor,
			operators: HashMap::new(),
			flows: HashMap::new(),
			sources: HashMap::new(),
			sinks: HashMap::new(),
			analyzer: FlowGraphAnalyzer::new(),
			event_bus,
			flow_creation_versions: HashMap::new(),
			clock,
		}
	}

	/// Create an FFI operator instance from the global singleton loader
	#[cfg(reifydb_target = "native")]
	#[instrument(name = "flow::engine::create_ffi_operator", level = "debug", skip(self, config), fields(operator = %operator, node_id = ?node_id))]
	pub(crate) fn create_ffi_operator(
		&self,
		operator: &str,
		node_id: FlowNodeId,
		config: &HashMap<String, Value>,
	) -> reifydb_type::Result<BoxedOperator> {
		let loader = ffi_operator_loader();
		let mut loader_write = loader.write().unwrap();

		// Serialize config to postcard
		let config_bytes = postcard::to_stdvec(config)
			.map_err(|e| Error(internal!("Failed to serialize operator config: {:?}", e)))?;

		let operator = loader_write
			.create_operator_by_name(operator, node_id, &config_bytes)
			.map_err(|e| Error(internal!("Failed to create FFI operator: {:?}", e)))?;

		Ok(Box::new(operator))
	}

	/// Check if an operator name corresponds to an FFI operator
	#[cfg(reifydb_target = "native")]
	pub(crate) fn is_ffi_operator(&self, operator: &str) -> bool {
		let loader = ffi_operator_loader();
		let loader_read = loader.read().unwrap();
		loader_read.has_operator(operator)
	}

	/// FFI operators are not supported in WASM
	#[cfg(not(reifydb_target = "native"))]
	pub(crate) fn is_ffi_operator(&self, _operator: &str) -> bool {
		false
	}

	/// Returns a set of all currently registered flow IDs
	pub fn flow_ids(&self) -> HashSet<FlowId> {
		self.flows.keys().copied().collect()
	}

	/// Clears all registered flows, operators, sources, sinks, dependency graph, and backfill versions
	pub fn clear(&mut self) {
		self.operators.clear();
		self.flows.clear();
		self.sources.clear();
		self.sinks.clear();
		self.analyzer.clear();
		self.flow_creation_versions.clear();
	}

	pub fn get_dependency_graph(&self) -> FlowDependencyGraph {
		self.analyzer.get_dependency_graph().clone()
	}

	pub fn get_flows_depending_on_table(&self, table_id: TableId) -> Vec<FlowId> {
		let dependency_graph = self.analyzer.get_dependency_graph();
		self.analyzer.get_flows_depending_on_table(dependency_graph, table_id)
	}

	pub fn get_flows_depending_on_view(&self, view_id: ViewId) -> Vec<FlowId> {
		let dependency_graph = self.analyzer.get_dependency_graph();
		self.analyzer.get_flows_depending_on_view(dependency_graph, view_id)
	}

	pub fn get_flow_producing_view(&self, view_id: ViewId) -> Option<FlowId> {
		let dependency_graph = self.analyzer.get_dependency_graph();
		self.analyzer.get_flow_producing_view(dependency_graph, view_id)
	}

	pub fn calculate_execution_order(&self) -> Vec<FlowId> {
		let dependency_graph = self.analyzer.get_dependency_graph();
		self.analyzer.calculate_execution_order(dependency_graph)
	}
}
