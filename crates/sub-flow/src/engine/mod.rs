// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod eval;
pub mod process;
pub mod register;

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	sync::Arc,
};

#[cfg(reifydb_target = "native")]
use postcard::to_stdvec;
use reifydb_catalog::catalog::Catalog;
#[cfg(reifydb_target = "native")]
use reifydb_core::internal;
use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::catalog::{
		flow::{FlowId, FlowNodeId},
		id::{TableId, ViewId},
		shape::ShapeId,
	},
};
use reifydb_engine::vm::executor::Executor;
#[cfg(reifydb_target = "native")]
use reifydb_extension::operator::ffi_loader::ffi_operator_loader;
use reifydb_rql::flow::{
	analyzer::{FlowDependencyGraph, FlowGraphAnalyzer},
	flow::FlowDag,
};
use reifydb_runtime::context::{RuntimeContext, clock::Clock};
#[cfg(reifydb_target = "native")]
use reifydb_type::{Result, error::Error, value::Value};
use tracing::instrument;

#[cfg(reifydb_target = "native")]
use crate::operator::BoxedOperator;
#[cfg(reifydb_target = "native")]
use crate::operator::ffi::FFIOperator;
use crate::{builder::OperatorFactory, operator::Operators};

pub struct FlowEngine {
	pub(crate) catalog: Catalog,
	pub(crate) executor: Executor,
	pub operators: BTreeMap<FlowNodeId, Arc<Operators>>,
	pub flows: BTreeMap<FlowId, FlowDag>,
	pub sources: BTreeMap<ShapeId, Vec<(FlowId, FlowNodeId)>>,
	pub sinks: BTreeMap<ShapeId, Vec<(FlowId, FlowNodeId)>>,
	pub analyzer: FlowGraphAnalyzer,
	#[allow(dead_code)]
	pub(crate) event_bus: EventBus,
	pub(crate) flow_creation_versions: BTreeMap<FlowId, CommitVersion>,
	pub(crate) runtime_context: RuntimeContext,
	pub(crate) custom_operators: Arc<HashMap<String, OperatorFactory>>,
}

impl FlowEngine {
	#[instrument(
		name = "flow::engine::new",
		level = "debug",
		skip(catalog, executor, event_bus, runtime_context, custom_operators)
	)]
	pub fn new(
		catalog: Catalog,
		executor: Executor,
		event_bus: EventBus,
		runtime_context: RuntimeContext,
		custom_operators: Arc<HashMap<String, OperatorFactory>>,
	) -> Self {
		Self {
			catalog,
			executor,
			operators: BTreeMap::new(),
			flows: BTreeMap::new(),
			sources: BTreeMap::new(),
			sinks: BTreeMap::new(),
			analyzer: FlowGraphAnalyzer::new(),
			event_bus,
			flow_creation_versions: BTreeMap::new(),
			runtime_context,
			custom_operators,
		}
	}

	pub fn clock(&self) -> &Clock {
		&self.runtime_context.clock
	}

	#[cfg(reifydb_target = "native")]
	#[instrument(name = "flow::engine::create_ffi_operator", level = "debug", skip(self, config), fields(operator = %operator, node_id = ?node_id))]
	pub(crate) fn create_ffi_operator(
		&self,
		operator: &str,
		node_id: FlowNodeId,
		config: &BTreeMap<String, Value>,
	) -> Result<BoxedOperator> {
		let loader = ffi_operator_loader();
		let mut loader_write = loader.write().unwrap();

		let config_bytes = to_stdvec(config)
			.map_err(|e| Error(Box::new(internal!("Failed to serialize operator config: {:?}", e))))?;

		let (descriptor, instance) = loader_write
			.create_operator_by_name(operator, node_id, &config_bytes)
			.map_err(|e| Error(Box::new(internal!("Failed to create FFI operator: {:?}", e))))?;

		Ok(Box::new(FFIOperator::new(descriptor, instance, node_id, self.executor.clone())))
	}

	#[cfg(reifydb_target = "native")]
	pub(crate) fn is_ffi_operator(&self, operator: &str) -> bool {
		let loader = ffi_operator_loader();
		let loader_read = loader.read().unwrap();
		loader_read.has_operator(operator)
	}

	#[cfg(not(reifydb_target = "native"))]
	#[allow(dead_code)]
	pub(crate) fn is_ffi_operator(&self, _operator: &str) -> bool {
		false
	}

	pub fn flow_ids(&self) -> BTreeSet<FlowId> {
		self.flows.keys().copied().collect()
	}

	pub fn clear(&mut self) {
		self.operators.clear();
		self.flows.clear();
		self.sources.clear();
		self.sinks.clear();
		self.analyzer.clear();
		self.flow_creation_versions.clear();
	}

	pub fn remove_flow(&mut self, flow_id: FlowId) {
		let node_ids: Vec<FlowNodeId> =
			self.flows.get(&flow_id).map(|flow| flow.get_node_ids().collect()).unwrap_or_default();

		for node_id in node_ids {
			self.operators.remove(&node_id);
		}

		for entries in self.sources.values_mut() {
			entries.retain(|(fid, _)| *fid != flow_id);
		}
		self.sources.retain(|_, v| !v.is_empty());

		for entries in self.sinks.values_mut() {
			entries.retain(|(fid, _)| *fid != flow_id);
		}
		self.sinks.retain(|_, v| !v.is_empty());

		self.flows.remove(&flow_id);

		self.analyzer.remove(flow_id);
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

	pub fn calculate_execution_levels(&self) -> Vec<Vec<FlowId>> {
		let dependency_graph = self.analyzer.get_dependency_graph();
		self.analyzer.calculate_execution_levels(dependency_graph)
	}
}
