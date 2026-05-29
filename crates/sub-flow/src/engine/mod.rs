// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Flow execution engine. Registers compiled flow definitions, evaluates each flow's operator graph against
//! incoming change deltas, and writes the resulting outputs back through the catalog. Process drives the per-tick
//! work; eval is where individual operators run; register is the wiring step that turns a flow definition into
//! an executable graph.

pub mod cache;
pub mod eval;
pub mod process;
pub mod register;

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	sync::Arc,
	time::Duration,
};

use dashmap::DashMap;
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
	analyzer::{FlowDependencyGraph, FlowGraphAnalyzer, FlowSchedule},
	flow::FlowDag,
};
use reifydb_runtime::context::{RuntimeContext, clock::Clock};
#[cfg(reifydb_target = "native")]
use reifydb_sdk::config::Config;
#[cfg(reifydb_target = "native")]
use reifydb_value::{Result, error::Error, value::Value};
use tracing::instrument;

#[cfg(reifydb_target = "native")]
use crate::operator::BoxedOperator;
#[cfg(reifydb_target = "native")]
use crate::operator::ffi::FFIOperator;
#[cfg(reifydb_target = "native")]
use crate::operator::native::native_operator_loader;
use crate::{
	builder::OperatorFactory,
	engine::cache::{ExecutionLevelCache, ScheduleCache},
	operator::OperatorCell,
};

pub struct FlowEngine {
	pub(crate) catalog: Catalog,
	pub(crate) executor: Executor,
	pub operators: BTreeMap<FlowNodeId, OperatorCell>,
	pub flows: BTreeMap<FlowId, Arc<FlowDag>>,
	pub sources: BTreeMap<ShapeId, Vec<(FlowId, FlowNodeId)>>,
	pub sinks: BTreeMap<ShapeId, Vec<(FlowId, FlowNodeId)>>,
	pub analyzer: FlowGraphAnalyzer,
	pub(crate) execution_level_cache: ExecutionLevelCache,
	pub(crate) schedule_cache: ScheduleCache,
	#[allow(dead_code)]
	pub(crate) event_bus: EventBus,
	pub(crate) flow_creation_versions: BTreeMap<FlowId, CommitVersion>,
	pub(crate) runtime_context: RuntimeContext,
	pub(crate) custom_operators: Arc<HashMap<String, OperatorFactory>>,
	operator_tick_times: DashMap<FlowNodeId, u64>,
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
			execution_level_cache: ExecutionLevelCache::new(),
			schedule_cache: ScheduleCache::new(),
			event_bus,
			flow_creation_versions: BTreeMap::new(),
			runtime_context,
			custom_operators,
			operator_tick_times: DashMap::new(),
		}
	}

	pub fn clock(&self) -> &Clock {
		&self.runtime_context.clock
	}

	fn operator_due(&self, node_id: FlowNodeId, now_nanos: u64, interval: Duration) -> bool {
		let interval_nanos = interval.as_nanos() as u64;
		let due = match self.operator_tick_times.get(&node_id) {
			Some(last) => now_nanos.saturating_sub(*last) >= interval_nanos,
			None => true,
		};
		if due {
			self.operator_tick_times.insert(node_id, now_nanos);
		}
		due
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
		let mut loader_write = loader.write();

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
		let loader_read = loader.read();
		loader_read.has_operator(operator)
	}

	#[cfg(reifydb_target = "native")]
	#[instrument(name = "flow::engine::create_native_operator", level = "debug", skip(self, config), fields(operator = %operator, node_id = ?node_id))]
	pub(crate) fn create_native_operator(
		&self,
		operator: &str,
		node_id: FlowNodeId,
		config: &Config,
	) -> Result<BoxedOperator> {
		let loader = native_operator_loader();
		let mut loader_write = loader.write();
		loader_write.create_operator_by_name(operator, node_id, config)
	}

	#[cfg(reifydb_target = "native")]
	pub(crate) fn is_native_operator(&self, operator: &str) -> bool {
		native_operator_loader().read().has_operator(operator)
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
		self.execution_level_cache.invalidate();
		self.schedule_cache.invalidate();
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
		self.execution_level_cache.invalidate();
		self.schedule_cache.invalidate();
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
		if let Some(levels) = self.execution_level_cache.get() {
			return levels;
		}

		let dependency_graph = self.analyzer.get_dependency_graph();
		let levels = self.analyzer.calculate_execution_levels(dependency_graph);
		self.execution_level_cache.set(levels.clone());
		levels
	}

	pub fn calculate_schedule(&self) -> FlowSchedule {
		if let Some(schedule) = self.schedule_cache.get() {
			return schedule;
		}

		let dependency_graph = self.analyzer.get_dependency_graph();
		let schedule = self.analyzer.calculate_schedule(dependency_graph);
		self.schedule_cache.set(schedule.clone());
		schedule
	}
}
