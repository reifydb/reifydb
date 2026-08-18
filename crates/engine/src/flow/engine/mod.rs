// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Flow execution engine. Registers compiled flow definitions, evaluates each flow's operator graph against
//! incoming change deltas, and writes the resulting outputs back through the catalog. Process drives the per-tick
//! work; eval is where individual operators run; register is the wiring step that turns a flow definition into
//! an executable graph.

pub mod cache;
pub mod eval;
pub mod register;
pub mod schedule;

use std::{
	collections::{BTreeMap, BTreeSet},
	sync::Arc,
};

use dashmap::DashMap;
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::catalog::{
		flow::{FlowId, OperatorId},
		id::{TableId, ViewId},
		object::ObjectId,
	},
};
use crate::vm::executor::Executor;
#[cfg(reifydb_target = "host")]
use reifydb_rql::flow::{
	analyzer::{FlowDependencyGraph, FlowGraphAnalyzer},
	flow::FlowDag,
};
use reifydb_runtime::{
	context::{RuntimeContext, clock::Clock},
	sync::rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};
use reifydb_value::value::duration::Duration;
use tracing::instrument;

#[cfg(reifydb_target = "host")]
#[cfg(reifydb_target = "host")]
use crate::flow::{
	builder::CustomOperators,
	engine::{cache::ScheduleCache, schedule::{FlowSchedule, calculate_schedule}},
	operator::OperatorCell,
	transaction::allocators::FlowAllocators,
};

pub struct FlowEngineInner {
	pub(crate) catalog: Catalog,
	pub(crate) executor: Executor,
	pub(crate) operators: BTreeMap<OperatorId, OperatorCell>,
	pub(crate) flows: BTreeMap<FlowId, FlowDag>,
	pub(crate) sources: BTreeMap<ObjectId, Vec<(FlowId, OperatorId)>>,
	pub(crate) sinks: BTreeMap<ObjectId, Vec<(FlowId, OperatorId)>>,
	pub(crate) analyzer: FlowGraphAnalyzer,
	pub(crate) schedule_cache: ScheduleCache,
	#[allow(dead_code)]
	pub(crate) event_bus: EventBus,
	pub(crate) flow_creation_versions: BTreeMap<FlowId, CommitVersion>,
	pub(crate) runtime_context: RuntimeContext,
	pub(crate) custom_operators: CustomOperators,
	operator_tick_times: DashMap<OperatorId, u64>,
	pub(crate) allocators: FlowAllocators,
}

#[derive(Clone)]
pub struct FlowEngine {
	inner: Arc<RwLock<FlowEngineInner>>,
}

impl FlowEngine {
	pub fn new(
		catalog: Catalog,
		executor: Executor,
		event_bus: EventBus,
		runtime_context: RuntimeContext,
		custom_operators: CustomOperators,
		allocators: FlowAllocators,
	) -> Self {
		Self {
			inner: Arc::new(RwLock::new(FlowEngineInner::new(
				catalog,
				executor,
				event_bus,
				runtime_context,
				custom_operators,
				allocators,
			))),
		}
	}

	pub fn read(&self) -> RwLockReadGuard<'_, FlowEngineInner> {
		self.inner.read()
	}

	pub fn read_recursive(&self) -> RwLockReadGuard<'_, FlowEngineInner> {
		self.inner.read_recursive()
	}

	pub fn write(&self) -> RwLockWriteGuard<'_, FlowEngineInner> {
		self.inner.write()
	}
}

impl FlowEngineInner {
	#[instrument(
		name = "flow::engine::new",
		level = "debug",
		skip(catalog, executor, event_bus, runtime_context, custom_operators, allocators)
	)]
	pub fn new(
		catalog: Catalog,
		executor: Executor,
		event_bus: EventBus,
		runtime_context: RuntimeContext,
		custom_operators: CustomOperators,
		allocators: FlowAllocators,
	) -> Self {
		Self {
			catalog,
			executor,
			operators: BTreeMap::new(),
			flows: BTreeMap::new(),
			sources: BTreeMap::new(),
			sinks: BTreeMap::new(),
			analyzer: FlowGraphAnalyzer::new(),
			schedule_cache: ScheduleCache::new(),
			event_bus,
			flow_creation_versions: BTreeMap::new(),
			runtime_context,
			custom_operators,
			operator_tick_times: DashMap::new(),
			allocators,
		}
	}

	pub fn clock(&self) -> &Clock {
		&self.runtime_context.clock
	}

	pub fn operator(&self, node_id: OperatorId) -> Option<OperatorCell> {
		self.operators.get(&node_id).cloned()
	}

	pub fn insert_operator(&mut self, node_id: OperatorId, operator: OperatorCell) {
		self.operators.insert(node_id, operator);
	}

	pub fn register_flow_dag(&mut self, flow: FlowDag) {
		self.analyzer.add(flow.clone());
		self.flows.insert(flow.id, flow);
	}

	pub fn flow_by_id(&self, flow_id: FlowId) -> Option<FlowDag> {
		self.flows.get(&flow_id).cloned()
	}

	pub fn has_sources(&self) -> bool {
		!self.sources.is_empty()
	}

	pub fn flows_for_source_shape(&self, shape: ObjectId) -> Option<Vec<(FlowId, OperatorId)>> {
		self.sources.get(&shape).cloned()
	}

	pub(crate) fn operator_due(&self, node_id: OperatorId, now_nanos: u64, interval: Duration) -> bool {
		let interval_nanos = interval.to_std().as_nanos() as u64;
		let due = match self.operator_tick_times.get(&node_id) {
			Some(last) => now_nanos.saturating_sub(*last) >= interval_nanos,
			None => true,
		};
		if due {
			self.operator_tick_times.insert(node_id, now_nanos);
		}
		due
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
		self.schedule_cache.invalidate();
	}

	pub fn remove_flow(&mut self, flow_id: FlowId) {
		let node_ids: Vec<OperatorId> =
			self.flows.get(&flow_id).map(|flow| flow.get_operator_ids().collect()).unwrap_or_default();

		for node_id in node_ids {
			self.operators.remove(&node_id);
			self.allocators.row.evict(node_id);
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

	pub fn calculate_schedule(&self) -> FlowSchedule {
		if let Some(schedule) = self.schedule_cache.get() {
			return schedule;
		}

		let dependency_graph = self.analyzer.get_dependency_graph();
		let schedule = calculate_schedule(dependency_graph);
		self.schedule_cache.set(schedule.clone());
		schedule
	}
}
