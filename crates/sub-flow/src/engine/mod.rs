// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Flow execution engine. Registers compiled flow definitions, evaluates each flow's operator graph against
//! incoming change deltas, and writes the resulting outputs back through the catalog. Process drives the per-tick
//! work; eval is where individual operators run; register is the wiring step that turns a flow definition into
//! an executable graph.

pub mod cache;
pub mod eval;
pub mod register;

use std::{
	collections::{BTreeMap, BTreeSet},
	sync::Arc,
};

use dashmap::DashMap;
use reifydb_catalog::catalog::Catalog;
#[cfg(reifydb_target = "native")]
use reifydb_codec::{key::encoded::EncodedKey, value::encode_params};
use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::catalog::{
		config::ConfigKey,
		flow::{FlowId, FlowNodeId},
		id::{TableId, ViewId},
		object::ObjectId,
	},
	lifecycle::metrics::RetentionMetrics,
	metrics::heap::{OperatorSample, StateMemory},
	state::budget::{LeaseReport, OperatorStateBudgetHandle},
};
use reifydb_engine::vm::executor::Executor;
#[cfg(reifydb_target = "native")]
use reifydb_extension::operator::ffi_loader::ffi_operator_loader;
#[cfg(reifydb_target = "native")]
use reifydb_flow::operator::BoxedOperator;
use reifydb_flow::transaction::substrate::FlowSubstrate;
use reifydb_rql::flow::{
	analyzer::{FlowDependencyGraph, FlowGraphAnalyzer, FlowSchedule},
	flow::FlowDag,
};
use reifydb_runtime::{
	context::{RuntimeContext, clock::Clock},
	sync::rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};
#[cfg(reifydb_target = "native")]
use reifydb_sdk::config::Config;
#[cfg(reifydb_target = "native")]
use reifydb_value::{Result, error::Error, params::Params};
use reifydb_value::{
	byte_size::ByteSize,
	value::Value,
};
use tracing::{debug, instrument};

#[cfg(reifydb_target = "native")]
use crate::error::{FlowStateError, NativeOperatorError};
#[cfg(reifydb_target = "native")]
use crate::operator::ffi::FFIOperator;
#[cfg(reifydb_target = "native")]
use crate::operator::native::native_operator_loader;
use crate::{
	builder::CustomOperators,
	engine::cache::{ExecutionLevelCache, ScheduleCache},
	operator::{OperatorCell, metrics::OperatorSampleRegistry},
};

pub struct FlowEngineInner {
	pub(crate) catalog: Catalog,
	pub(crate) executor: Executor,
	pub(crate) operators: BTreeMap<FlowNodeId, OperatorCell>,
	pub(crate) flows: BTreeMap<FlowId, FlowDag>,
	pub(crate) sources: BTreeMap<ObjectId, Vec<(FlowId, FlowNodeId)>>,
	pub(crate) sinks: BTreeMap<ObjectId, Vec<(FlowId, FlowNodeId)>>,
	pub(crate) analyzer: FlowGraphAnalyzer,
	pub(crate) execution_level_cache: ExecutionLevelCache,
	pub(crate) schedule_cache: ScheduleCache,
	#[allow(dead_code)]
	pub(crate) event_bus: EventBus,
	pub(crate) flow_creation_versions: BTreeMap<FlowId, CommitVersion>,
	pub(crate) runtime_context: RuntimeContext,
	pub(crate) custom_operators: CustomOperators,
	pub(crate) mapping_cursors: DashMap<FlowNodeId, Option<EncodedKey>>,
	pub(crate) substrate: FlowSubstrate,
	pub(crate) operator_samples: OperatorSampleRegistry,
	pub(crate) state_budget: OperatorStateBudgetHandle,
	pub(crate) retention_metrics: RetentionMetrics,
}

#[derive(Clone)]
pub struct FlowEngine {
	inner: Arc<RwLock<FlowEngineInner>>,
}

impl FlowEngine {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		catalog: Catalog,
		executor: Executor,
		event_bus: EventBus,
		runtime_context: RuntimeContext,
		custom_operators: CustomOperators,
		substrate: FlowSubstrate,
		operator_samples: OperatorSampleRegistry,
		state_budget: OperatorStateBudgetHandle,
	) -> Self {
		Self {
			inner: Arc::new(RwLock::new(FlowEngineInner::new(
				catalog,
				executor,
				event_bus,
				runtime_context,
				custom_operators,
				substrate,
				operator_samples,
				state_budget,
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
		skip(
			catalog,
			executor,
			event_bus,
			runtime_context,
			custom_operators,
			substrate,
			operator_samples,
			state_budget
		)
	)]
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		catalog: Catalog,
		executor: Executor,
		event_bus: EventBus,
		runtime_context: RuntimeContext,
		custom_operators: CustomOperators,
		substrate: FlowSubstrate,
		operator_samples: OperatorSampleRegistry,
		state_budget: OperatorStateBudgetHandle,
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
			mapping_cursors: DashMap::new(),
			substrate,
			operator_samples,
			state_budget,
			retention_metrics: RetentionMetrics::new(),
		}
	}

	pub fn adopt_retention_metrics(&mut self, metrics: RetentionMetrics) {
		self.retention_metrics = metrics;
	}

	#[instrument(name = "flow::engine::sample", level = "debug", skip_all)]
	pub fn sample_operators(&self) {
		for (node, operator) in &self.operators {
			let sample = operator.sample();
			if self.state_budget.current_lease(*node).is_some() {
				match &sample {
					Some(sample) => {
						self.state_budget.report_lease(*node, lease_report_from_sample(sample));
					}
					None => {
						self.state_budget.report_lease_none(*node);
						debug!(node = node.0, "leased operator reported no state usage");
					}
				}
			}
			if let Some(sample) = sample {
				self.operator_samples.record(*node, sample);
			}
		}
	}

	pub fn forget_operator_samples(&self) {
		for node in self.operators.keys() {
			self.operator_samples.forget(*node);
		}
	}

	pub fn state_budget(&self) -> OperatorStateBudgetHandle {
		self.state_budget.clone()
	}

	pub fn clock(&self) -> &Clock {
		&self.runtime_context.clock
	}

	pub fn operator(&self, node_id: FlowNodeId) -> Option<OperatorCell> {
		self.operators.get(&node_id).cloned()
	}

	pub fn insert_operator(&mut self, node_id: FlowNodeId, operator: OperatorCell) {
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

	pub fn flows_for_source_object(&self, object: ObjectId) -> Option<Vec<(FlowId, FlowNodeId)>> {
		self.sources.get(&object).cloned()
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

		let config_params =
			Params::Named(Arc::new(config.iter().map(|(k, v)| (k.clone(), v.clone())).collect()));
		let config_bytes = encode_params(&config_params).map_err(|e| {
			Error::from(FlowStateError::Encode {
				state: "operator config",
				cause: e.to_string(),
			})
		})?;

		let lease = self.state_budget.grant_lease(node_id, state_lease_default());
		let created = loader_write.create_operator_by_name(operator, node_id, &config_bytes);
		let (descriptor, instance) = match created {
			Ok(created) => created,
			Err(e) => {
				self.state_budget.release_lease(node_id);
				return Err(Error::from(NativeOperatorError::CreateFailed {
					cause: format!("{:?}", e),
				}));
			}
		};

		Ok(Box::new(FFIOperator::new(
			descriptor,
			instance,
			node_id,
			self.executor.clone(),
			self.state_budget.clone(),
			lease,
		)))
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
		let _lease = self.state_budget.grant_lease(node_id, state_lease_default());
		match loader_write.create_operator_by_name(operator, node_id, config) {
			Ok(op) => Ok(op),
			Err(e) => {
				self.state_budget.release_lease(node_id);
				Err(e)
			}
		}
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
		for node_id in self.operators.keys() {
			self.state_budget.release_lease(*node_id);
		}
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
			self.substrate.row.evict(node_id);
			self.state_budget.release_lease(node_id);
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

pub(crate) fn state_lease_default() -> ByteSize {
	match ConfigKey::OperatorStateLeaseDefault.default_value() {
		Value::Uint8(bytes) => ByteSize::from_bytes(bytes),
		other => panic!("OPERATOR_STATE_LEASE_DEFAULT default must be Uint8 bytes, got {:?}", other),
	}
}

pub(crate) fn lease_report_from_sample(sample: &OperatorSample) -> LeaseReport {
	LeaseReport {
		state: sample.memory.unwrap_or(StateMemory::ZERO),
		row_numbers: sample.row_number_cache.unwrap_or(StateMemory::ZERO),
	}
}

#[cfg(reifydb_target = "native")]
pub(crate) fn lease_demand(report: &LeaseReport) -> ByteSize {
	let reported = report.total_bytes().as_bytes();
	ByteSize::from_bytes(reported.saturating_add(reported / 4))
}

#[cfg(test)]
mod tests {
	use reifydb_value::{byte_size::ByteSize, count::Count};

	use super::*;

	fn memory(entries: u64, bytes: u64) -> StateMemory {
		StateMemory::new(Count::new(entries), ByteSize::from_bytes(bytes))
	}

	#[test]
	fn lease_charge_does_not_add_the_dirty_subset_on_top_of_the_total() {
		// Producers fill both fields from one cache: memory comes from
		// approximate_memory (clean + dirty) and dirty_memory is the dirty
		// subset of that same number. Adding them charged clean + 2 * dirty,
		// which inflates leased_bytes and can trip a spurious overage exactly
		// when a batch of pending writes is largest.
		let sample = OperatorSample::with_memory(memory(10, 4096)).with_dirty_memory(memory(4, 1024));

		let report = lease_report_from_sample(&sample);

		assert_eq!(report.state, memory(10, 4096), "the lease charges the reported total, once");
	}

	#[test]
	fn lease_charge_falls_back_to_zero_when_an_operator_reports_no_memory() {
		let report = lease_report_from_sample(&OperatorSample::default());

		assert_eq!(report.state, StateMemory::ZERO);
		assert_eq!(report.row_numbers, StateMemory::ZERO);
	}

	#[test]
	fn lease_charge_keeps_row_number_cache_separate_from_state() {
		// The two are distinct budget lines; folding one into the other would
		// hide which of them is actually growing.
		let sample = OperatorSample::with_memory(memory(10, 4096)).with_row_number_cache(memory(2, 512));

		let report = lease_report_from_sample(&sample);

		assert_eq!(report.state, memory(10, 4096));
		assert_eq!(report.row_numbers, memory(2, 512));
	}

	#[test]
	fn lease_demand_adds_a_quarter_headroom_over_reported_usage() {
		// Decision D1: the grant tracks demand with 25% headroom so a
		// steadily growing operator is not clamped by its own lease and
		// forced into a resize on every single sampling tick.
		let report = lease_report_from_sample(&OperatorSample::with_memory(memory(10, 4096)));

		assert_eq!(lease_demand(&report), ByteSize::from_bytes(5120));
	}

	#[test]
	fn lease_demand_counts_row_numbers_alongside_state() {
		// Both budget lines are guest memory; sizing the grant from
		// state alone would under-lease exactly the operators with
		// large row-number caches.
		let report = lease_report_from_sample(
			&OperatorSample::with_memory(memory(10, 4096)).with_row_number_cache(memory(2, 4096)),
		);

		assert_eq!(lease_demand(&report), ByteSize::from_bytes(10240));
	}
}
