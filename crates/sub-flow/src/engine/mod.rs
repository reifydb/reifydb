// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Flow execution engine: registers compiled flow definitions and evaluates each flow's operator
//! graph against incoming change deltas, writing the outputs back through the catalog.

pub mod cache;
pub mod eval;
pub mod register;

use std::{
	collections::{BTreeMap, BTreeSet},
	sync::Arc,
};

use dashmap::DashMap;
use reifydb_catalog::catalog::Catalog;
use reifydb_codec::key::encoded::EncodedKey;
#[cfg(reifydb_target = "native")]
use reifydb_codec::value::encode_params;
use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::catalog::{
		config::{ConfigKey, GetConfig},
		flow::{FlowId, OperatorId},
		id::{TableId, ViewId},
		object::ObjectId,
	},
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
use reifydb_value::{Result, error::Error, params::Params, value::Value};
use reifydb_value::{byte_size::ByteSize, value::datetime::DateTime};
use tracing::{debug, instrument};

#[cfg(reifydb_target = "native")]
use crate::error::{FlowStateError, NativeOperatorError};
#[cfg(reifydb_target = "native")]
use crate::operator::ffi::FFIOperatorHandle;
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
	pub(crate) operators: BTreeMap<OperatorId, OperatorCell>,
	pub(crate) flows: BTreeMap<FlowId, FlowDag>,
	pub(crate) sources: BTreeMap<ObjectId, Vec<(FlowId, OperatorId)>>,
	pub(crate) sinks: BTreeMap<ObjectId, Vec<(FlowId, OperatorId)>>,
	pub(crate) analyzer: FlowGraphAnalyzer,
	pub(crate) execution_level_cache: ExecutionLevelCache,
	pub(crate) schedule_cache: ScheduleCache,
	#[allow(dead_code)]
	pub(crate) event_bus: EventBus,
	pub(crate) flow_creation_versions: BTreeMap<FlowId, CommitVersion>,
	pub(crate) runtime_context: RuntimeContext,
	pub(crate) custom_operators: CustomOperators,
	pub(crate) mapping_cursors: DashMap<OperatorId, Option<EncodedKey>>,

	pub(crate) compacted_at: DashMap<OperatorId, DateTime>,
	pub(crate) substrate: FlowSubstrate,
	pub(crate) operator_samples: OperatorSampleRegistry,
	pub(crate) state_budget: OperatorStateBudgetHandle,
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
			compacted_at: DashMap::new(),
			substrate,
			operator_samples,
			state_budget,
		}
	}

	#[instrument(name = "flow::engine::sample", level = "debug", skip_all)]
	pub fn sample_operators(&self) {
		for (operator_id, operator) in &self.operators {
			let sample = operator.sample();
			if self.state_budget.current_lease(*operator_id).is_some() {
				match &sample {
					Some(sample) => {
						self.state_budget
							.report_lease(*operator_id, lease_report_from_sample(sample));
					}
					None => {
						self.state_budget.report_lease_none(*operator_id);
						debug!(
							operator = operator_id.0,
							"leased operator reported no state usage"
						);
					}
				}
			}
			if let Some(sample) = sample {
				self.operator_samples.record(*operator_id, sample);
			}
		}
	}

	pub fn forget_operator_samples(&self) {
		for operator in self.operators.keys() {
			self.operator_samples.forget(*operator);
		}
	}

	pub fn state_budget(&self) -> OperatorStateBudgetHandle {
		self.state_budget.clone()
	}

	pub fn clock(&self) -> &Clock {
		&self.runtime_context.clock
	}

	pub fn operator(&self, operator_id: OperatorId) -> Option<OperatorCell> {
		self.operators.get(&operator_id).cloned()
	}

	pub fn insert_operator(&mut self, operator_id: OperatorId, operator: OperatorCell) {
		self.operators.insert(operator_id, operator);
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

	pub fn flows_for_source_object(&self, object: ObjectId) -> Option<Vec<(FlowId, OperatorId)>> {
		self.sources.get(&object).cloned()
	}

	#[cfg(reifydb_target = "native")]
	#[instrument(name = "flow::engine::create_ffi_operator", level = "debug", skip(self, config), fields(operator = %operator, operator_id = ?operator_id))]
	pub(crate) fn create_ffi_operator(
		&self,
		operator: &str,
		operator_id: OperatorId,
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

		let lease = self.state_budget.grant_lease(operator_id, self.state_lease_default());
		let created = loader_write.create_operator_by_name(operator, operator_id, &config_bytes);
		let (descriptor, instance) = match created {
			Ok(created) => created,
			Err(e) => {
				self.state_budget.release_lease(operator_id);
				return Err(Error::from(NativeOperatorError::CreateFailed {
					cause: format!("{:?}", e),
				}));
			}
		};

		Ok(Box::new(FFIOperatorHandle::new(
			descriptor,
			instance,
			operator_id,
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
	#[instrument(name = "flow::engine::create_native_operator", level = "debug", skip(self, config), fields(operator = %operator, operator_id = ?operator_id))]
	pub(crate) fn create_native_operator(
		&self,
		operator: &str,
		operator_id: OperatorId,
		config: &Config,
	) -> Result<BoxedOperator> {
		let loader = native_operator_loader();
		let mut loader_write = loader.write();
		let _lease = self.state_budget.grant_lease(operator_id, self.state_lease_default());
		match loader_write.create_operator_by_name(operator, operator_id, config) {
			Ok(op) => Ok(op),
			Err(e) => {
				self.state_budget.release_lease(operator_id);
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
		for operator_id in self.operators.keys() {
			self.state_budget.release_lease(*operator_id);
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
		let node_ids: Vec<OperatorId> =
			self.flows.get(&flow_id).map(|flow| flow.get_operator_ids().collect()).unwrap_or_default();

		for operator_id in node_ids {
			self.operators.remove(&operator_id);
			self.substrate.row.evict(operator_id);
			self.substrate.operators.drop_arena(operator_id);
			self.state_budget.release_lease(operator_id);
			self.executor.services().node_retention_store.remove(operator_id);
			self.mapping_cursors.remove(&operator_id);
			self.compacted_at.remove(&operator_id);
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

	pub(crate) fn state_lease_default(&self) -> ByteSize {
		ByteSize::from_bytes(self.catalog.get_config_uint8(ConfigKey::OperatorStateLeaseDefault))
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
	use std::collections::HashMap;

	use reifydb_codec::encoded::row::EncodedRow;
	use reifydb_core::{
		common::TimeDomain,
		interface::{
			WithEventBus,
			catalog::{flow::FlowId, id::SeriesId},
		},
	};
	use reifydb_rql::flow::{
		flow::FlowDag,
		operator::{FlowNode, OperatorDef},
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{byte_size::ByteSize, count::Count, util::cowvec::CowVec, value::Value};

	use super::*;
	use crate::operator::scan::series::SourceSeriesOperator;

	fn memory(entries: u64, bytes: u64) -> StateMemory {
		StateMemory::new(Count::new(entries), ByteSize::from_bytes(bytes))
	}

	#[test]
	fn lease_charge_does_not_add_the_dirty_subset_on_top_of_the_total() {
		// dirty_memory is a subset of memory, not an addition to it, so summing the two charges
		// clean + 2 * dirty and trips a spurious overage exactly when pending writes peak.
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
		// Distinct budget lines: folding one into the other hides which is actually growing.
		let sample = OperatorSample::with_memory(memory(10, 4096)).with_row_number_cache(memory(2, 512));

		let report = lease_report_from_sample(&sample);

		assert_eq!(report.state, memory(10, 4096));
		assert_eq!(report.row_numbers, memory(2, 512));
	}

	#[test]
	fn lease_demand_adds_a_quarter_headroom_over_reported_usage() {
		// Headroom keeps a steadily growing operator from being clamped by its own lease and
		// forced into a resize on every sampling tick.
		let report = lease_report_from_sample(&OperatorSample::with_memory(memory(10, 4096)));

		assert_eq!(lease_demand(&report), ByteSize::from_bytes(5120));
	}

	#[test]
	fn lease_demand_counts_row_numbers_alongside_state() {
		// Both lines are guest memory, so sizing the grant from state alone under-leases exactly
		// the operators with large row-number caches.
		let report = lease_report_from_sample(
			&OperatorSample::with_memory(memory(10, 4096)).with_row_number_cache(memory(2, 4096)),
		);

		assert_eq!(lease_demand(&report), ByteSize::from_bytes(10240));
	}

	#[test]
	fn state_lease_default_reads_the_configured_value_not_the_compiled_in_default() {
		// OPERATOR_STATE_LEASE_DEFAULT is declared live-reconfigurable (requires_restart() is
		// false), so every lease grant has to resolve it through the engine's catalog. Resolving
		// ConfigKey::default_value() instead pins every operator at the compiled-in 64 MiB and no
		// configured value can ever be observed.
		let compiled_in = match ConfigKey::OperatorStateLeaseDefault.default_value() {
			Value::Uint8(bytes) => bytes,
			other => panic!("OPERATOR_STATE_LEASE_DEFAULT default must be Uint8 bytes, got {other:?}"),
		};
		let configured = compiled_in / 4;
		assert_ne!(configured, compiled_in, "the fixture must differ from the default or nothing is proven");

		let engine = TestEngine::new();
		engine.catalog()
			.cache()
			.set_config(ConfigKey::OperatorStateLeaseDefault, CommitVersion(1), Value::Uint8(configured))
			.expect("a positive lease default must be accepted");

		let inner = FlowEngineInner::new(
			engine.catalog(),
			engine.executor(),
			engine.event_bus().clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			CustomOperators::new(HashMap::new()),
			FlowSubstrate::default(),
			OperatorSampleRegistry::new(),
			OperatorStateBudgetHandle::default(),
		);

		assert_eq!(inner.state_lease_default(), ByteSize::from_bytes(configured));
	}

	#[test]
	fn state_lease_default_falls_back_to_the_compiled_in_default_when_unconfigured() {
		// The catalog read must not change the out-of-the-box grant size: an unconfigured engine
		// still has to lease the declared default rather than zero or the lease floor.
		let compiled_in = match ConfigKey::OperatorStateLeaseDefault.default_value() {
			Value::Uint8(bytes) => bytes,
			other => panic!("OPERATOR_STATE_LEASE_DEFAULT default must be Uint8 bytes, got {other:?}"),
		};

		let engine = TestEngine::new();
		let inner = FlowEngineInner::new(
			engine.catalog(),
			engine.executor(),
			engine.event_bus().clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			CustomOperators::new(HashMap::new()),
			FlowSubstrate::default(),
			OperatorSampleRegistry::new(),
			OperatorStateBudgetHandle::default(),
		);

		assert_eq!(inner.state_lease_default(), ByteSize::from_bytes(compiled_in));
	}

	#[test]
	fn removing_a_flow_drops_its_operators_arenas() {
		// remove_flow is the only arena teardown a retired flow gets: without drop_arena its
		// operators' state stays resident (and counted in total_bytes) until the process restarts.
		// Mutation falsified against: removing the drop_arena call from remove_flow (per-operator
		// bytes and the process-wide total both stay non-zero).
		let engine = TestEngine::new();
		let mut inner = FlowEngineInner::new(
			engine.catalog(),
			engine.executor(),
			engine.event_bus().clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			CustomOperators::new(HashMap::new()),
			FlowSubstrate {
				operators: engine.inner().operator_state(),
				..FlowSubstrate::default()
			},
			OperatorSampleRegistry::new(),
			OperatorStateBudgetHandle::default(),
		);

		let operator = OperatorId(7);
		let mut builder = FlowDag::builder(FlowId(1));
		builder.add_node(FlowNode::new(
			operator,
			OperatorDef::SourceSeries {
				series: SeriesId(1),
				time_domain: TimeDomain::None,
			},
		));
		inner.register_flow_dag(builder.build());
		inner.insert_operator(operator, OperatorCell::new(SourceSeriesOperator::new(operator)));

		let store = inner.substrate.operators.clone();
		store.set(operator, EncodedKey::new(b"k"), EncodedRow(CowVec::new(vec![1u8; 64])));
		assert!(store.bytes(operator) > 0, "precondition: the operator's arena holds state");

		inner.remove_flow(FlowId(1));

		assert_eq!(store.bytes(operator), 0, "the retired operator's arena must be dropped");
		assert_eq!(store.total_bytes(), 0, "and its bytes must leave the process-wide accounting");
	}
}
