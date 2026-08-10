// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Flow execution engine: registers compiled flow definitions and evaluates each flow's operator
//! graph against incoming change deltas, writing the outputs back through the catalog.

pub mod eval;
pub mod register;

use std::{
	collections::{BTreeMap, BTreeSet},
	sync::Arc,
};

use reifydb_catalog::catalog::Catalog;
#[cfg(reifydb_target = "native")]
use reifydb_codec::value::encode_params;
use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::catalog::{
		flow::{FlowId, OperatorId},
		id::{TableId, ViewId},
		object::ObjectId,
	},
};
use reifydb_engine::vm::executor::Executor;
#[cfg(reifydb_target = "native")]
use reifydb_extension::operator::ffi_loader::ffi_operator_loader;
#[cfg(reifydb_target = "native")]
use reifydb_flow::operator::BoxedOperator;
use reifydb_flow::transaction::substrate::FlowSubstrate;
use reifydb_rql::flow::{
	analyzer::{FlowDependencyGraph, FlowGraphAnalyzer},
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
use tracing::instrument;

#[cfg(reifydb_target = "native")]
use crate::error::{FlowStateError, NativeOperatorError};
#[cfg(reifydb_target = "native")]
use crate::operator::ffi::FFIOperatorHandle;
#[cfg(reifydb_target = "native")]
use crate::operator::native::native_operator_loader;
use crate::{
	builder::CustomOperators,
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
	#[allow(dead_code)]
	pub(crate) event_bus: EventBus,
	pub(crate) flow_creation_versions: BTreeMap<FlowId, CommitVersion>,
	pub(crate) runtime_context: RuntimeContext,
	pub(crate) custom_operators: CustomOperators,
	pub(crate) substrate: FlowSubstrate,
	pub(crate) operator_samples: OperatorSampleRegistry,
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
		skip(catalog, executor, event_bus, runtime_context, custom_operators, substrate, operator_samples)
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
			substrate,
			operator_samples,
		}
	}

	#[instrument(name = "flow::engine::sample", level = "debug", skip_all)]
	pub fn sample_operators(&self) {
		for (operator_id, operator) in &self.operators {
			if let Some(sample) = operator.sample() {
				self.operator_samples.record(*operator_id, sample);
			}
		}
	}

	pub fn forget_operator_samples(&self) {
		for operator in self.operators.keys() {
			self.operator_samples.forget(*operator);
		}
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

		let created = loader_write.create_operator_by_name(operator, operator_id, &config_bytes);
		let (descriptor, instance) = match created {
			Ok(created) => created,
			Err(e) => {
				return Err(Error::from(NativeOperatorError::CreateFailed {
					cause: format!("{:?}", e),
				}));
			}
		};

		Ok(Box::new(FFIOperatorHandle::new(descriptor, instance, operator_id, self.executor.clone())))
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
		loader_write.create_operator_by_name(operator, operator_id, config)
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
	}

	pub fn remove_flow(&mut self, flow_id: FlowId) {
		let node_ids: Vec<OperatorId> =
			self.flows.get(&flow_id).map(|flow| flow.get_operator_ids().collect()).unwrap_or_default();

		for operator_id in node_ids {
			self.operators.remove(&operator_id);
			self.substrate.row.evict(operator_id);
			self.substrate.operators.drop_arena(operator_id);
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

}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use reifydb_codec::{key::encoded::EncodedKey, row::operator::EncodedOperatorRow};
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

	use super::*;
	use crate::operator::scan::series::SourceSeriesOperator;

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
		store.set(operator, EncodedKey::new(b"k"), EncodedOperatorRow::timeless(&[1u8; 64]));
		assert!(store.bytes(operator) > 0, "precondition: the operator's arena holds state");

		inner.remove_flow(FlowId(1));

		assert_eq!(store.bytes(operator), 0, "the retired operator's arena must be dropped");
		assert_eq!(store.total_bytes(), 0, "and its bytes must leave the process-wide accounting");
	}
}
