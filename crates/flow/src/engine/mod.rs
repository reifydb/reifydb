// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Flow execution engine: registers compiled flow definitions and evaluates each flow's operator
//! graph against incoming change deltas, writing the outputs back through the catalog.

mod dispatch;
pub mod frontier;
mod lifecycle;
mod process;
pub mod register;
mod tick;
mod timers;

use std::{
	collections::{BTreeMap, BTreeSet},
	sync::Arc,
};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	event::EventBus,
	interface::catalog::{
		flow::{FlowId, OperatorId},
		id::{TableId, ViewId},
		object::ObjectId,
	},
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::flow::{
	analyzer::{FlowDependencyGraph, FlowGraphAnalyzer},
	flow::FlowDag,
};
use reifydb_runtime::context::{RuntimeContext, clock::Clock};
use tracing::instrument;

use crate::{
	operator::{
		BoxedHostOperator, HostOperator, metrics::OperatorSampleRegistry, provider::OperatorProvider,
		sink::BoxedDurableSink,
	},
	timer::registry::TimerRegistry,
	transaction::substrate::FlowSubstrate,
};

pub const COMPLETENESS_OBJECT: ObjectId = ObjectId::Table(TableId::SOURCE_COMPLETENESS);

pub struct FlowEngineInner {
	pub(crate) catalog: Catalog,
	pub(crate) routines: Routines,
	pub(crate) operators: BTreeMap<OperatorId, BoxedHostOperator>,
	pub(crate) durable_sinks: BTreeMap<OperatorId, BoxedDurableSink>,
	pub(crate) flows: BTreeMap<FlowId, FlowDag>,
	pub(crate) sources: BTreeMap<ObjectId, Vec<(FlowId, OperatorId)>>,
	pub(crate) sinks: BTreeMap<ObjectId, Vec<(FlowId, OperatorId)>>,
	pub(crate) analyzer: FlowGraphAnalyzer,
	#[allow(dead_code)]
	pub(crate) event_bus: EventBus,
	pub(crate) runtime_context: RuntimeContext,
	pub(crate) operator_provider: Arc<dyn OperatorProvider>,
	pub(crate) substrate: FlowSubstrate,
	pub(crate) operator_samples: OperatorSampleRegistry,
	pub(crate) timers: TimerRegistry,
}

impl FlowEngineInner {
	#[instrument(
		name = "flow::engine::new",
		level = "debug",
		skip(catalog, routines, event_bus, runtime_context, operator_provider, substrate, operator_samples)
	)]
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		catalog: Catalog,
		routines: Routines,
		event_bus: EventBus,
		runtime_context: RuntimeContext,
		operator_provider: Arc<dyn OperatorProvider>,
		substrate: FlowSubstrate,
		operator_samples: OperatorSampleRegistry,
	) -> Self {
		Self {
			catalog,
			routines,
			operators: BTreeMap::new(),
			durable_sinks: BTreeMap::new(),
			flows: BTreeMap::new(),
			sources: BTreeMap::new(),
			sinks: BTreeMap::new(),
			analyzer: FlowGraphAnalyzer::new(),
			event_bus,
			runtime_context,
			operator_provider,
			substrate,
			operator_samples,
			timers: TimerRegistry::default(),
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

	pub fn substrate(&self) -> &FlowSubstrate {
		&self.substrate
	}

	pub fn operator(&self, operator_id: OperatorId) -> Option<&dyn HostOperator> {
		self.operators.get(&operator_id).map(|operator| &**operator)
	}

	pub fn insert_operator(&mut self, operator_id: OperatorId, operator: BoxedHostOperator) {
		self.operators.insert(operator_id, operator);
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

	pub fn flow_ids(&self) -> BTreeSet<FlowId> {
		self.flows.keys().copied().collect()
	}

	pub fn get_dependency_graph(&self) -> FlowDependencyGraph {
		self.analyzer.get_dependency_graph().clone()
	}

	pub fn get_flow_producing_view(&self, view_id: ViewId) -> Option<FlowId> {
		let dependency_graph = self.analyzer.get_dependency_graph();
		self.analyzer.get_flow_producing_view(dependency_graph, view_id)
	}
}
