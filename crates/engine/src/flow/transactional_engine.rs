// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_core::interface::{
	WithEventBus,
	version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_rql::flow::loader::load_flow_dag;
use reifydb_runtime::{
	actor::system::ActorSpawner,
	context::RuntimeContext,
	shutdown::Shutdown,
};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_transaction::{interceptor::interceptors::Interceptors, transaction::Transaction};
use reifydb_value::{Result, value::identity::IdentityId};

use crate::{
	engine::StandardEngine,
	flow::{
		builder::{CustomOperators, FlowConfig, FlowConfigurator},
		engine::FlowEngine,
		lineage::FlowLineageTracker,
		transaction::allocators::FlowAllocators,
		transactional::{
			interceptor::{TransactionalFlowPostCommitInterceptor, TransactionalFlowPreCommitInterceptor},
			registry::TransactionalFlowRegistry,
		},
	},
};

pub struct TransactionalFlowEngine {
	flow_engine: FlowEngine,
	lineage: FlowLineageTracker,
	scope: ActorSpawner,
	running: AtomicBool,
}

impl TransactionalFlowEngine {
	pub fn with_defaults(engine: StandardEngine) -> Result<Self> {
		Self::new(FlowConfigurator::new().configure(), engine)
	}

	pub fn new(config: FlowConfig, engine: StandardEngine) -> Result<Self> {
		let clock = engine.clock().clone();
		let catalog = engine.catalog();

		let flow_engine = FlowEngine::new(
			catalog.clone(),
			engine.executor(),
			engine.event_bus().clone(),
			RuntimeContext::with_clock(clock),
			CustomOperators::new(config.custom_operators),
			FlowAllocators::with_dictionary(engine.dictionary_allocators()),
		);

		let lineage = FlowLineageTracker::new(engine.view_lineage());

		let registry = TransactionalFlowRegistry {
			flow_engine: flow_engine.clone(),
			engine: engine.clone(),
			catalog: catalog.clone(),
			lineage: lineage.clone(),
		};

		register_interceptors(&engine, &registry);
		bootstrap_flows(&engine, &registry)?;

		let scope = engine.spawner().scope();

		Ok(Self {
			flow_engine,
			lineage,
			scope,
			running: AtomicBool::new(true),
		})
	}
}

fn register_interceptors(engine: &StandardEngine, registry: &TransactionalFlowRegistry) {
	let flow_engine = registry.flow_engine.clone();
	let target = registry.engine.clone();
	let catalog = registry.catalog.clone();
	let lineage = registry.lineage.clone();

	engine.add_interceptor_factory(Arc::new(move |interceptors: &mut Interceptors| {
		interceptors.pre_commit.add(Arc::new(TransactionalFlowPreCommitInterceptor {
			flow_engine: flow_engine.clone(),
			engine: target.clone(),
			catalog: catalog.clone(),
		}));
		interceptors.post_commit.add(Arc::new(TransactionalFlowPostCommitInterceptor {
			registrar: TransactionalFlowRegistry {
				flow_engine: flow_engine.clone(),
				engine: target.clone(),
				catalog: catalog.clone(),
				lineage: lineage.clone(),
			},
		}));
	}));
}

fn bootstrap_flows(engine: &StandardEngine, registry: &TransactionalFlowRegistry) -> Result<()> {
	let mut query = engine.begin_query(IdentityId::system())?;
	let flows = engine.catalog().list_flows_all(&mut Transaction::Query(&mut query))?;

	for flow in flows {
		let dag = load_flow_dag(&mut Transaction::Query(&mut query), flow.id)?;
		registry.try_register(dag, &mut query)?;
	}

	Ok(())
}

impl Shutdown for TransactionalFlowEngine {
	fn shutdown(&self) {
		if self.running.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
			return;
		}

		self.scope.shutdown();

		self.flow_engine.write().clear();
		self.lineage.clear();
	}
}

impl Subsystem for TransactionalFlowEngine {
	fn name(&self) -> &'static str {
		"transactional-flow"
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::Acquire)
	}

	fn health_status(&self) -> HealthStatus {
		if self.is_running() {
			HealthStatus::Healthy
		} else {
			HealthStatus::Unknown
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}
}

impl HasVersion for TransactionalFlowEngine {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "transactional-flow".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Inline transactional flow execution subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

impl Drop for TransactionalFlowEngine {
	fn drop(&mut self) {
		self.shutdown();
	}
}
