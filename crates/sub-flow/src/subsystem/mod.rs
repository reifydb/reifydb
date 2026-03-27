// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod factory;
#[cfg(reifydb_target = "native")]
pub mod ffi;

use std::{
	any::Any,
	sync::{Arc, RwLock},
	time::Duration,
};

#[cfg(reifydb_target = "native")]
use ffi::load_ffi_operators;
use reifydb_cdc::{
	consume::{
		consumer::{CdcConsume, CdcConsumer},
		poll::{PollConsumer, PollConsumerConfig},
	},
	storage::CdcStore,
};
use reifydb_core::{
	interface::{
		WithEventBus,
		cdc::{Cdc, CdcConsumerId},
		flow::FlowLagsProvider,
		version::{ComponentType, HasVersion, SystemVersion},
	},
	key::{EncodableKey, cdc_consumer::CdcConsumerKey},
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_rql::flow::loader::load_flow_dag;
use reifydb_runtime::{SharedRuntime, actor::system::ActorHandle, context::RuntimeContext};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_transaction::{
	interceptor::interceptors::Interceptors,
	transaction::{TestTransaction, Transaction},
};
use reifydb_type::{Result, value::identity::IdentityId};
use tracing::{info, warn};

use crate::{
	builder::FlowBuilderConfig,
	catalog::FlowCatalog,
	deferred::{
		coordinator::{CoordinatorActor, CoordinatorMsg, FlowConsumeRef, extract_new_flow_ids},
		lag::FlowLags,
		pool::{PoolActor, PoolMsg},
		tracker::PrimitiveVersionTracker,
		worker::{FlowMsg, FlowWorkerActor},
	},
	engine::FlowEngine,
	transactional::{
		interceptor::{TransactionalFlowPostCommitInterceptor, TransactionalFlowPreCommitInterceptor},
		registrar::TransactionalFlowRegistrar,
	},
};

/// Thin wrapper around the deferred coordinator that intercepts new flows
/// and registers transactional ones before forwarding to the coordinator.
struct FlowConsumeDispatcher {
	coordinator: FlowConsumeRef,
	registrar: TransactionalFlowRegistrar,
	flow_catalog: FlowCatalog,
	engine: StandardEngine,
}

impl CdcConsume for FlowConsumeDispatcher {
	fn consume(&self, cdcs: Vec<Cdc>, reply: Box<dyn FnOnce(Result<()>) + Send>) {
		// Check for newly-created flows that might be transactional views.
		let new_flow_ids = extract_new_flow_ids(&cdcs);
		if !new_flow_ids.is_empty() {
			if let Ok(mut query) = self.engine.begin_query(IdentityId::system()) {
				for flow_id in new_flow_ids {
					match self
						.flow_catalog
						.get_or_load_flow(&mut Transaction::Query(&mut query), flow_id)
					{
						Ok((flow, true)) => {
							// Newly-loaded flow: try to register as transactional.
							// If transactional, FlowCatalog now caches it so the
							// coordinator's get_or_load_flow sees is_new=false.
							match self.registrar.try_register(flow) {
								Ok(true) => { /* transactional, leave cached */ }
								Ok(false) => {
									// NOT transactional — remove from cache so
									// the coordinator discovers it as new.
									self.flow_catalog.remove(flow_id);
								}
								Err(e) => {
									self.flow_catalog.remove(flow_id);
									warn!(
										flow_id = flow_id.0,
										error = %e,
										"failed to register transactional flow"
									);
								}
							}
						}
						Ok((_, false)) => {
							// Already cached — nothing to do.
						}
						Err(e) => {
							warn!(
								flow_id = flow_id.0,
								error = %e,
								"failed to load flow for transactional check"
							);
						}
					}
				}
			}
		}

		// Forward CDC batch to the deferred coordinator.
		// Transactional flows will have is_new=false in the coordinator's
		// get_or_load_flow call (shared cache), so they are skipped automatically.
		self.coordinator.consume(cdcs, reply);
	}
}

/// Flow subsystem - single-threaded flow processing.
pub struct FlowSubsystem {
	consumer: PollConsumer<StandardEngine, FlowConsumeDispatcher>,
	worker_handles: Vec<ActorHandle<FlowMsg>>,
	pool_handle: Option<ActorHandle<PoolMsg>>,
	coordinator_handle: Option<ActorHandle<CoordinatorMsg>>,
	transactional_flow_engine: Arc<RwLock<FlowEngine>>,
	running: bool,
}

impl FlowSubsystem {
	/// Create a new flow subsystem.
	pub fn new(config: FlowBuilderConfig, engine: StandardEngine, ioc: &IocContainer) -> Self {
		let catalog = engine.catalog();
		let executor = engine.executor();
		let event_bus = engine.event_bus().clone();

		#[cfg(reifydb_target = "native")]
		if let Some(ref operators_dir) = config.operators_dir {
			if let Err(e) = load_ffi_operators(operators_dir, &event_bus) {
				panic!("Failed to load FFI operators from {:?}: {}", operators_dir, e);
			}
			event_bus.wait_for_completion();
		}

		let runtime = ioc.resolve::<SharedRuntime>().expect("SharedRuntime must be registered");
		let clock = runtime.clock().clone();
		let runtime_context = RuntimeContext::with_clock(clock.clone());
		let runtime_context_for_factory = runtime_context.clone();

		let custom_operators = Arc::new(config.custom_operators);
		let custom_operators_for_factory = custom_operators.clone();

		let factory_builder = move || {
			let cat = catalog.clone();
			let exec = executor.clone();
			let bus = event_bus.clone();
			let rc = runtime_context_for_factory.clone();
			let co = custom_operators_for_factory.clone();

			move || FlowEngine::new(cat, exec, bus, rc, co)
		};

		let primitive_tracker = Arc::new(PrimitiveVersionTracker::new());

		let cdc_store = ioc.resolve::<CdcStore>().expect("CdcStore must be registered");

		let num_workers = config.num_workers;
		info!(num_workers, "initializing flow coordinator with {} workers", num_workers);

		let actor_system = engine.actor_system();

		let mut worker_refs = Vec::with_capacity(num_workers);
		let mut worker_handles = Vec::with_capacity(num_workers);
		for i in 0..num_workers {
			let worker_factory = factory_builder();
			let worker = FlowWorkerActor::new(worker_factory, engine.clone(), engine.catalog());
			let handle = actor_system.spawn(&format!("flow-worker-{}", i), worker);
			worker_refs.push(handle.actor_ref().clone());
			worker_handles.push(handle);
		}

		let pool = PoolActor::new(worker_refs, clock.clone());
		let pool_handle = actor_system.spawn("flow-pool", pool);
		let pool_ref = pool_handle.actor_ref().clone();

		// Shared flow catalog: clones share the same cache so the dispatcher
		// and coordinator see the same flow-cache state.
		let flow_catalog = FlowCatalog::new(engine.catalog());

		let coordinator = CoordinatorActor::new(
			engine.clone(),
			flow_catalog.clone(),
			pool_ref,
			primitive_tracker.clone(),
			cdc_store.clone(),
			num_workers,
			clock,
		);
		let coordinator_handle = actor_system.spawn("flow-coordinator", coordinator);
		let actor_ref = coordinator_handle.actor_ref().clone();

		let consumer_id = CdcConsumerId::new("flow-coordinator");
		let consumer_key = CdcConsumerKey {
			consumer: consumer_id.clone(),
		}
		.encode();
		let consume_ref = FlowConsumeRef {
			actor_ref,
			consumer_key,
		};

		// Transactional flow engine — a separate FlowEngine for transactional views only.
		let transactional_flow_engine = Arc::new(RwLock::new(FlowEngine::new(
			engine.catalog(),
			engine.executor(),
			engine.event_bus().clone(),
			RuntimeContext::with_clock(runtime.clock().clone()),
			custom_operators.clone(),
		)));

		// Registrar: detects transactional flows from CDC and registers them.
		let registrar = TransactionalFlowRegistrar {
			flow_engine: transactional_flow_engine.clone(),
			engine: engine.clone(),
			catalog: engine.catalog(),
		};

		let transactional_flow_engine_for_self = transactional_flow_engine.clone();

		// Register pre-commit, post-commit, and test pre-commit interceptors via a single factory function.
		{
			let flow_engine_for_interceptor = transactional_flow_engine.clone();
			let engine_for_interceptor = engine.clone();
			let catalog_for_interceptor = engine.catalog();
			let registrar_for_interceptor = TransactionalFlowRegistrar {
				flow_engine: transactional_flow_engine,
				engine: engine.clone(),
				catalog: engine.catalog(),
			};

			// Captures for the test_pre_commit hook.
			let test_flow_engine = flow_engine_for_interceptor.clone();
			let test_engine = engine_for_interceptor.clone();
			let test_catalog = catalog_for_interceptor.clone();
			let test_event_bus = engine.event_bus().clone();
			let test_runtime_context = RuntimeContext::with_clock(runtime.clock().clone());
			let test_custom_operators = custom_operators.clone();

			engine.add_interceptor_factory(Arc::new(move |interceptors: &mut Interceptors| {
				interceptors.pre_commit.add(Arc::new(TransactionalFlowPreCommitInterceptor {
					flow_engine: flow_engine_for_interceptor.clone(),
					engine: engine_for_interceptor.clone(),
					catalog: catalog_for_interceptor.clone(),
				}));
				interceptors.post_commit.add(Arc::new(TransactionalFlowPostCommitInterceptor {
					registrar: TransactionalFlowRegistrar {
						flow_engine: registrar_for_interceptor.flow_engine.clone(),
						engine: registrar_for_interceptor.engine.clone(),
						catalog: registrar_for_interceptor.catalog.clone(),
					},
				}));

				// Hook for test flow processing: rebuilds the shared transactional flow
				// engine from all catalog flows (including uncommitted ones visible through
				// the admin transaction), so that capture_testing_pre_commit can process
				// flows for views that haven't been committed yet.
				let hook_flow_engine = test_flow_engine.clone();
				let hook_engine = test_engine.clone();
				let hook_catalog = test_catalog.clone();
				let hook_event_bus = test_event_bus.clone();
				let hook_runtime_context = test_runtime_context.clone();
				let hook_custom_operators = test_custom_operators.clone();

				interceptors.set_test_pre_commit(Arc::new(move |test_txn: &mut TestTransaction<'_>| {
						let mut fresh_engine = FlowEngine::new(
							hook_catalog.clone(),
							hook_engine.executor(),
							hook_event_bus.clone(),
							hook_runtime_context.clone(),
							hook_custom_operators.clone(),
						);

						let flows = hook_catalog
							.list_flows_all(&mut Transaction::Test(test_txn.reborrow()))?;

						for flow in flows {
							let dag = load_flow_dag(
								&hook_catalog,
								&mut Transaction::Test(test_txn.reborrow()),
								flow.id,
							)?;
							fresh_engine.register_with_transaction(
								&mut Transaction::Test(test_txn.reborrow()),
								dag,
							)?;
						}

						*hook_flow_engine.write().unwrap() = fresh_engine;
						Ok(())
					}));
			}));
		}

		ioc.register_service::<Arc<dyn FlowLagsProvider>>(Arc::new(FlowLags::new(
			primitive_tracker,
			engine.clone(),
			flow_catalog.clone(),
		)));

		let poll_config =
			PollConsumerConfig::new(consumer_id, "flow-cdc-poll", Duration::from_millis(10), Some(100));

		// Wrap the coordinator reference in a dispatcher that handles transactional flows.
		let dispatcher = FlowConsumeDispatcher {
			coordinator: consume_ref,
			registrar,
			flow_catalog: flow_catalog.clone(),
			engine: engine.clone(),
		};

		let consumer = PollConsumer::new(poll_config, engine, dispatcher, cdc_store, actor_system);

		Self {
			consumer,
			worker_handles,
			pool_handle: Some(pool_handle),
			coordinator_handle: Some(coordinator_handle),
			transactional_flow_engine: transactional_flow_engine_for_self,
			running: false,
		}
	}
}

impl Subsystem for FlowSubsystem {
	fn name(&self) -> &'static str {
		"sub-flow"
	}

	fn start(&mut self) -> Result<()> {
		if self.running {
			return Ok(());
		}

		self.consumer.start()?;
		self.running = true;
		Ok(())
	}

	fn shutdown(&mut self) -> Result<()> {
		if !self.running {
			return Ok(());
		}

		self.consumer.stop()?;

		if let Some(handle) = self.coordinator_handle.take() {
			let _ = handle.join();
		}

		if let Some(handle) = self.pool_handle.take() {
			let _ = handle.join();
		}

		for handle in self.worker_handles.drain(..) {
			let _ = handle.join();
		}

		// Clear the transactional flow engine to drop all Arc<Operators>,
		// which triggers FFI operator cleanup and frees LRU caches.
		if let Ok(mut engine) = self.transactional_flow_engine.write() {
			engine.clear();
		}

		self.running = false;
		Ok(())
	}

	fn is_running(&self) -> bool {
		self.running
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

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}
}

impl HasVersion for FlowSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Data flow and stream processing subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

impl Drop for FlowSubsystem {
	fn drop(&mut self) {
		if self.running {
			let _ = self.shutdown();
		}
	}
}
