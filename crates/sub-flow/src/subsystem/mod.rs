// SPDX-License-Identifier: AGPL-3.0-or-later
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
use reifydb_runtime::{SharedRuntime, actor::system::ActorHandle};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_transaction::{interceptor::interceptors::Interceptors, transaction::Transaction};
use reifydb_type::Result;
use tracing::info;

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
			if let Ok(mut query) = self.engine.begin_query() {
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
									tracing::warn!(
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
							tracing::warn!(
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
		let clock_for_factory = clock.clone();

		let factory_builder = move || {
			let cat = catalog.clone();
			let exec = executor.clone();
			let bus = event_bus.clone();
			let clk = clock_for_factory.clone();

			move || FlowEngine::new(cat, exec, bus, clk)
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
			runtime.clock().clone(),
		)));

		// Registrar: detects transactional flows from CDC and registers them.
		let registrar = TransactionalFlowRegistrar {
			flow_engine: transactional_flow_engine.clone(),
			engine: engine.clone(),
			catalog: engine.catalog(),
		};

		// Register both pre-commit and post-commit interceptors via a single factory function.
		{
			let flow_engine_for_interceptor = transactional_flow_engine.clone();
			let engine_for_interceptor = engine.clone();
			let catalog_for_interceptor = engine.catalog();
			let registrar_for_interceptor = TransactionalFlowRegistrar {
				flow_engine: transactional_flow_engine,
				engine: engine.clone(),
				catalog: engine.catalog(),
			};

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
