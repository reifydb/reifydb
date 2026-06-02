// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub mod factory;
#[cfg(reifydb_target = "native")]
pub mod ffi;

use std::{
	any::Any,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

#[cfg(reifydb_target = "native")]
use ffi::{load_ffi_operators, load_native_operators};
use reifydb_cdc::{
	consume::{
		consumer::{CdcConsume, CdcConsumer},
		poll::{PollConsumer, PollConsumerConfig},
		wake::CdcWakeRegistry,
	},
	storage::CdcStore,
};
use reifydb_core::{
	actors::flow::{FlowCoordinatorHandle, FlowCoordinatorMessage, FlowHandle, FlowMessage, FlowPoolHandle},
	interface::{
		WithEventBus,
		catalog::{
			config::{ConfigKey, GetConfig},
			flow::FlowId,
		},
		cdc::{Cdc, CdcConsumerId},
		flow::FlowWatermarkSampler,
		version::{ComponentType, HasVersion, SystemVersion},
	},
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_rql::flow::loader::load_flow_dag;
use reifydb_runtime::{
	actor::{
		mailbox::ActorRef,
		system::{ActorHandle, ActorSpawner},
	},
	context::{RuntimeContext, clock::Clock},
	shutdown::Shutdown,
	sync::mutex::Mutex,
};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_transaction::{
	interceptor::interceptors::Interceptors,
	transaction::{TestTransaction, Transaction},
};
use reifydb_value::{Result, value::identity::IdentityId};
use tracing::{info, warn};

use crate::{
	builder::{CustomOperators, FlowConfig},
	catalog::FlowCatalog,
	deferred::{
		coordinator::{CoordinatorActor, FlowConsumeRef, registration::extract_new_flow_ids},
		pool::PoolActor,
		tracker::{FlowPositionTracker, ShapeVersionTracker},
		watermark::compute_flow_watermarks,
		worker::FlowWorkerActor,
	},
	engine::{FlowEngine, FlowEngineInner},
	transaction::row_allocator::RowAllocatorRegistry,
	transactional::{
		interceptor::{TransactionalFlowPostCommitInterceptor, TransactionalFlowPreCommitInterceptor},
		registry::TransactionalFlowRegistry,
		tick::{TransactionalTickActor, TransactionalTickMessage},
	},
};

struct FlowConsumeDispatcher {
	coordinator: FlowConsumeRef,
	registrar: TransactionalFlowRegistry,
	flow_catalog: FlowCatalog,
	engine: StandardEngine,
}

impl CdcConsume for FlowConsumeDispatcher {
	fn consume(&self, cdcs: Vec<Cdc>, reply: Box<dyn FnOnce(Result<()>) + Send>) {
		let new_flow_ids = extract_new_flow_ids(&cdcs);
		if !new_flow_ids.is_empty()
			&& let Ok(mut query) = self.engine.begin_query(IdentityId::system())
		{
			for flow_id in new_flow_ids {
				match self.flow_catalog.get_or_load_flow(&mut Transaction::Query(&mut query), flow_id) {
					Ok((flow, true)) => match self.registrar.try_register(flow) {
						Ok(true) => {}
						Ok(false) => {
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
					},
					Ok((_, false)) => {}
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

		self.coordinator.consume(cdcs, reply);
	}
}

pub struct FlowSubsystem {
	consumer: Mutex<PollConsumer<StandardEngine, FlowConsumeDispatcher>>,
	flow_scope: ActorSpawner,
	worker_handles: Mutex<Vec<FlowHandle>>,
	pool_handle: Mutex<Option<FlowPoolHandle>>,
	coordinator_handle: Mutex<Option<FlowCoordinatorHandle>>,
	transactional_tick_handle: Mutex<Option<ActorHandle<TransactionalTickMessage>>>,
	transactional_flow_engine: FlowEngine,
	running: AtomicBool,
}

impl FlowSubsystem {
	pub fn new(config: FlowConfig, engine: StandardEngine, ioc: &IocContainer) -> Result<Self> {
		Self::maybe_load_ffi_operators(&config, &engine);

		let clock = ioc.resolve::<Clock>().expect("Clock must be registered");
		let spawner = ioc.resolve::<ActorSpawner>().expect("ActorSpawner must be registered");
		let custom_operators = CustomOperators::new(config.custom_operators);
		let row_allocators = RowAllocatorRegistry::new();
		let primitive_tracker = ShapeVersionTracker::new();
		let flow_tracker = FlowPositionTracker::new();
		let cdc_store = ioc.resolve::<CdcStore>().expect("CdcStore must be registered");

		let flow_scope = spawner.scope();
		let configured_workers = engine.catalog().get_config_uint2(ConfigKey::FlowWorkerThreads) as usize;
		let num_workers = if configured_workers == 0 {
			spawner.pools().system_thread_count()
		} else {
			configured_workers
		}
		.max(2);
		info!(num_workers, "initializing flow coordinator with {} workers", num_workers);

		let flow_catalog = FlowCatalog::new(engine.catalog());

		let (worker_refs, worker_handles) = Self::spawn_flow_workers(
			&flow_scope,
			num_workers,
			&engine,
			&flow_catalog,
			&clock,
			&custom_operators,
			&row_allocators,
		);

		let pool_handle = flow_scope.spawn_system("flow-pool", PoolActor::new(worker_refs, clock.clone()));
		let pool_ref = pool_handle.actor_ref().clone();

		let flow_consumer_id = CdcConsumerId::new("flow-coordinator");
		let coordinator_handle = flow_scope.spawn_system(
			"flow-coordinator",
			CoordinatorActor::new(
				engine.clone(),
				flow_catalog.clone(),
				pool_ref,
				primitive_tracker.clone(),
				flow_tracker.clone(),
				cdc_store.clone(),
				num_workers,
				clock.clone(),
				flow_consumer_id.clone(),
			),
		);
		let consume_ref = FlowConsumeRef {
			actor_ref: coordinator_handle.actor_ref().clone(),
		};

		let transactional_flow_engine =
			Self::build_transactional_engine(&engine, &clock, &custom_operators, &row_allocators);

		let registrar = TransactionalFlowRegistry {
			flow_engine: transactional_flow_engine.clone(),
			engine: engine.clone(),
			catalog: engine.catalog(),
		};

		Self::register_flow_interceptors(&engine, &transactional_flow_engine, &clock, &custom_operators);

		let transactional_tick_handle = flow_scope.spawn_system(
			"transactional-flow-tick",
			TransactionalTickActor::new(
				transactional_flow_engine.clone(),
				engine.clone(),
				engine.catalog(),
				clock.clone(),
			),
		);

		Self::register_watermark_sampler(ioc, &primitive_tracker, &flow_tracker, &flow_catalog);

		let cdc_wake_registry = ioc.resolve::<CdcWakeRegistry>().expect("CdcWakeRegistry must be registered");
		let poll_config =
			PollConsumerConfig::new(flow_consumer_id, "flow-cdc-poll", Duration::from_secs(1), Some(100))
				.with_wake_registry(cdc_wake_registry);

		let bootstrap_flows = Self::bootstrap_flows(&engine, &flow_catalog, &registrar);
		let _ = coordinator_handle.actor_ref().send(FlowCoordinatorMessage::Bootstrap {
			flows: bootstrap_flows,
		});

		let dispatcher = FlowConsumeDispatcher {
			coordinator: consume_ref,
			registrar,
			flow_catalog,
			engine: engine.clone(),
		};
		let mut consumer = PollConsumer::new(poll_config, engine, dispatcher, cdc_store, flow_scope.clone());
		consumer.start()?;

		Ok(Self {
			consumer: Mutex::new(consumer),
			flow_scope,
			worker_handles: Mutex::new(worker_handles),
			pool_handle: Mutex::new(Some(pool_handle)),
			coordinator_handle: Mutex::new(Some(coordinator_handle)),
			transactional_tick_handle: Mutex::new(Some(transactional_tick_handle)),
			transactional_flow_engine,
			running: AtomicBool::new(true),
		})
	}

	#[inline]
	fn maybe_load_ffi_operators(config: &FlowConfig, engine: &StandardEngine) {
		#[cfg(reifydb_target = "native")]
		if let Some(ref operators_dir) = config.operators_dir {
			let event_bus = engine.event_bus();
			if let Err(e) = load_native_operators(operators_dir, event_bus) {
				panic!("Failed to load native operators from {:?}: {}", operators_dir, e);
			}
			if let Err(e) = load_ffi_operators(operators_dir, event_bus) {
				panic!("Failed to load FFI operators from {:?}: {}", operators_dir, e);
			}
			event_bus.wait_for_completion();
		}
		#[cfg(not(reifydb_target = "native"))]
		{
			let _ = (config, engine);
		}
	}

	#[inline]
	fn spawn_flow_workers(
		spawner: &ActorSpawner,
		num_workers: usize,
		engine: &StandardEngine,
		flow_catalog: &FlowCatalog,
		clock: &Clock,
		custom_operators: &CustomOperators,
		row_allocators: &RowAllocatorRegistry,
	) -> (Vec<ActorRef<FlowMessage>>, Vec<FlowHandle>) {
		let mut worker_refs = Vec::with_capacity(num_workers);
		let mut worker_handles = Vec::with_capacity(num_workers);

		for i in 0..num_workers {
			let cat = engine.catalog();
			let exec = engine.executor();
			let bus = engine.event_bus().clone();
			let rc = RuntimeContext::with_clock(clock.clone());
			let co = custom_operators.clone();
			let ra = row_allocators.clone();
			let worker_factory = move || FlowEngineInner::new(cat, exec, bus, rc, co, ra);

			let worker = FlowWorkerActor::new(
				worker_factory,
				engine.clone(),
				engine.catalog(),
				flow_catalog.clone(),
			);
			let handle = spawner.spawn_system(&format!("flow-worker-{}", i), worker);
			worker_refs.push(handle.actor_ref().clone());
			worker_handles.push(handle);
		}

		(worker_refs, worker_handles)
	}

	#[inline]
	fn build_transactional_engine(
		engine: &StandardEngine,
		clock: &Clock,
		custom_operators: &CustomOperators,
		row_allocators: &RowAllocatorRegistry,
	) -> FlowEngine {
		FlowEngine::new(
			engine.catalog(),
			engine.executor(),
			engine.event_bus().clone(),
			RuntimeContext::with_clock(clock.clone()),
			custom_operators.clone(),
			row_allocators.clone(),
		)
	}

	#[inline]
	fn register_watermark_sampler(
		ioc: &IocContainer,
		primitive_tracker: &ShapeVersionTracker,
		flow_tracker: &FlowPositionTracker,
		flow_catalog: &FlowCatalog,
	) {
		ioc.register_service::<FlowWatermarkSampler>(FlowWatermarkSampler::new({
			let tracker = primitive_tracker.clone();
			let flow_tracker = flow_tracker.clone();
			let flow_catalog = flow_catalog.clone();
			move || compute_flow_watermarks(&tracker, &flow_tracker, &flow_catalog)
		}));
	}

	#[inline]
	fn bootstrap_flows(
		engine: &StandardEngine,
		flow_catalog: &FlowCatalog,
		registrar: &TransactionalFlowRegistry,
	) -> Vec<(FlowId, bool)> {
		let mut bootstrap_flows = Vec::new();
		if let Ok(mut query) = engine.begin_query(IdentityId::system()) {
			match engine.catalog().list_flows_all(&mut Transaction::Query(&mut query)) {
				Ok(existing_flows) => {
					for existing in existing_flows {
						match flow_catalog.get_or_load_flow(
							&mut Transaction::Query(&mut query),
							existing.id,
						) {
							Ok((flow, _)) => match registrar.try_register(flow) {
								Ok(is_transactional) => bootstrap_flows
									.push((existing.id, !is_transactional)),
								Err(e) => warn!(
									flow_id = existing.id.0,
									error = %e,
									"failed to register transactional flow during bootstrap"
								),
							},
							Err(e) => warn!(
								flow_id = existing.id.0,
								error = %e,
								"failed to load flow during bootstrap"
							),
						}
					}
				}
				Err(e) => warn!(error = %e, "failed to list flows during bootstrap"),
			}
		}
		bootstrap_flows
	}

	#[inline]
	fn register_flow_interceptors(
		engine: &StandardEngine,
		transactional_flow_engine: &FlowEngine,
		clock: &Clock,
		custom_operators: &CustomOperators,
	) {
		let flow_engine_for_pre = transactional_flow_engine.clone();
		let engine_for_pre = engine.clone();
		let catalog_for_pre = engine.catalog();

		let flow_engine_for_post = transactional_flow_engine.clone();
		let engine_for_post = engine.clone();
		let catalog_for_post = engine.catalog();

		let test_flow_engine = transactional_flow_engine.clone();
		let test_engine = engine.clone();
		let test_catalog = engine.catalog();
		let test_event_bus = engine.event_bus().clone();
		let test_runtime_context = RuntimeContext::with_clock(clock.clone());
		let test_custom_operators = custom_operators.clone();

		engine.add_interceptor_factory(Arc::new(move |interceptors: &mut Interceptors| {
			interceptors.pre_commit.add(Arc::new(TransactionalFlowPreCommitInterceptor {
				flow_engine: flow_engine_for_pre.clone(),
				engine: engine_for_pre.clone(),
				catalog: catalog_for_pre.clone(),
			}));
			interceptors.post_commit.add(Arc::new(TransactionalFlowPostCommitInterceptor {
				registrar: TransactionalFlowRegistry {
					flow_engine: flow_engine_for_post.clone(),
					engine: engine_for_post.clone(),
					catalog: catalog_for_post.clone(),
				},
			}));

			let hook_flow_engine = test_flow_engine.clone();
			let hook_engine = test_engine.clone();
			let hook_catalog = test_catalog.clone();
			let hook_event_bus = test_event_bus.clone();
			let hook_runtime_context = test_runtime_context.clone();
			let hook_custom_operators = test_custom_operators.clone();

			interceptors.set_test_pre_commit(Arc::new(move |test_txn: &mut TestTransaction<'_>| {
				let mut fresh_engine = FlowEngineInner::new(
					hook_catalog.clone(),
					hook_engine.executor(),
					hook_event_bus.clone(),
					hook_runtime_context.clone(),
					hook_custom_operators.clone(),
					RowAllocatorRegistry::new(),
				);

				let flows = hook_catalog
					.list_flows_all(&mut Transaction::Test(Box::new(test_txn.reborrow())))?;

				for flow in flows {
					let dag = load_flow_dag(
						&hook_catalog,
						&mut Transaction::Test(Box::new(test_txn.reborrow())),
						flow.id,
					)?;
					fresh_engine.register_with_transaction(
						&mut Transaction::Test(Box::new(test_txn.reborrow())),
						dag,
					)?;
				}

				*hook_flow_engine.write() = fresh_engine;
				Ok(())
			}));
		}));
	}
}

impl Shutdown for FlowSubsystem {
	fn shutdown(&self) {
		if self.running.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
			return;
		}

		if let Err(e) = self.consumer.lock().stop() {
			warn!(error = %e, "flow consumer stop failed during shutdown");
		}

		self.flow_scope.shutdown();

		if let Some(handle) = self.coordinator_handle.lock().take() {
			let _ = handle.join();
		}

		if let Some(handle) = self.pool_handle.lock().take() {
			let _ = handle.join();
		}

		if let Some(handle) = self.transactional_tick_handle.lock().take() {
			let _ = handle.join();
		}

		let workers: Vec<_> = self.worker_handles.lock().drain(..).collect();
		for handle in workers {
			let _ = handle.join();
		}

		self.transactional_flow_engine.write().clear();
	}
}

impl Subsystem for FlowSubsystem {
	fn name(&self) -> &'static str {
		"sub-flow"
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
		self.shutdown();
	}
}
