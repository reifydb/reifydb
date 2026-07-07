// SPDX-License-Identifier: Apache-2.0
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
};

#[cfg(reifydb_target = "native")]
use ffi::{load_ffi_operators, load_native_operators};
use reifydb_cdc::{
	consume::{
		consumer::{CdcConsume, CdcConsumer},
		poll::{PollConsumer, PollConsumerConfig},
		wake::CdcWakeRegistry,
		watermark::{CdcConsumerWatermark, FlowCaughtUpWatermark},
	},
	storage::CdcStore,
};
use reifydb_core::{
	actors::flow::{FlowSupervisorHandle, FlowSupervisorMessage},
	interface::{
		WithEventBus,
		catalog::flow::FlowId,
		cdc::{Cdc, CdcConsumerId},
		flow::FlowWatermarkSampler,
		version::{ComponentType, HasVersion, SystemVersion},
	},
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_rql::flow::loader::load_flow_dag;
use reifydb_runtime::{
	actor::system::{ActorHandle, ActorSpawner},
	context::{RuntimeContext, clock::Clock},
	shutdown::Shutdown,
	sync::mutex::Mutex,
};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_transaction::{
	interceptor::interceptors::Interceptors,
	transaction::{TestTransaction, Transaction},
};
use reifydb_value::{
	Result,
	value::{duration::Duration, identity::IdentityId},
};
use tracing::warn;

use crate::{
	builder::{CustomOperators, FlowConfig},
	catalog::FlowCatalog,
	deferred::{
		committer::{Committer, CommitterActor, CommitterHandle},
		ddl::extract_new_flow_ids,
		health::FlowHealthRegistry,
		supervisor::{FlowConsumeRef, FlowSupervisor},
		tracker::{FlowPositionTracker, ShapeVersionTracker},
		watermark::compute_flow_watermarks,
	},
	engine::{FlowEngine, FlowEngineInner},
	transaction::allocators::FlowAllocators,
	transactional::{
		interceptor::{TransactionalFlowPostCommitInterceptor, TransactionalFlowPreCommitInterceptor},
		registry::TransactionalFlowRegistry,
		tick::{TransactionalTickActor, TransactionalTickMessage},
	},
};

/// Maximum CDC transactions a flow actor pulls and commits per chunk.
const FLOW_CHUNK_SIZE: u64 = 1_000;

/// Versions of in-memory skip-ahead a flow tolerates before forcing a checkpoint-only commit.
const FLOW_CHECKPOINT_LAG: u64 = 10_000;

struct FlowConsumeDispatcher {
	flow_consumer: FlowConsumeRef,
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
					Ok((flow, true)) => match self.registrar.try_register(flow, &mut query) {
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

		self.flow_consumer.consume(cdcs, reply);
	}
}

pub struct FlowSubsystem {
	consumer: Mutex<PollConsumer<StandardEngine, FlowConsumeDispatcher>>,
	flow_scope: ActorSpawner,
	committer_handle: Mutex<Option<CommitterHandle>>,
	supervisor_handle: Mutex<Option<FlowSupervisorHandle>>,
	transactional_tick_handle: Mutex<Option<ActorHandle<TransactionalTickMessage>>>,
	transactional_flow_engine: FlowEngine,
	health: FlowHealthRegistry,
	running: AtomicBool,
}

impl FlowSubsystem {
	pub fn new(config: FlowConfig, engine: StandardEngine, ioc: &IocContainer) -> Result<Self> {
		Self::maybe_load_ffi_operators(&config, &engine);

		let clock = ioc.resolve::<Clock>().expect("Clock must be registered");
		let spawner = ioc.resolve::<ActorSpawner>().expect("ActorSpawner must be registered");
		let custom_operators = CustomOperators::new(config.custom_operators);
		let allocators = FlowAllocators::with_dictionary(engine.dictionary_allocators());
		let primitive_tracker = ShapeVersionTracker::new();
		let flow_tracker = FlowPositionTracker::new();
		let cdc_store = ioc.resolve::<CdcStore>().expect("CdcStore must be registered");

		let flow_scope = spawner.scope();
		let flow_catalog = FlowCatalog::new(engine.catalog());

		let committer = Committer::new(engine.clone(), flow_catalog.clone(), flow_tracker.clone());
		let committer_handle = flow_scope.spawn_system("flow-committer", CommitterActor::new(committer));
		let committer_ref = committer_handle.actor_ref().clone();

		let health = FlowHealthRegistry::new();
		let flow_consumer_id = CdcConsumerId::flow_consumer();
		let supervisor_handle = flow_scope.spawn_system(
			"flow-supervisor",
			FlowSupervisor::new(
				engine.clone(),
				flow_catalog.clone(),
				committer_ref,
				cdc_store.clone(),
				primitive_tracker.clone(),
				flow_tracker.clone(),
				health.clone(),
				custom_operators.clone(),
				allocators.clone(),
				clock.clone(),
				flow_scope.clone(),
				flow_consumer_id.clone(),
				FLOW_CHUNK_SIZE,
				FLOW_CHECKPOINT_LAG,
			),
		);
		let flow_consumer = FlowConsumeRef {
			actor_ref: supervisor_handle.actor_ref().clone(),
		};

		let transactional_flow_engine =
			Self::build_transactional_engine(&engine, &clock, &custom_operators, &allocators);

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

		// How far the flow poll consumer has *discovered* CDC (and spawned/nudged flows). Advances the
		// moment a batch is consumed - before the per-flow actors have committed their output - so it is
		// only an input to the "caught up" watermark below, never the caught-up signal itself.
		let poll_frontier = CdcConsumerWatermark::default();

		// The "caught up" watermark the `reifydb` facade resolves for `wait_for_flow_consumer`: the
		// version up to which every live deferred flow has actually materialized. It is the min of the
		// poll frontier (gates discovery + covers the no-flows case) and the slowest deferred flow's
		// processed position. `flow_tracker` holds exactly the live deferred flows: the supervisor
		// seeds an entry when it spawns one, each FlowActor advances its own entry (on commit AND on
		// skip, so empty-output flows still report progress), and the committer removes it on drop.
		// Transactional flows never enter it. Computed on read so it always reflects current progress.
		let caught_up = {
			let poll_frontier = poll_frontier.clone();
			let flow_tracker = flow_tracker.clone();
			FlowCaughtUpWatermark::new(move || {
				let poll = poll_frontier.get();
				match flow_tracker.all().values().min().copied() {
					Some(slowest) => poll.min(slowest),
					None => poll,
				}
			})
		};
		ioc.register_service::<FlowCaughtUpWatermark>(caught_up);

		let cdc_wake_registry = ioc.resolve::<CdcWakeRegistry>().expect("CdcWakeRegistry must be registered");
		let poll_config = PollConsumerConfig::new(
			flow_consumer_id,
			"flow-cdc-poll",
			Duration::from_seconds(1).unwrap(),
			Some(100),
		)
		.with_wake_registry(cdc_wake_registry)
		.with_consumer_watermark(poll_frontier.clone());

		let bootstrap_flows = Self::bootstrap_flows(&engine, &flow_catalog, &registrar);
		let _ = supervisor_handle.actor_ref().send(FlowSupervisorMessage::Bootstrap {
			flows: bootstrap_flows,
		});

		let dispatcher = FlowConsumeDispatcher {
			flow_consumer,
			registrar,
			flow_catalog,
			engine: engine.clone(),
		};
		let mut consumer = PollConsumer::new(poll_config, engine, dispatcher, cdc_store, flow_scope.clone());
		consumer.start()?;

		Ok(Self {
			consumer: Mutex::new(consumer),
			flow_scope,
			committer_handle: Mutex::new(Some(committer_handle)),
			supervisor_handle: Mutex::new(Some(supervisor_handle)),
			transactional_tick_handle: Mutex::new(Some(transactional_tick_handle)),
			transactional_flow_engine,
			health,
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
	fn build_transactional_engine(
		engine: &StandardEngine,
		clock: &Clock,
		custom_operators: &CustomOperators,
		allocators: &FlowAllocators,
	) -> FlowEngine {
		FlowEngine::new(
			engine.catalog(),
			engine.executor(),
			engine.event_bus().clone(),
			RuntimeContext::with_clock(clock.clone()),
			custom_operators.clone(),
			allocators.clone(),
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
							Ok((flow, _)) => match registrar.try_register(flow, &mut query)
							{
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
					FlowAllocators::with_dictionary(hook_engine.dictionary_allocators()),
				);

				let flows = hook_catalog
					.list_flows_all(&mut Transaction::Test(Box::new(test_txn.reborrow())))?;

				for flow in flows {
					let dag = load_flow_dag(
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

		if let Some(handle) = self.supervisor_handle.lock().take() {
			let _ = handle.join();
		}

		if let Some(handle) = self.committer_handle.lock().take() {
			let _ = handle.join();
		}

		if let Some(handle) = self.transactional_tick_handle.lock().take() {
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
		if !self.is_running() {
			return HealthStatus::Unknown;
		}
		let poisoned = self.health.poisoned();
		if poisoned.is_empty() {
			return HealthStatus::Healthy;
		}
		let flows: Vec<String> =
			poisoned.iter().map(|(id, reason)| format!("flow {}: {}", id.0, reason)).collect();
		HealthStatus::Degraded {
			description: format!("{} deferred flow(s) poisoned: {}", poisoned.len(), flows.join("; ")),
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
