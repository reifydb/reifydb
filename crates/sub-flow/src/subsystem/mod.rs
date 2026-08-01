// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod factory;
#[cfg(reifydb_target = "native")]
pub mod ffi;

use std::{
	any::Any,
	path::PathBuf,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

#[cfg(reifydb_target = "native")]
use ffi::{load_ffi_operators, load_native_operators};
use reifydb_cdc::{
	consume::{
		backlog::FlowBacklog,
		watermark::{CdcConsumerWatermark, FlowCaughtUpWatermark},
	},
	storage::CdcStore,
};
use reifydb_core::{
	actors::flow::{FlowSupervisorHandle, FlowSupervisorMessage},
	event::operator::OperatorLoadedEvent,
	interface::{
		WithEventBus,
		catalog::{
			config::{ConfigKey, GetConfig},
			flow::FlowId,
		},
		cdc::CdcConsumerId,
		flow::FlowWatermarkSampler,
		version::{ComponentType, HasVersion, SystemVersion},
	},
	lifecycle::{
		class::RetentionClass, coverage::RetentionCoverage, metrics::RetentionMetrics,
		watermark::ConsumerPositions,
	},
	metrics::registry::MetricsRegistry,
	state::budget::OperatorStateBudgetHandle,
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_flow::transaction::substrate::FlowSubstrate;
use reifydb_rql::flow::loader::load_flow_dag;
use reifydb_runtime::{
	actor::system::{ActorHandle, ActorSpawner},
	context::{RuntimeContext, clock::Clock},
	shutdown::Shutdown,
	sync::mutex::Mutex,
};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_transaction::{
	group::{GroupCommitBegin, GroupCommitHandle},
	interceptor::interceptors::Interceptors,
	transaction::{TestTransaction, Transaction},
};
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	value::{Value, duration::Duration, identity::IdentityId},
};
use tracing::warn;

use crate::{
	builder::{CustomOperators, FlowConfig},
	catalog::FlowCatalog,
	deferred::{
		committer::{Committer, CommitterActor, CommitterHandle},
		frontier::ControlFrontier,
		health::FlowHealthRegistry,
		loader::{LoaderActor, LoaderHandle, LoaderMetrics},
		quiescence::FlowMaterialization,
		supervisor::{FlowSupervisor, FlowSupervisorParams},
		tracker::{FlowPositionTracker, ObjectVersionTracker},
		watermark::compute_flow_watermarks,
	},
	engine::{FlowEngine, FlowEngineInner},
	lineage::FlowLineageTracker,
	operator::metrics::{
		GroupInternerMetricsCollector, OperatorSampleCollector, OperatorSampleRegistry,
		OperatorStateBudgetCollector, RowNumberMetricsCollector,
	},
	transactional::{
		interceptor::{TransactionalFlowPostCommitInterceptor, TransactionalFlowPreCommitInterceptor},
		registry::TransactionalFlowRegistry,
		tick::{TransactionalTickActor, TransactionalTickMessage},
	},
};

/// Versions of in-memory skip-ahead a flow tolerates before forcing a checkpoint-only commit.
const FLOW_CHECKPOINT_LAG: u64 = 10_000;
const FLOW_CHECKPOINT_MAX_AGE_MS: i64 = 5_000;

const FLOW_TICK_RECLAIM: &str = "flow-tick-reclaim";

pub struct FlowSubsystem {
	flow_scope: ActorSpawner,
	loader_handle: Mutex<Option<LoaderHandle>>,
	committer_handle: Mutex<Option<CommitterHandle>>,
	supervisor_handle: Mutex<Option<FlowSupervisorHandle>>,
	transactional_tick_handle: Mutex<Option<ActorHandle<TransactionalTickMessage>>>,
	transactional_flow_engine: FlowEngine,
	lineage: FlowLineageTracker,
	health: FlowHealthRegistry,
	running: AtomicBool,
}

impl FlowSubsystem {
	pub fn publish_operator_catalog(config: &FlowConfig, engine: &StandardEngine) {
		Self::publish_custom_operators(&config.custom_operators, engine);
	}

	pub fn new(config: FlowConfig, engine: StandardEngine, ioc: &IocContainer) -> Result<Self> {
		Self::maybe_load_ffi_operators(&config, &engine);

		let clock = ioc.resolve::<Clock>().expect("Clock must be registered");
		let spawner = ioc.resolve::<ActorSpawner>().expect("ActorSpawner must be registered");
		let custom_operators = config.custom_operators;
		let substrate = FlowSubstrate::with_dictionary(engine.dictionary_allocators());
		let object_tracker = ObjectVersionTracker::new();
		let flow_tracker = FlowPositionTracker::new();
		let cdc_store = ioc.resolve::<CdcStore>().expect("CdcStore must be registered");

		let flow_scope = spawner.scope();
		let flow_catalog = FlowCatalog::new(engine.catalog());

		let group_commit = ioc.try_resolve::<GroupCommitHandle>().unwrap_or_else(|| {
			let begin_engine = engine.clone();
			let begin: GroupCommitBegin =
				Arc::new(move || begin_engine.begin_command(IdentityId::system()));
			GroupCommitHandle::inline(begin)
		});
		let state_budget = ioc
			.resolve::<OperatorStateBudgetHandle>()
			.expect("OperatorStateBudgetHandle must be registered");
		let retention_metrics = ioc.resolve::<RetentionMetrics>().expect("RetentionMetrics must be registered");
		if let Some(coverage) = ioc.try_resolve::<RetentionCoverage>() {
			coverage.cover(RetentionClass::OperatorGroupData, FLOW_TICK_RECLAIM);
			coverage.cover(RetentionClass::OperatorGroupIdentity, FLOW_TICK_RECLAIM);
		}
		let poll_frontier = CdcConsumerWatermark::default();
		let materialization = FlowMaterialization::new(poll_frontier.clone(), flow_tracker.clone());
		let committer = Committer::new(flow_catalog.clone(), flow_tracker.clone(), materialization.clone());
		let committer_handle = flow_scope.spawn_flow(
			"flow-committer",
			CommitterActor::new(committer, group_commit, state_budget.clone()),
		);
		let committer_ref = committer_handle.actor_ref().clone();

		let health = FlowHealthRegistry::new();
		let operator_samples = OperatorSampleRegistry::new();
		let metrics_registry = ioc.resolve::<MetricsRegistry>().expect("MetricsRegistry must be registered");
		metrics_registry
			.register_operator_collector(Arc::new(OperatorSampleCollector::new(operator_samples.clone())));
		metrics_registry.register_collector(Arc::new(OperatorStateBudgetCollector::new(state_budget.clone())));
		metrics_registry
			.register_operator_collector(Arc::new(RowNumberMetricsCollector::new(substrate.row.clone())));
		metrics_registry.register_operator_collector(Arc::new(GroupInternerMetricsCollector::new(
			substrate.group.clone(),
		)));
		let transactional_flow_engine = Self::build_transactional_engine(
			&engine,
			&clock,
			&custom_operators,
			&substrate,
			&operator_samples,
			&state_budget,
			&retention_metrics,
		);

		let lineage = FlowLineageTracker::new(engine.view_lineage());

		let registrar = TransactionalFlowRegistry {
			flow_engine: transactional_flow_engine.clone(),
			engine: engine.clone(),
			catalog: engine.catalog(),
			lineage: lineage.clone(),
		};

		let backlog = ioc.resolve::<FlowBacklog>().expect("FlowBacklog must be registered");
		metrics_registry.register_collector(Arc::new(backlog.clone()));
		let loader_metrics = LoaderMetrics::default();
		metrics_registry.register_collector(Arc::new(loader_metrics.clone()));
		let control = ControlFrontier::new();
		let loader_handle =
			flow_scope.spawn_flow("flow-loader", LoaderActor::new(cdc_store.hot_reader(), loader_metrics));
		let pull_batch_bytes =
			ByteSize::from_bytes(engine.catalog().get_config_uint8(ConfigKey::FlowPullBatchBytes));
		let load_batch_bytes =
			ByteSize::from_bytes(engine.catalog().get_config_uint8(ConfigKey::FlowLoadBatchBytes));

		let flow_consumer_id = CdcConsumerId::flow_consumer();
		let supervisor_handle = flow_scope.spawn_flow(
			"flow-supervisor",
			FlowSupervisor::new(FlowSupervisorParams {
				engine: engine.clone(),
				flow_catalog: flow_catalog.clone(),
				committer: committer_ref,
				backlog: backlog.clone(),
				loader: loader_handle.actor_ref().clone(),
				control,
				poll_frontier: poll_frontier.clone(),
				registrar: registrar.clone(),
				tracker: object_tracker.clone(),
				flow_tracker: flow_tracker.clone(),
				health: health.clone(),
				custom_operators: custom_operators.clone(),
				substrate: substrate.clone(),
				operator_samples: operator_samples.clone(),
				state_budget: state_budget.clone(),
				retention_metrics: retention_metrics.clone(),
				clock: clock.clone(),
				spawner: flow_scope.clone(),
				consumer_id: flow_consumer_id,
				pull_batch_bytes,
				load_batch_bytes,
				checkpoint_lag: FLOW_CHECKPOINT_LAG,
				checkpoint_max_age: Duration::from_milliseconds(FLOW_CHECKPOINT_MAX_AGE_MS).unwrap(),
			}),
		);

		Self::register_flow_interceptors(
			&engine,
			&transactional_flow_engine,
			&lineage,
			&clock,
			&custom_operators,
		);

		let transactional_tick_handle = flow_scope.spawn_flow(
			"transactional-flow-tick",
			TransactionalTickActor::new(
				transactional_flow_engine.clone(),
				engine.clone(),
				engine.catalog(),
				clock.clone(),
			),
		);

		Self::register_watermark_sampler(
			ioc,
			&engine,
			&object_tracker,
			&flow_tracker,
			&flow_catalog,
			&materialization,
		);

		ioc.register_service::<FlowCaughtUpWatermark>(FlowCaughtUpWatermark::new(move || {
			materialization.caught_up()
		}));

		ioc.register_service::<Arc<dyn ConsumerPositions>>(Arc::new(flow_tracker.clone()));

		let scan_from = engine.current_version().ok();
		let bootstrap_flows = Self::bootstrap_flows(&engine, &flow_catalog, &registrar);
		let _ = supervisor_handle.actor_ref().send(FlowSupervisorMessage::Bootstrap {
			flows: bootstrap_flows,
			scan_from,
		});

		let supervisor_ref = supervisor_handle.actor_ref().clone();
		backlog.set_waker(move || {
			let _ = supervisor_ref.send(FlowSupervisorMessage::Wake);
		});
		let _ = supervisor_handle.actor_ref().send(FlowSupervisorMessage::Wake);

		Ok(Self {
			flow_scope,
			loader_handle: Mutex::new(Some(loader_handle)),
			committer_handle: Mutex::new(Some(committer_handle)),
			supervisor_handle: Mutex::new(Some(supervisor_handle)),
			transactional_tick_handle: Mutex::new(Some(transactional_tick_handle)),
			transactional_flow_engine,
			lineage,
			health,
			running: AtomicBool::new(true),
		})
	}

	#[inline]
	fn publish_custom_operators(custom_operators: &CustomOperators, engine: &StandardEngine) {
		let event_bus = engine.event_bus();
		for (name, entry) in custom_operators.iter() {
			event_bus.emit(OperatorLoadedEvent::new(
				name.clone(),
				PathBuf::new(),
				entry.api,
				entry.version.clone(),
				entry.description.clone(),
				entry.input.clone(),
				entry.output.clone(),
				entry.capabilities,
			));
		}
		event_bus.wait_for_completion();
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
		substrate: &FlowSubstrate,
		operator_samples: &OperatorSampleRegistry,
		state_budget: &OperatorStateBudgetHandle,
		retention_metrics: &RetentionMetrics,
	) -> FlowEngine {
		let flow_engine = FlowEngine::new(
			engine.catalog(),
			engine.executor(),
			engine.event_bus().clone(),
			RuntimeContext::with_clock(clock.clone()),
			custom_operators.clone(),
			substrate.clone(),
			operator_samples.clone(),
			state_budget.clone(),
		);
		flow_engine.write().adopt_retention_metrics(retention_metrics.clone());
		flow_engine
	}

	#[inline]
	fn register_watermark_sampler(
		ioc: &IocContainer,
		engine: &StandardEngine,
		object_tracker: &ObjectVersionTracker,
		flow_tracker: &FlowPositionTracker,
		flow_catalog: &FlowCatalog,
		materialization: &FlowMaterialization,
	) {
		ioc.register_service::<FlowWatermarkSampler>(FlowWatermarkSampler::new({
			let engine = engine.clone();
			let tracker = object_tracker.clone();
			let flow_tracker = flow_tracker.clone();
			let flow_catalog = flow_catalog.clone();
			let materialization = materialization.clone();
			move || {
				compute_flow_watermarks(&tracker, &flow_tracker, &flow_catalog, || {
					engine.done_until().max(materialization.output_frontier())
				})
			}
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
		lineage: &FlowLineageTracker,
		clock: &Clock,
		custom_operators: &CustomOperators,
	) {
		let flow_engine_for_pre = transactional_flow_engine.clone();
		let engine_for_pre = engine.clone();
		let catalog_for_pre = engine.catalog();

		let flow_engine_for_post = transactional_flow_engine.clone();
		let engine_for_post = engine.clone();
		let catalog_for_post = engine.catalog();
		let lineage_for_post = lineage.clone();

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
					lineage: lineage_for_post.clone(),
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
					FlowSubstrate::with_dictionary(hook_engine.dictionary_allocators()),
					OperatorSampleRegistry::new(),
					OperatorStateBudgetHandle::new(state_budget_default()),
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

fn state_budget_default() -> ByteSize {
	match ConfigKey::OperatorStateMemoryLimit.default_value() {
		Value::Uint8(bytes) => ByteSize::from_bytes(bytes),
		other => panic!("OPERATOR_STATE_MEMORY_LIMIT default must be Uint8 bytes, got {:?}", other),
	}
}

impl Shutdown for FlowSubsystem {
	fn shutdown(&self) {
		if self.running.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
			return;
		}

		self.flow_scope.shutdown();

		if let Some(handle) = self.supervisor_handle.lock().take() {
			let _ = handle.join();
		}

		if let Some(handle) = self.loader_handle.lock().take() {
			let _ = handle.join();
		}

		if let Some(handle) = self.committer_handle.lock().take() {
			let _ = handle.join();
		}

		if let Some(handle) = self.transactional_tick_handle.lock().take() {
			let _ = handle.join();
		}

		self.transactional_flow_engine.write().clear();
		self.lineage.clear();
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
