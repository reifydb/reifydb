// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
pub mod extern_c;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
pub mod extern_rust;
pub mod factory;
pub mod shutdown;

use std::{
	any::Any,
	path::PathBuf,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use extern_c::load_extern_c_operators;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use extern_rust::load_extern_rust_operators;
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
	lifecycle::watermark::ConsumerPositions,
	metrics::registry::MetricsRegistry,
	util::ioc::IocContainer,
};
use reifydb_engine::{engine::StandardEngine, vm::flow_lineage::ViewLineage};
use reifydb_flow::{
	operator::metrics::{OperatorSampleCollector, OperatorSampleRegistry, RowNumberMetricsCollector},
	transaction::substrate::FlowSubstrate,
};
use reifydb_runtime::{actor::system::ActorSpawner, context::clock::Clock, shutdown::Shutdown, sync::mutex::Mutex};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_transaction::{
	group::{GroupCommitBegin, GroupCommitHandle},
	transaction::Transaction,
};
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	value::{duration::Duration, identity::IdentityId},
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
	subsystem::shutdown::FlowShutdownState,
};

/// Versions of in-memory skip-ahead a flow tolerates before forcing a checkpoint-only commit.
const FLOW_CHECKPOINT_LAG: u64 = 10_000;
const FLOW_CHECKPOINT_MAX_AGE_MS: i64 = 5_000;
const FLOW_FRONTIER_PERSIST_MS: i64 = 5_000;

pub struct FlowSubsystem {
	flow_scope: ActorSpawner,
	loader_handle: Mutex<Option<LoaderHandle>>,
	committer_handle: Mutex<Option<CommitterHandle>>,
	supervisor_handle: Mutex<Option<FlowSupervisorHandle>>,
	view_lineage: ViewLineage,
	health: FlowHealthRegistry,
	shutdown_state: FlowShutdownState,
	running: AtomicBool,
}

impl FlowSubsystem {
	pub fn publish_operator_catalog(config: &FlowConfig, engine: &StandardEngine) {
		Self::publish_custom_operators(&config.custom_operators, engine);
	}

	pub fn new(config: FlowConfig, engine: StandardEngine, ioc: &IocContainer) -> Result<Self> {
		Self::maybe_load_extern_operators(&config, &engine);

		let clock = ioc.resolve::<Clock>().expect("Clock must be registered");
		let spawner = ioc.resolve::<ActorSpawner>().expect("ActorSpawner must be registered");
		let custom_operators = config.custom_operators;
		let substrate = FlowSubstrate::with_dictionary(engine.dictionary_allocators(), engine.operator_state());
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
		let poll_frontier = CdcConsumerWatermark::default();
		let materialization = FlowMaterialization::new(poll_frontier.clone(), flow_tracker.clone());
		let committer =
			Committer::new(flow_tracker.clone(), materialization.clone(), substrate.operators.clone());
		let committer_handle =
			flow_scope.spawn_flow("flow-committer", CommitterActor::new(committer, group_commit));
		let committer_ref = committer_handle.actor_ref().clone();

		let health = FlowHealthRegistry::new();
		let operator_samples = OperatorSampleRegistry::new();
		let metrics_registry = ioc.resolve::<MetricsRegistry>().expect("MetricsRegistry must be registered");
		metrics_registry
			.register_operator_collector(Arc::new(OperatorSampleCollector::new(operator_samples.clone())));
		metrics_registry
			.register_operator_collector(Arc::new(RowNumberMetricsCollector::new(substrate.row.clone())));
		let view_lineage = engine.view_lineage();

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
				view_lineage: view_lineage.clone(),
				tracker: object_tracker.clone(),
				flow_tracker: flow_tracker.clone(),
				health: health.clone(),
				custom_operators: custom_operators.clone(),
				substrate: substrate.clone(),
				operator_samples: operator_samples.clone(),
				clock: clock.clone(),
				spawner: flow_scope.clone(),
				consumer_id: flow_consumer_id,
				pull_batch_bytes,
				load_batch_bytes,
				checkpoint_lag: FLOW_CHECKPOINT_LAG,
				checkpoint_max_age: Duration::from_milliseconds(FLOW_CHECKPOINT_MAX_AGE_MS).unwrap(),
				frontier_persist: Duration::from_milliseconds(FLOW_FRONTIER_PERSIST_MS).unwrap(),
			}),
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
		let bootstrap_flows = Self::bootstrap_flows(&engine);
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
			view_lineage,
			health,
			shutdown_state: FlowShutdownState::new(engine, substrate),
			running: AtomicBool::new(true),
		})
	}

	pub fn persist_frontiers(&self) {
		if self.is_running() {
			self.shutdown_state.persist_frontiers();
		}
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
	fn maybe_load_extern_operators(config: &FlowConfig, engine: &StandardEngine) {
		#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
		if let Some(ref operators_dir) = config.operators_dir {
			let event_bus = engine.event_bus();
			if let Err(e) = load_extern_rust_operators(operators_dir, event_bus) {
				panic!("Failed to load extern-Rust operators from {:?}: {}", operators_dir, e);
			}
			if let Err(e) = load_extern_c_operators(operators_dir, event_bus) {
				panic!("Failed to load extern-C operators from {:?}: {}", operators_dir, e);
			}
			event_bus.wait_for_completion();
		}
		#[cfg(not(all(reifydb_target = "host", not(reifydb_dst))))]
		{
			let _ = (config, engine);
		}
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
	fn bootstrap_flows(engine: &StandardEngine) -> Vec<FlowId> {
		let mut bootstrap_flows = Vec::new();
		if let Ok(mut query) = engine.begin_query(IdentityId::system()) {
			match engine.catalog().list_flows_all(&mut Transaction::Query(&mut query)) {
				Ok(existing_flows) => {
					bootstrap_flows.extend(existing_flows.into_iter().map(|existing| existing.id));
				}
				Err(e) => warn!(error = %e, "failed to list flows during bootstrap"),
			}
		}
		bootstrap_flows
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

		self.view_lineage.publish(Default::default());
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
