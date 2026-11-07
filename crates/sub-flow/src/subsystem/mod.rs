// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
pub mod extern_c;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
pub mod extern_rust;
pub mod factory;
pub mod shutdown;
#[cfg(test)]
mod tests;

use std::{
	any::Any,
	mem::take,
	path::PathBuf,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread,
};

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use extern_c::load_extern_c_operators;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use extern_rust::load_extern_rust_operators;
use reifydb_core::{
	commit::{
		loader::{CommitLoader, CommitLoaderHandle, CommitLoaderMetrics},
		queue::{CommitQueue, FlowQueueFloor},
		watermark::{FlowCaughtUpWatermark, ReaderWatermark},
	},
	event::operator::OperatorLoadedEvent,
	interface::{
		WithEventBus,
		catalog::config::{ConfigKey, GetConfig},
		flow::{FlowTickSampler, FlowUnitRow, FlowUnitSampler, FlowWatermarkSampler},
		version::{ComponentType, HasVersion, SystemVersion},
	},
	lifecycle::watermark::ConsumerPositions,
	marker::LatencyConsumer,
	metrics::registry::MetricsRegistry,
	util::ioc::IocContainer,
};
use reifydb_engine::{engine::StandardEngine, flow_check::FlowRegistrationCheckRef, vm::flow_lineage::ViewLineage};
use reifydb_flow_async::{
	operator::metrics::{OperatorSampleCollector, OperatorSampleRegistry},
	transaction::substrate::FlowSubstrate,
};
use reifydb_runtime::{
	actor::system::ActorSpawner,
	context::clock::Clock,
	shutdown::{Shutdown, ShutdownErrors},
	sync::mutex::Mutex,
};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_transaction::{
	commit::{CommitBegin, CommitHandle},
	wal::flow_wal,
};
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	value::{duration::Duration, identity::IdentityId},
};

use crate::{
	builder::{CustomOperators, FlowConfig},
	check::FlowRegistrationProbe,
	commit::{
		committer::{Committer, CommitterActor, CommitterHandle},
		quiescence::FlowMaterialization,
	},
	control::{
		health::{FlowHealthRegistry, PoisonedFlowsCollector, stored_poisoned},
		supervisor::{FlowSupervisor, FlowSupervisorHandle, FlowSupervisorMessage, FlowSupervisorParams},
	},
	marker::MarkerPlane,
	progress::{
		frontier::ControlFrontier,
		tracker::{FlowPositionTracker, ObjectVersionTracker},
		watermark::compute_flow_watermarks,
	},
	subsystem::shutdown::FlowShutdownState,
};

const FLOW_CHECKPOINT_LAG: u64 = 10_000;
const FLOW_CHECKPOINT_MAX_AGE_MS: i64 = 5_000;
const FLOW_FRONTIER_PERSIST_MS: i64 = 5_000;

pub struct FlowSubsystem {
	flow_scope: ActorSpawner,
	loader_handle: Mutex<Option<CommitLoaderHandle>>,
	committer_handles: Mutex<Vec<CommitterHandle>>,
	supervisor_handle: Mutex<Option<FlowSupervisorHandle>>,
	view_lineage: ViewLineage,
	health: FlowHealthRegistry,
	engine: StandardEngine,
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
		let flow_wal = flow_wal(engine.multi().wal());

		let flow_scope = spawner.scope();

		let commit_handle = ioc.try_resolve::<CommitHandle>().unwrap_or_else(|| {
			let begin_engine = engine.clone();
			let begin: CommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
			CommitHandle::new(begin)
		});
		let poll_frontier = ReaderWatermark::default();
		let materialization = FlowMaterialization::new(poll_frontier.clone(), flow_tracker.clone());
		let markers = ioc.try_resolve::<LatencyConsumer>().map(|_| MarkerPlane::register(ioc));
		let committer = Committer::new(
			flow_tracker.clone(),
			materialization.clone(),
			substrate.operators.clone().expect("the flow substrate is built with an operator store"),
		)
		.with_markers(markers.clone());
		let shards = engine.catalog().get_config_uint2(ConfigKey::FlowCommitterShards).max(1) as usize;
		let mut committer_handles = Vec::with_capacity(shards * 2);
		let mut committer_refs = Vec::with_capacity(shards);
		let mut terminal_committer_refs = Vec::with_capacity(shards);
		for _ in 0..shards {
			let handle = flow_scope.spawn_flow(
				"flow-committer",
				CommitterActor::new(committer.clone(), commit_handle.clone()),
			);
			committer_refs.push(handle.actor_ref().clone());
			committer_handles.push(handle);
			let terminal = CommitterActor::new(committer.clone(), commit_handle.clone())
				.named("flow-committer-terminal");
			let handle = flow_scope.spawn_flow("flow-committer-terminal", terminal);
			terminal_committer_refs.push(handle.actor_ref().clone());
			committer_handles.push(handle);
		}

		let health = FlowHealthRegistry::new();
		let operator_samples = OperatorSampleRegistry::new();
		let metrics_registry = ioc.resolve::<MetricsRegistry>().expect("MetricsRegistry must be registered");
		metrics_registry
			.register_operator_collector(Arc::new(OperatorSampleCollector::new(operator_samples.clone())));
		let view_lineage = engine.view_lineage();

		let queue = ioc.resolve::<CommitQueue>().expect("CommitQueue must be registered");
		let FlowQueueFloor(queue_floor) =
			ioc.resolve::<FlowQueueFloor>().expect("FlowQueueFloor must be registered");
		let loader_metrics = CommitLoaderMetrics::default();
		metrics_registry.register_collector(Arc::new(loader_metrics.clone()));
		metrics_registry.register_collector(Arc::new(health.clone()));
		metrics_registry.register_collector(Arc::new(PoisonedFlowsCollector::new(engine.clone())));
		let control = ControlFrontier::new();
		ioc.register_service::<ControlFrontier>(control.clone());
		let loader_handle =
			flow_scope.spawn_flow("flow-loader", CommitLoader::new(flow_wal.log.clone(), loader_metrics));
		let pull_batch_bytes =
			ByteSize::from_bytes(engine.catalog().get_config_uint8(ConfigKey::FlowPullBatchBytes));
		let load_batch_bytes =
			ByteSize::from_bytes(engine.catalog().get_config_uint8(ConfigKey::FlowLoadBatchBytes));

		let supervisor_handle = flow_scope.spawn_flow(
			"flow-supervisor",
			FlowSupervisor::new(FlowSupervisorParams {
				engine: engine.clone(),
				committers: committer_refs,
				terminal_committers: terminal_committer_refs,
				queue: queue.clone(),
				queue_floor,
				loader: loader_handle.actor_ref().clone(),
				log: flow_wal.log.clone(),
				floor: flow_wal.floor.clone(),
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
				pull_batch_bytes,
				load_batch_bytes,
				checkpoint_lag: FLOW_CHECKPOINT_LAG,
				checkpoint_max_age: Duration::from_milliseconds(FLOW_CHECKPOINT_MAX_AGE_MS).unwrap(),
				frontier_persist: Duration::from_milliseconds(FLOW_FRONTIER_PERSIST_MS).unwrap(),
				markers: markers.map(|plane| plane.map().clone()),
			}),
		);

		Self::register_watermark_sampler(ioc, &engine, &object_tracker, &flow_tracker, &materialization);
		Self::register_unit_sampler(ioc, &flow_tracker);
		Self::register_tick_sampler(ioc, &flow_tracker);

		let poisoned_engine = engine.clone();
		let stalled_health = health.clone();
		ioc.register_service::<FlowCaughtUpWatermark>(FlowCaughtUpWatermark::new(
			move || materialization.caught_up(),
			move || stored_poisoned(&poisoned_engine),
			move || stalled_health.stalled(),
		));

		ioc.register_service::<Arc<dyn ConsumerPositions>>(Arc::new(flow_tracker.clone()));
		ioc.register_service::<FlowRegistrationCheckRef>(Arc::new(FlowRegistrationProbe {
			engine: engine.clone(),
			custom_operators: custom_operators.clone(),
			substrate: substrate.clone(),
		}));

		let (scan_from, scan_lease) = engine.acquire_current_snapshot_lease()?;
		if supervisor_handle
			.actor_ref()
			.send(FlowSupervisorMessage::Bootstrap {
				scan_from,
				scan_lease,
			})
			.is_err()
		{
			panic!("flow supervisor refused its bootstrap message");
		}

		let _ = supervisor_handle.actor_ref().send(FlowSupervisorMessage::Wake);

		Ok(Self {
			flow_scope,
			loader_handle: Mutex::new(Some(loader_handle)),
			committer_handles: Mutex::new(committer_handles),
			supervisor_handle: Mutex::new(Some(supervisor_handle)),
			view_lineage,
			health,
			engine: engine.clone(),
			shutdown_state: FlowShutdownState::new(engine, substrate),
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
				entry.abi,
				entry.version.clone(),
				entry.description.clone(),
				entry.input.clone(),
				entry.output.clone(),
				entry.capabilities,
				entry.class,
				entry.window,
				entry.unmanaged_because.clone(),
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
	fn register_unit_sampler(ioc: &IocContainer, flow_tracker: &FlowPositionTracker) {
		let flow_tracker = flow_tracker.clone();
		ioc.register_service::<FlowUnitSampler>(FlowUnitSampler::new(move || {
			flow_tracker
				.units()
				.into_iter()
				.map(|(flow_id, unit)| FlowUnitRow {
					flow_id,
					unit,
				})
				.collect()
		}));
	}

	#[inline]
	fn register_tick_sampler(ioc: &IocContainer, flow_tracker: &FlowPositionTracker) {
		let flow_tracker = flow_tracker.clone();
		ioc.register_service::<FlowTickSampler>(FlowTickSampler::new(move || flow_tracker.ticks()));
	}

	#[inline]
	fn register_watermark_sampler(
		ioc: &IocContainer,
		engine: &StandardEngine,
		object_tracker: &ObjectVersionTracker,
		flow_tracker: &FlowPositionTracker,
		materialization: &FlowMaterialization,
	) {
		ioc.register_service::<FlowWatermarkSampler>(FlowWatermarkSampler::new({
			let engine = engine.clone();
			let tracker = object_tracker.clone();
			let flow_tracker = flow_tracker.clone();
			let materialization = materialization.clone();
			move || {
				compute_flow_watermarks(&tracker, &flow_tracker, || {
					engine.done_until().max(materialization.output_frontier())
				})
			}
		}));
	}
}

impl Shutdown for FlowSubsystem {
	fn shutdown(&self) -> Result<()> {
		if self.running.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
			return Ok(());
		}

		let mut errors = ShutdownErrors::new("flow");
		self.flow_scope.shutdown();

		if let Some(handle) = self.supervisor_handle.lock().take()
			&& let Err(err) = handle.join()
		{
			errors.fail("supervisor", err);
		}

		if let Some(handle) = self.loader_handle.lock().take()
			&& let Err(err) = handle.join()
		{
			errors.fail("loader", err);
		}

		for handle in take(&mut *self.committer_handles.lock()) {
			if let Err(err) = handle.join() {
				errors.fail("committer", err);
			}
		}

		self.shutdown_state.persist_frontiers();
		self.view_lineage.publish(Default::default());
		errors.finish()
	}
}

impl Subsystem for FlowSubsystem {
	fn name(&self) -> &'static str {
		"flow"
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::Acquire)
	}

	fn health_status(&self) -> HealthStatus {
		if !self.is_running() {
			return HealthStatus::Unknown;
		}
		let poisoned = stored_poisoned(&self.engine)
			.unwrap_or_else(|e| panic!("flow subsystem health cannot list the poisoned flows: {e}"));
		let stalled = self.health.stalled();
		if poisoned.is_empty() && stalled.is_empty() {
			return HealthStatus::Healthy;
		}
		let mut problems: Vec<String> = Vec::new();
		if !poisoned.is_empty() {
			let flows: Vec<String> =
				poisoned.iter().map(|(id, reason)| format!("flow {}: {}", id.0, reason)).collect();
			problems.push(format!("{} deferred flow(s) poisoned: {}", poisoned.len(), flows.join("; ")));
		}
		if !stalled.is_empty() {
			let flows: Vec<String> = stalled.iter().map(|id| format!("flow {}", id.0)).collect();
			problems.push(format!(
				"{} deferred flow(s) stalled with input pending: {}",
				stalled.len(),
				flows.join("; ")
			));
		}
		HealthStatus::Degraded {
			description: problems.join("; "),
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
		if let Err(err) = self.shutdown() {
			if thread::panicking() {
				eprintln!("flow shutdown failed during unwind: {err}");
			} else {
				panic!("flow shutdown failed: {err}");
			}
		}
	}
}
