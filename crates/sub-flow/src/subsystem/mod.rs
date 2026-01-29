// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod factory;
#[cfg(reifydb_target = "native")]
pub mod ffi;

use std::{any::Any, sync::Arc, time::Duration};

#[cfg(reifydb_target = "native")]
use ffi::load_ffi_operators;
use reifydb_cdc::{
	consume::{
		consumer::CdcConsumer,
		poll::{PollConsumer, PollConsumerConfig},
	},
	storage::CdcStore,
};
use reifydb_core::{
	interface::{
		WithEventBus,
		cdc::CdcConsumerId,
		flow::FlowLagsProvider,
		version::{ComponentType, HasVersion, SystemVersion},
	},
	key::{EncodableKey, cdc_consumer::CdcConsumerKey},
	util::ioc::IocContainer,
};
use reifydb_engine::{engine::StandardEngine, evaluate::column::StandardColumnEvaluator};
use reifydb_runtime::{SharedRuntime, actor::system::ActorHandle};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_type::Result;
use tracing::info;

use crate::{
	FlowEngine,
	builder::FlowBuilderConfig,
	catalog::FlowCatalog,
	coordinator::{CoordinatorActor, CoordinatorMsg, FlowConsumeRef},
	lag::FlowLags,
	pool::{PoolActor, PoolMsg},
	tracker::PrimitiveVersionTracker,
	worker::{FlowMsg, FlowWorkerActor},
};

/// Flow subsystem - single-threaded flow processing.
pub struct FlowSubsystem {
	consumer: PollConsumer<StandardEngine, FlowConsumeRef>,
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

			move || FlowEngine::new(cat, StandardColumnEvaluator::default(), exec, bus, clk)
		};

		let primitive_tracker = Arc::new(PrimitiveVersionTracker::new());

		let cdc_store = ioc.resolve::<CdcStore>().expect("CdcStore must be registered");

		let num_workers = config.num_workers;
		info!(num_workers, "initializing flow coordinator with {} workers", num_workers);

		let actor_system = engine.actor_system();

		// Spawn worker
		let mut worker_refs = Vec::with_capacity(num_workers);
		let mut worker_handles = Vec::with_capacity(num_workers);
		for i in 0..num_workers {
			let worker_factory = factory_builder();
			let worker = FlowWorkerActor::new(worker_factory, engine.clone(), engine.catalog());
			let handle = actor_system.spawn(&format!("flow-worker-{}", i), worker);
			worker_refs.push(handle.actor_ref().clone());
			worker_handles.push(handle);
		}

		// Spawn pool
		let pool = PoolActor::new(worker_refs, clock.clone());
		let pool_handle = actor_system.spawn("flow-pool", pool);
		let pool_ref = pool_handle.actor_ref().clone();

		let flow_catalog = FlowCatalog::new(engine.catalog());

		// Spawn coordinator actor
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

		// Create the thin CdcConsume impl
		let consumer_id = CdcConsumerId::new("flow-coordinator");
		let consumer_key = CdcConsumerKey {
			consumer: consumer_id.clone(),
		}
		.encode();
		let consume_ref = FlowConsumeRef {
			actor_ref,
			consumer_key,
		};

		// Register FlowLags with access to the flow catalog
		ioc.register_service::<Arc<dyn FlowLagsProvider>>(Arc::new(FlowLags::new(
			primitive_tracker,
			engine.clone(),
			flow_catalog,
		)));

		let poll_config =
			PollConsumerConfig::new(consumer_id, "flow-cdc-poll", Duration::from_millis(10), Some(100));

		let consumer = PollConsumer::new(poll_config, engine, consume_ref, cdc_store, actor_system);

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

		// Stop the poll consumer first (signals PollActor to stop)
		self.consumer.stop()?;

		// Join coordinator (it sends messages to pool and workers)
		if let Some(handle) = self.coordinator_handle.take() {
			let _ = handle.join();
		}

		// Join pool (it sends messages to workers)
		if let Some(handle) = self.pool_handle.take() {
			let _ = handle.join();
		}

		// Join workers last
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
			name: "sub-flow".to_string(),
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
