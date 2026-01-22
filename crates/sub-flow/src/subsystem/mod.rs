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
	util::ioc::IocContainer,
};
use reifydb_engine::{engine::StandardEngine, evaluate::column::StandardColumnEvaluator};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_type::Result;
use tracing::info;

use crate::{
	FlowEngine, builder::FlowBuilderConfig, coordinator::FlowCoordinator, lag::FlowLags,
	tracker::PrimitiveVersionTracker,
};

/// Flow subsystem - single-threaded flow processing.
pub struct FlowSubsystem {
	consumer: PollConsumer<StandardEngine, FlowCoordinator>,
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
		}

		let factory_builder = move || {
			let cat = catalog.clone();
			let exec = executor.clone();
			let bus = event_bus.clone();

			move || FlowEngine::new(cat, StandardColumnEvaluator::default(), exec, bus)
		};

		let primitive_tracker = Arc::new(PrimitiveVersionTracker::new());

		let cdc_store = ioc.resolve::<CdcStore>().expect("CdcStore must be registered");

		let num_workers = config.num_workers;
		info!(num_workers, "initializing flow coordinator with {} workers", num_workers);

		// Use the engine's actor runtime instead of creating a new one
		// This is critical for WASM where actors on different runtimes cannot communicate
		let runtime = engine.actor_runtime();

		let coordinator = FlowCoordinator::new(
			engine.clone(),
			primitive_tracker.clone(),
			num_workers,
			factory_builder,
			cdc_store.clone(),
			runtime.clone(),
		);

		// Register FlowLags with access to the flow catalog
		let catalog = coordinator.catalog();
		ioc.register_service::<Arc<dyn FlowLagsProvider>>(Arc::new(FlowLags::new(
			primitive_tracker,
			engine.clone(),
			catalog,
		)));

		let poll_config = PollConsumerConfig::new(
			CdcConsumerId::new("flow-coordinator"),
			"flow-cdc-poll",
			Duration::from_millis(10),
			Some(100),
		);

		// Pass the same shared runtime to PollConsumer so PollActor can communicate
		// with the coordinator and worker actors
		let consumer = PollConsumer::new(poll_config, engine, coordinator, cdc_store, runtime);

		Self {
			consumer,
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
			self.running = false;
		}
	}
}
