// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod factory;
mod ffi;

use std::{any::Any, sync::Arc, time::Duration};

use crate::builder::FlowBuilderConfig;
use crate::{
	FlowEngine, coordinator::FlowCoordinator, lag::FlowLags, pool::FlowWorkerPool, tracker::PrimitiveVersionTracker,
};
pub use factory::FlowSubsystemFactory;
use ffi::load_ffi_operators;
use reifydb_cdc::{CdcConsumer, PollConsumer, PollConsumerConfig};
use reifydb_core::{
	Result,
	interface::{
		CdcConsumerId, FlowLagsProvider, WithEventBus,
		version::{ComponentType, HasVersion, SystemVersion},
	},
	util::ioc::IocContainer,
};
use reifydb_engine::{StandardColumnEvaluator, StandardEngine};
use reifydb_sub_api::{HealthStatus, Subsystem};
use tracing::info;

/// Flow subsystem - single-threaded flow processing.
pub struct FlowSubsystem {
	consumer: PollConsumer<FlowCoordinator>,
	running: bool,
}

impl FlowSubsystem {
	/// Create a new flow subsystem.
	pub(crate) fn new(config: FlowBuilderConfig, engine: StandardEngine, ioc: &IocContainer) -> Self {
		let catalog = engine.catalog();
		let executor = engine.executor();
		let event_bus = engine.event_bus().clone();

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

		let num_workers = config.num_workers;
		info!(num_workers, "initializing flow worker pool");

		let worker_pool = FlowWorkerPool::new(num_workers, factory_builder, engine.clone(), engine.catalog());

		let coordinator = FlowCoordinator::new(engine.clone(), primitive_tracker.clone(), worker_pool);

		// Register FlowLags with access to the flow catalog
		let catalog = coordinator.catalog.clone();
		ioc.register_service::<Arc<dyn FlowLagsProvider>>(Arc::new(FlowLags::new(
			primitive_tracker,
			engine.clone(),
			catalog,
		)));

		let poll_config = PollConsumerConfig::new(
			CdcConsumerId::new("flow-coordinator"),
			"flow-cdc-poll",
			Duration::from_micros(100),
			Some(10),
		);

		let consumer = PollConsumer::new(poll_config, engine, coordinator);

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
