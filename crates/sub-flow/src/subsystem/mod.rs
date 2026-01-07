// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod factory;

use std::{any::Any, path::PathBuf, sync::Arc, time::Duration};

pub use factory::FlowSubsystemFactory;
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

use crate::{
	FlowEngine, coordinator::FlowCoordinator, lag::FlowLags,
	operator::transform::registry::TransformOperatorRegistry, pool::FlowWorkerPool,
	tracker::PrimitiveVersionTracker,
};

/// Flow subsystem - single-threaded flow processing.
pub struct FlowSubsystem {
	consumer: PollConsumer<FlowCoordinator>,
	running: bool,
}

impl FlowSubsystem {
	/// Create a new flow subsystem.
	pub fn new(
		engine: StandardEngine,
		operators_dir: Option<PathBuf>,
		num_workers: Option<usize>,
		ioc: &IocContainer,
	) -> Self {
		let operator_registry = TransformOperatorRegistry::new();

		let flow_engine = Arc::new(FlowEngine::new(
			engine.catalog(),
			StandardColumnEvaluator::default(),
			engine.executor(),
			operator_registry,
			engine.event_bus().clone(),
			operators_dir,
		));

		let primitive_tracker = Arc::new(PrimitiveVersionTracker::new());

		ioc.register_service::<Arc<dyn FlowLagsProvider>>(Arc::new(FlowLags::new_simple(
			primitive_tracker.clone(),
			flow_engine.clone(),
			engine.clone(),
		)));

		let num_workers = num_workers.unwrap_or(1);
		info!(num_workers, "initializing flow worker pool");

		let worker_pool =
			FlowWorkerPool::new(num_workers, flow_engine.clone(), engine.clone(), engine.catalog());

		let coordinator = FlowCoordinator::new(engine.clone(), flow_engine, primitive_tracker, worker_pool);

		let poll_config = PollConsumerConfig::new(
			CdcConsumerId::new("flow-coordinator"),
			Duration::from_micros(100),
			Some(50),
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
			// Best effort - can't await in Drop
			self.running = false;
		}
	}
}
