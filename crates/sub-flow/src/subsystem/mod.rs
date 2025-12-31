// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod factory;

use std::{any::Any, path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
pub use factory::FlowSubsystemFactory;
use reifydb_core::{
	Result,
	interface::{
		FlowLagsProvider, WithEventBus,
		version::{ComponentType, HasVersion, SystemVersion},
	},
	util::ioc::IocContainer,
};
use reifydb_engine::{StandardColumnEvaluator, StandardEngine};
use reifydb_sub_api::{HealthStatus, Subsystem};

use crate::{
	FlowEngine, coordinator::Coordinator, lag::FlowLagsV2,
	operator::transform::registry::TransformOperatorRegistry, registry::FlowConsumerRegistry,
	tracker::PrimitiveVersionTracker,
};

/// Flow subsystem - greenfield rewrite with independent per-flow consumers.
pub struct FlowSubsystem {
	coordinator: Coordinator,
	running: bool,
}

impl FlowSubsystem {
	/// Create a new flow subsystem.
	pub fn new(engine: StandardEngine, operators_dir: Option<PathBuf>, ioc: &IocContainer) -> Self {
		// Create operator registry
		let operator_registry = TransformOperatorRegistry::new();

		// Create FlowEngine
		let flow_engine = FlowEngine::new(
			engine.catalog(),
			StandardColumnEvaluator::default(),
			engine.executor(),
			operator_registry,
			engine.event_bus().clone(),
			operators_dir,
		);

		let registry = Arc::new(FlowConsumerRegistry::new());
		let primitive_tracker = Arc::new(PrimitiveVersionTracker::new());

		// Create lag provider
		let lags_provider =
			Arc::new(FlowLagsV2::new(registry.clone(), primitive_tracker.clone(), engine.clone()));

		// Register in IoC for virtual table access
		ioc.register_service::<Arc<dyn FlowLagsProvider>>(lags_provider);

		let coordinator = Coordinator::new(engine, Arc::new(flow_engine), registry, primitive_tracker);

		Self {
			coordinator,
			running: false,
		}
	}
}

#[async_trait]
impl Subsystem for FlowSubsystem {
	fn name(&self) -> &'static str {
		"sub-flow"
	}

	async fn start(&mut self) -> Result<()> {
		if self.running {
			return Ok(());
		}

		self.coordinator.start()?;
		self.running = true;
		Ok(())
	}

	async fn shutdown(&mut self) -> Result<()> {
		if !self.running {
			return Ok(());
		}

		// Use 30 second timeout for graceful shutdown
		let timeout = Duration::from_secs(30);
		self.coordinator.shutdown(timeout).await;

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
