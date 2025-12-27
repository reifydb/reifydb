// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Greenfield rewrite of the sub-flow subsystem to eliminate race conditions.
//!
//! This module provides a simpler architecture with two main components:
//! - **Coordinator**: Monitors CDC for flow creation and spawns flow consumers
//! - **Flow Consumer**: Per-flow CDC consumer that processes source changes from earliest CDC version
//!
//! Key differences from the original implementation:
//! - Each flow has its own independent CDC consumer (N+1 consumers vs 1)
//! - Flow consumers start from CommitVersion(1) and catch up
//! - No backfilling (historical data processed via CDC)
//! - Not restart-safe (coordinator doesn't persist spawned flows)
//! - Simpler lifecycle management (only handles creation, not deletion/modification)

mod coordinator;
mod flow;
mod lags;
mod registry;
mod tracker;

use std::{any::Any, path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
pub use coordinator::Coordinator;
pub use flow::FlowConsumer;
pub use lags::FlowLagsV2;
pub use registry::FlowConsumerRegistry;
use reifydb_core::{
	Result,
	interface::{
		FlowLagsProvider, WithEventBus,
		version::{ComponentType, HasVersion, SystemVersion},
	},
	util::ioc::IocContainer,
};
use reifydb_engine::{StandardEngine, StandardRowEvaluator};
use reifydb_sub_api::{HealthStatus, Subsystem};
pub use tracker::PrimitiveVersionTracker;

use crate::{FlowEngine, operator::transform::registry::TransformOperatorRegistry};

/// Flow subsystem V2 - greenfield rewrite with independent per-flow consumers.
pub struct FlowSubsystemV2 {
	coordinator: Coordinator,
	running: bool,
}

impl FlowSubsystemV2 {
	/// Create a new flow subsystem.
	pub fn new(engine: StandardEngine, operators_dir: Option<PathBuf>, ioc: &IocContainer) -> Self {
		// Create operator registry
		let operator_registry = TransformOperatorRegistry::new();

		// Create FlowEngine
		let flow_engine = FlowEngine::new(
			StandardRowEvaluator::default(),
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
impl Subsystem for FlowSubsystemV2 {
	fn name(&self) -> &'static str {
		"sub-flow-v2"
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

impl HasVersion for FlowSubsystemV2 {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "sub-flow-v2".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Data flow and stream processing subsystem (V2 rewrite)".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

impl Drop for FlowSubsystemV2 {
	fn drop(&mut self) {
		if self.running {
			// Best effort - can't await in Drop
			self.running = false;
		}
	}
}
