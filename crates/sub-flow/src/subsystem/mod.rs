// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod factory;

use std::{any::Any, path::PathBuf, sync::Arc};

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
	FlowEngine,
	lag::FlowLags,
	r#loop::{FlowLoop, FlowLoopConfig},
	operator::transform::registry::TransformOperatorRegistry,
	tracker::PrimitiveVersionTracker,
};

/// Flow subsystem - single-threaded flow processing.
pub struct FlowSubsystem {
	flow_loop: FlowLoop,
	running: bool,
}

impl FlowSubsystem {
	/// Create a new flow subsystem.
	pub fn new(
		engine: StandardEngine,
		operators_dir: Option<PathBuf>,
		ioc: &IocContainer,
		_runtime: Option<()>, // Ignored in single-threaded version
	) -> Self {
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

		let primitive_tracker = Arc::new(PrimitiveVersionTracker::new());

		// Create lag provider (simplified - uses primitive tracker + engine for checkpoints)
		let lags_provider = Arc::new(FlowLags::new_simple(primitive_tracker.clone(), engine.clone()));

		// Register in IoC for virtual table access
		ioc.register_service::<Arc<dyn FlowLagsProvider>>(lags_provider);

		let config = FlowLoopConfig::default();
		let flow_loop = FlowLoop::new(engine, Arc::new(flow_engine), primitive_tracker, config);

		Self {
			flow_loop,
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

		self.flow_loop.start()?;
		self.running = true;
		Ok(())
	}

	fn shutdown(&mut self) -> Result<()> {
		if !self.running {
			return Ok(());
		}

		self.flow_loop.stop()?;
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
