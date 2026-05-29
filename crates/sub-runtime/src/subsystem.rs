// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	sync::atomic::{AtomicBool, Ordering},
};

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
use reifydb_runtime::{Runtime, actor::system::ActorSpawner};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_value::Result;
use tracing::info;

pub struct RuntimeSubsystem {
	running: AtomicBool,
	sampling: bool,
	sampler_scope: Option<ActorSpawner>,
	runtime: Option<Runtime>,
}

impl RuntimeSubsystem {
	pub fn new(sampler_scope: Option<ActorSpawner>, runtime: Runtime) -> Self {
		Self {
			running: AtomicBool::new(false),
			sampling: sampler_scope.is_some(),
			sampler_scope,
			runtime: Some(runtime),
		}
	}
}

impl Subsystem for RuntimeSubsystem {
	fn name(&self) -> &'static str {
		"sub-runtime"
	}

	fn start(&mut self) -> Result<()> {
		self.running.store(true, Ordering::Release);
		info!("Runtime metrics subsystem started (history sampling={})", self.sampling);
		Ok(())
	}

	fn shutdown(&mut self) -> Result<()> {
		if self.running.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
			return Ok(());
		}
		info!("Runtime metrics subsystem shutting down");
		drop(self.sampler_scope.take());
		if let Some(runtime) = self.runtime.take() {
			runtime.shutdown();
		}
		Ok(())
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::Acquire)
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

impl HasVersion for RuntimeSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Always-on runtime-metrics subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}
