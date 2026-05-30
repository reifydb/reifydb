// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	sync::atomic::{AtomicBool, Ordering},
};

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
use reifydb_runtime::{Runtime, actor::system::ActorSpawner, shutdown::Shutdown, sync::mutex::Mutex};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use tracing::info;

pub struct RuntimeSubsystem {
	running: AtomicBool,
	sampler_scope: Mutex<Option<ActorSpawner>>,
	runtime: Mutex<Option<Runtime>>,
}

impl RuntimeSubsystem {
	pub fn new(sampler_scope: Option<ActorSpawner>, runtime: Runtime) -> Self {
		info!("Runtime metrics subsystem started (history sampling={})", sampler_scope.is_some());
		Self {
			running: AtomicBool::new(true),
			sampler_scope: Mutex::new(sampler_scope),
			runtime: Mutex::new(Some(runtime)),
		}
	}
}

impl Shutdown for RuntimeSubsystem {
	fn shutdown(&self) {
		if self.running.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
			return;
		}
		info!("Runtime metrics subsystem shutting down");
		drop(self.sampler_scope.lock().take());
		if let Some(runtime) = self.runtime.lock().take() {
			runtime.shutdown();
		}
	}
}

impl Subsystem for RuntimeSubsystem {
	fn name(&self) -> &'static str {
		"sub-runtime"
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
