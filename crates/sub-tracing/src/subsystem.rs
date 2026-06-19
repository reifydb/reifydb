// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	io::{Write, stderr, stdout},
	sync::atomic::{AtomicBool, Ordering},
};

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
use reifydb_runtime::shutdown::Shutdown;
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use tracing::{info, instrument};

pub struct TracingSubsystem {
	running: AtomicBool,
}

impl TracingSubsystem {
	#[instrument(name = "tracing::subsystem::new", level = "info")]
	pub fn new() -> Self {
		info!("Tracing subsystem started");
		Self {
			running: AtomicBool::new(true),
		}
	}
}

impl Default for TracingSubsystem {
	fn default() -> Self {
		Self::new()
	}
}

impl Shutdown for TracingSubsystem {
	#[instrument(name = "tracing::subsystem::shutdown", level = "info", skip(self))]
	fn shutdown(&self) {
		if self.running.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
			return;
		}

		info!("Tracing subsystem shutting down");

		let _ = stdout().flush();
		let _ = stderr().flush();
	}
}

impl Subsystem for TracingSubsystem {
	fn name(&self) -> &'static str {
		"sub-tracing"
	}

	#[instrument(name = "tracing::subsystem::is_running", level = "trace", skip(self))]
	fn is_running(&self) -> bool {
		self.running.load(Ordering::Acquire)
	}

	#[instrument(name = "tracing::subsystem::health_status", level = "trace", skip(self))]
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

impl HasVersion for TracingSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Tracing subsystem using tracing_subscriber".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}
