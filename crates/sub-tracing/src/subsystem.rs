// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	any::Any,
	io::{Write, stderr, stdout},
	sync::atomic::{AtomicBool, Ordering},
};

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_type::Result;
use tracing::{info, instrument};

pub struct TracingSubsystem {
	running: AtomicBool,
}

impl TracingSubsystem {
	#[instrument(name = "tracing::subsystem::new", level = "debug")]
	pub fn new() -> Self {
		Self {
			running: AtomicBool::new(false),
		}
	}
}

impl Default for TracingSubsystem {
	fn default() -> Self {
		Self::new()
	}
}

impl Subsystem for TracingSubsystem {
	fn name(&self) -> &'static str {
		"sub-tracing"
	}

	#[instrument(name = "tracing::subsystem::start", level = "debug", skip(self))]
	fn start(&mut self) -> Result<()> {
		self.running.store(true, Ordering::Release);

		info!("Tracing subsystem started");

		Ok(())
	}

	#[instrument(name = "tracing::subsystem::shutdown", level = "debug", skip(self))]
	fn shutdown(&mut self) -> Result<()> {
		if self.running.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
			return Ok(());
		}

		info!("Tracing subsystem shutting down");

		let _ = stdout().flush();
		let _ = stderr().flush();

		Ok(())
	}

	#[instrument(name = "tracing::subsystem::is_running", level = "trace", skip(self))]
	fn is_running(&self) -> bool {
		self.running.load(Ordering::Acquire)
	}

	#[instrument(name = "tracing::subsystem::health_status", level = "debug", skip(self))]
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
