// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Tracing subsystem implementation
//!
//! This is a lightweight wrapper that integrates tracing_subscriber with the
//! ReifyDB subsystem architecture. The actual logging/tracing functionality
//! is handled by tracing_subscriber.

use std::{
	any::Any,
	sync::atomic::{AtomicBool, Ordering},
};

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_type::Result;
use tracing::instrument;

/// Tracing subsystem that integrates tracing_subscriber with ReifyDB
///
/// This subsystem acts as a thin wrapper around tracing_subscriber,
/// providing lifecycle management compatible with the ReifyDB subsystem
/// architecture. The actual log processing is handled by tracing_subscriber's
/// built-in mechanisms.
pub struct TracingSubsystem {
	/// Whether the subsystem is running
	running: AtomicBool,
}

impl TracingSubsystem {
	/// Create a new tracing subsystem
	///
	/// Note: The tracing subscriber should already be initialized before
	/// calling this. This is typically done in TracingBuilder::build().
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
		// Set running flag - tracing_subscriber is already initialized
		// by the builder
		self.running.store(true, Ordering::Release);

		tracing::info!("Tracing subsystem started");

		Ok(())
	}

	#[instrument(name = "tracing::subsystem::shutdown", level = "debug", skip(self))]
	fn shutdown(&mut self) -> Result<()> {
		if self.running.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
			// Already shutdown
			return Ok(());
		}

		tracing::info!("Tracing subsystem shutting down");

		// tracing_subscriber handles cleanup automatically when dropped
		// We just need to mark ourselves as not running

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
			name: env!("CARGO_PKG_NAME").strip_prefix("reifydb-").unwrap_or(env!("CARGO_PKG_NAME")).to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Tracing subsystem using tracing_subscriber".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}
