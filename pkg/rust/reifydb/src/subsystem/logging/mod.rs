// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Logging subsystem wrapper that implements the Subsystem trait

mod factory;
pub use factory::LoggingSubsystemFactory;

use crate::health::HealthStatus;
use crate::subsystem::Subsystem;
use reifydb_core::Result;
use reifydb_sub_log::{LoggingBuilder, LoggingSubsystem as InnerLogging};
use std::any::Any;
use std::sync::Arc;

/// Wrapper for LoggingSubsystem that implements the Subsystem trait
pub struct LoggingSubsystem {
	inner: Arc<InnerLogging>,
}

impl LoggingSubsystem {
	/// Create a new logging subsystem from a builder
	pub fn from_builder(builder: LoggingBuilder) -> Self {
		let inner = builder.build();

		// Initialize the global logger with the sender from the subsystem
		reifydb_sub_log::init_logger(inner.get_sender());

		Self {
			inner,
		}
	}

	/// Create with default configuration
	pub fn new() -> Self {
		Self::from_builder(LoggingBuilder::new().with_console())
	}

	/// Get the inner logging subsystem
	pub fn inner(&self) -> &Arc<InnerLogging> {
		&self.inner
	}
}

impl Default for LoggingSubsystem {
	fn default() -> Self {
		Self::new()
	}
}

impl Subsystem for LoggingSubsystem {
	fn name(&self) -> &'static str {
		"Logging"
	}

	fn start(&mut self) -> Result<()> {
		// Start the logging subsystem with its dedicated thread
		self.inner.start()?;
		Ok(())
	}

	fn stop(&mut self) -> Result<()> {
		self.inner.stop()
	}

	fn is_running(&self) -> bool {
		self.inner.is_running()
	}

	fn health_status(&self) -> HealthStatus {
		if !self.is_running() {
			return HealthStatus::Unknown;
		}

		let utilization = self.inner.buffer_utilization();

		if utilization >= 90 {
			HealthStatus::Degraded {
				description: format!(
					"Log buffer is {}% full",
					utilization
				),
			}
		} else {
			HealthStatus::Healthy
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}
	
	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}
}
