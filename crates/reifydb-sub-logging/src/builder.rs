// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Builder pattern for configuring the logging subsystem

use crate::backend::console::ConsoleBackend;
use crate::processor::ProcessorConfig;
use crate::subsystem::LoggingSubsystem;
use reifydb_core::interface::subsystem::logging::{init_logger, LogBackend};
use std::time::Duration;

/// Builder for configuring the logging subsystem
pub struct LoggingBuilder {
	backends: Vec<Box<dyn LogBackend>>,
	buffer_capacity: usize,
	batch_size: usize,
	flush_interval: Duration,
	immediate_on_error: bool,
}

impl LoggingBuilder {
	/// Create a new logging builder with default settings
	pub fn new() -> Self {
		Self {
			backends: Vec::new(),
			buffer_capacity: 10000,
			batch_size: 1000,
			flush_interval: Duration::from_millis(100),
			immediate_on_error: true,
		}
	}

	/// Add a logging backend
	pub fn add_backend(mut self, backend: Box<dyn LogBackend>) -> Self {
		self.backends.push(backend);
		self
	}

	/// Add a console backend with default settings
	pub fn with_console(self) -> Self {
		self.add_backend(Box::new(ConsoleBackend::new()))
	}

	/// Add a console backend with custom settings
	pub fn with_console_custom(
		self,
		use_color: bool,
		stderr_for_errors: bool,
	) -> Self {
		self.add_backend(Box::new(
			ConsoleBackend::new()
				.with_color(use_color)
				.with_stderr_for_errors(stderr_for_errors),
		))
	}

	pub fn buffer_capacity(mut self, capacity: usize) -> Self {
		self.buffer_capacity = capacity;
		self
	}

	pub fn batch_size(mut self, size: usize) -> Self {
		self.batch_size = size;
		self
	}

	pub fn flush_interval(mut self, interval: Duration) -> Self {
		self.flush_interval = interval;
		self
	}

	pub fn immediate_on_error(mut self, immediate: bool) -> Self {
		self.immediate_on_error = immediate;
		self
	}

	pub(crate) fn build(self) -> LoggingSubsystem {
		// If no backends configured, add console by default
		let backends = if self.backends.is_empty() {
			vec![Box::new(ConsoleBackend::new())
				as Box<dyn LogBackend>]
		} else {
			self.backends
		};

		let processor_config = ProcessorConfig {
			batch_size: self.batch_size,
			flush_interval: self.flush_interval,
			immediate_on_error: self.immediate_on_error,
		};

		let subsystem = LoggingSubsystem::new(
			self.buffer_capacity,
			backends,
			processor_config,
		);

		init_logger(subsystem.get_sender());

		subsystem
	}
}

impl Default for LoggingBuilder {
	fn default() -> Self {
		Self::new()
	}
}
