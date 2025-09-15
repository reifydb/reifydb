// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Builder pattern for configuring the worker pool subsystem

use std::time::Duration;

use crate::WorkerConfig;

/// Builder for configuring the worker pool subsystem
pub struct WorkerBuilder {
	num_workers: usize,
	max_queue_size: usize,
	scheduler_interval: Duration,
	task_timeout_warning: Duration,
}

impl Default for WorkerBuilder {
	fn default() -> Self {
		Self::new()
	}
}

impl WorkerBuilder {
	/// Create a new WorkerPoolBuilder with default settings
	pub fn new() -> Self {
		Self {
			num_workers: 1,
			max_queue_size: 10000,
			scheduler_interval: Duration::from_millis(10),
			task_timeout_warning: Duration::from_secs(30),
		}
	}

	/// Set the number of worker threads
	///
	/// Default: 1
	pub fn num_workers(mut self, workers: usize) -> Self {
		self.num_workers = workers.max(1);
		self
	}

	/// Set the maximum number of queued tasks
	///
	/// Default: 10000
	pub fn max_queue_size(mut self, size: usize) -> Self {
		self.max_queue_size = size.max(1);
		self
	}

	/// Set how often to check for periodic tasks
	///
	/// Default: 10ms
	pub fn scheduler_interval(mut self, interval: Duration) -> Self {
		self.scheduler_interval = interval;
		self
	}

	/// Set the maximum time a task can run before warning
	///
	/// Default: 30 seconds
	pub fn task_timeout_warning(mut self, timeout: Duration) -> Self {
		self.task_timeout_warning = timeout;
		self
	}

	/// Build the worker pool configuration
	pub fn build(self) -> WorkerConfig {
		WorkerConfig {
			num_workers: self.num_workers,
			max_queue_size: self.max_queue_size,
			scheduler_interval: self.scheduler_interval,
			task_timeout_warning: self.task_timeout_warning,
		}
	}
}
