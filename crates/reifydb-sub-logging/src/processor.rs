// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Batch processor task for the worker pool

use crate::buffer::Buffer;
use parking_lot::RwLock;
use reifydb_core::interface::subsystem::logging::LogBackend;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for the log processor
#[derive(Debug, Clone)]
pub struct ProcessorConfig {
	/// Maximum batch size to process at once
	pub batch_size: usize,
	/// Interval between batch processing
	pub flush_interval: Duration,
	/// Whether to process immediately on high-priority logs
	pub immediate_on_error: bool,
}

impl Default for ProcessorConfig {
	fn default() -> Self {
		Self {
			batch_size: 1000,
			flush_interval: Duration::from_millis(100),
			immediate_on_error: true,
		}
	}
}

/// Log processor task that runs in the worker pool
pub struct LogProcessor {
	buffer: Arc<Buffer>,
	backends: Arc<RwLock<Vec<Box<dyn LogBackend>>>>,
	config: ProcessorConfig,
	last_flush: RwLock<Instant>,
}

impl LogProcessor {
	pub fn new(
		buffer: Arc<Buffer>,
		backends: Arc<RwLock<Vec<Box<dyn LogBackend>>>>,
		config: ProcessorConfig,
	) -> Self {
		Self {
			buffer,
			backends,
			config,
			last_flush: RwLock::new(Instant::now()),
		}
	}

	/// Process a batch of logs
	pub fn process_batch(&self) -> reifydb_core::Result<bool> {
		let now = Instant::now();
		let should_flush = {
			let last = *self.last_flush.read();
			now.duration_since(last) >= self.config.flush_interval
		};

		// Check if we should process
		if !should_flush && !self.buffer.is_full() {
			return Ok(true); // Continue periodic task
		}

		// Drain logs from buffer
		let records = self.buffer.drain(self.config.batch_size);
		if records.is_empty() {
			return Ok(true); // Continue periodic task
		}

		// Write to all backends
		let backends = self.backends.read();
		for backend in backends.iter() {
			// Ignore backend errors to prevent logging failures from affecting the system
			let _ = backend.write(&records);
		}

		// Update last flush time
		*self.last_flush.write() = now;

		Ok(true) // Continue periodic task
	}

	/// Force flush all pending logs
	pub fn flush(&self) -> reifydb_core::Result<()> {
		let records = self.buffer.drain_all();
		if records.is_empty() {
			return Ok(());
		}

		let backends = self.backends.read();
		for backend in backends.iter() {
			let _ = backend.write(&records);
			let _ = backend.flush();
		}

		*self.last_flush.write() = Instant::now();
		Ok(())
	}
}
