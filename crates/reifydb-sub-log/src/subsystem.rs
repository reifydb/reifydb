// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Logging subsystem implementation

use crate::backend::LogBackend;
use crate::buffer::LogBuffer;
use crate::processor::{LogProcessor, ProcessorConfig};
use crate::record::LogRecord;
use crossbeam_channel::{unbounded, Sender, Receiver};
use parking_lot::RwLock;
use reifydb_core::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Logging subsystem with dedicated thread
pub struct LoggingSubsystem {
	/// Log buffer for async collection
	buffer: Arc<LogBuffer>,
	/// Configured backends - using RwLock for safe concurrent access
	backends: Arc<RwLock<Vec<Box<dyn LogBackend>>>>,
	/// Processor configuration
	processor_config: ProcessorConfig,
	/// Log processor
	processor: Arc<LogProcessor>,
	/// Whether the subsystem is running
	running: Arc<AtomicBool>,
	/// Channel sender for receiving logs from the global logger
	log_sender: Sender<LogRecord>,
	/// Channel receiver for processing logs
	log_receiver: Arc<RwLock<Option<Receiver<LogRecord>>>>,
	/// Dedicated logging thread handle
	logging_thread: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl LoggingSubsystem {
	/// Create a new logging subsystem
	pub fn new(
		buffer_capacity: usize,
		backends: Vec<Box<dyn LogBackend>>,
		processor_config: ProcessorConfig,
	) -> Self {
		let buffer = Arc::new(LogBuffer::new(buffer_capacity));
		let backends = Arc::new(RwLock::new(backends));
		let processor = Arc::new(LogProcessor::new(
			Arc::clone(&buffer),
			Arc::clone(&backends),
			processor_config.clone(),
		));

		let (sender, receiver) = unbounded::<LogRecord>();
		let running = Arc::new(AtomicBool::new(false));

		Self {
			buffer,
			backends,
			processor_config,
			processor,
			running,
			log_sender: sender,
			log_receiver: Arc::new(RwLock::new(Some(receiver))),
			logging_thread: Arc::new(RwLock::new(None)),
		}
	}

	/// Get the sender channel for the global logger
	pub fn get_sender(&self) -> Sender<LogRecord> {
		self.log_sender.clone()
	}


	/// Flush all pending logs
	pub fn flush(&self) -> Result<()> {
		self.processor.flush()
	}

	/// Add a new backend
	pub fn add_backend(&self, backend: Box<dyn LogBackend>) {
		self.backends.write().push(backend);
	}

	/// Remove all backends
	pub fn clear_backends(&self) {
		self.backends.write().clear();
	}

	/// Get the number of buffered logs
	pub fn buffered_count(&self) -> usize {
		self.buffer.len()
	}

	/// Start the logging subsystem with dedicated thread
	pub fn start(&self) -> Result<()> {
		// Just set the running flag so the receiver thread buffers logs
		if self.running
			.compare_exchange(
				false,
				true,
				Ordering::AcqRel,
				Ordering::Acquire,
			)
			.is_err()
		{
			// Already running
			return Ok(());
		}
		
		// Take the receiver out to move it into the thread
		let receiver = self.log_receiver.write().take();
		if receiver.is_none() {
			// Receiver already taken, restore running flag
			self.running.store(false, Ordering::Release);
			// return Err(reifydb_core::Error::Other(
			// 	"Log receiver already in use".into()
			// ));

			panic!(
				"Log receiver already in use"
			);
		}
		let receiver = receiver.unwrap();
		
		// Clone references for the thread
		let buffer = Arc::clone(&self.buffer);
		let processor = Arc::clone(&self.processor);
		let running = Arc::clone(&self.running);
		let flush_interval = self.processor_config.flush_interval;
		
		// Spawn dedicated logging thread
		let handle = thread::Builder::new()
			.name("logging-thread".to_string())
			.spawn(move || {
				let mut last_process = std::time::Instant::now();
				
				while running.load(Ordering::Acquire) {
					// Drain channel into buffer
					let mut received_count = 0;
					while let Ok(record) = receiver.try_recv() {
						buffer.force_push(record);
						received_count += 1;
						// Process in batches to avoid blocking the channel
						if received_count >= 100 {
							break;
						}
					}
					
					// Process buffer to backends periodically
					let now = std::time::Instant::now();
					if now.duration_since(last_process) >= flush_interval || buffer.is_full() {
						let _ = processor.process_batch();
						last_process = now;
					}
					
					// Sleep briefly if no logs received to avoid busy waiting
					if received_count == 0 {
						thread::sleep(Duration::from_millis(1));
					}
				}
				
				// Final flush on shutdown
				let _ = processor.flush();
			})
			.expect("Failed to spawn logging thread");
		
		// Store the thread handle
		*self.logging_thread.write() = Some(handle);
		
		Ok(())
	}

	/// Stop the logging subsystem
	pub fn stop(&self) -> Result<()> {
		// Try to set running flag from true to false
		if self.running
			.compare_exchange(
				true,
				false,
				Ordering::AcqRel,
				Ordering::Acquire,
			)
			.is_err()
		{
			// Already stopped
			return Ok(());
		}

		// Wait for the logging thread to finish
		if let Some(handle) = self.logging_thread.write().take() {
			// Give the thread a moment to finish gracefully
			let _ = handle.join();
		}

		// Final flush to ensure all logs are written
		self.flush()?;

		Ok(())
	}

	/// Check if the subsystem is running
	pub fn is_running(&self) -> bool {
		self.running.load(Ordering::Acquire)
	}

	/// Get buffer utilization percentage
	pub fn buffer_utilization(&self) -> usize {
		let buffered = self.buffer.len();
		let capacity = self.buffer.capacity();
		(buffered * 100) / capacity.max(1)
	}

	/// Get total logs processed
	pub fn total_logs_processed(&self) -> u64 {
		self.buffer.total_processed()
	}

	/// Get total logs dropped
	pub fn total_logs_dropped(&self) -> u64 {
		self.buffer.total_dropped()
	}

	/// Get logging metrics
	pub fn metrics(&self) -> LoggingMetrics {
		LoggingMetrics {
			buffered_count: self.buffer.len(),
			buffer_capacity: self.buffer.capacity(),
			buffer_utilization: self.buffer_utilization(),
			total_processed: self.buffer.total_processed(),
			total_dropped: self.buffer.total_dropped(),
			is_running: self.is_running(),
		}
	}
}

/// Metrics for the logging subsystem
#[derive(Debug, Clone)]
pub struct LoggingMetrics {
	pub buffered_count: usize,
	pub buffer_capacity: usize,
	pub buffer_utilization: usize,
	pub total_processed: u64,
	pub total_dropped: u64,
	pub is_running: bool,
}

impl Drop for LoggingSubsystem {
	fn drop(&mut self) {
		// Stop the subsystem gracefully
		let _ = self.stop();
	}
}
