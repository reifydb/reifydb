// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicUsize, Ordering},
	},
	thread,
	time::{Duration, Instant},
};

use LogLevel::Info;
use parking_lot::Mutex;
use reifydb_core::{
	interface::subsystem::{
		Subsystem,
		logging::{LogBackend, LogLevel, Record, log},
	},
	result::error::diagnostic::internal,
};
use reifydb_sub_logging::LoggingBuilder;
use reifydb_testing::util::wait::wait_for;

#[derive(Clone, Debug)]
struct MockBackend {
	logs: Arc<Mutex<Vec<Record>>>,
	write_count: Arc<AtomicUsize>,
	flush_count: Arc<AtomicUsize>,
	fail_on_write: Arc<AtomicBool>,
}

impl MockBackend {
	fn new() -> Self {
		Self {
			logs: Arc::new(Mutex::new(Vec::new())),
			write_count: Arc::new(AtomicUsize::new(0)),
			flush_count: Arc::new(AtomicUsize::new(0)),
			fail_on_write: Arc::new(AtomicBool::new(false)),
		}
	}

	fn get_logs(&self) -> Vec<Record> {
		self.logs.lock().clone()
	}

	fn set_fail_on_write(&self, fail: bool) {
		self.fail_on_write.store(fail, Ordering::Relaxed);
	}
}

impl LogBackend for MockBackend {
	fn name(&self) -> &str {
		"mock"
	}

	fn write(&self, records: &[Record]) -> reifydb_core::Result<()> {
		if self.fail_on_write.load(Ordering::Relaxed) {
			return Err(reifydb_core::Error(internal(
				"Mock write failure",
			)));
		}

		let mut logs = self.logs.lock();
		logs.extend_from_slice(records);
		self.write_count.fetch_add(1, Ordering::Relaxed);
		Ok(())
	}

	fn flush(&self) -> reifydb_core::Result<()> {
		self.flush_count.fetch_add(1, Ordering::Relaxed);
		Ok(())
	}
}

#[test]
fn test_subsystem_lifecycle() {
	// This test doesn't use logging, so it can use the regular build
	let mut subsystem = LoggingBuilder::new().buffer_capacity(100).build();

	// Test initial state
	assert!(!subsystem.is_running());
	assert_eq!(subsystem.name(), "Logging");

	// Start subsystem
	subsystem.start().expect("Failed to start");
	assert!(subsystem.is_running());

	// Double start should be idempotent
	subsystem.start().expect("Double start should succeed");
	assert!(subsystem.is_running());

	// Shutdown subsystem
	subsystem.shutdown().expect("Failed to shutdown");
	assert!(!subsystem.is_running());

	// Double shutdown should be idempotent
	subsystem.shutdown().expect("Double shutdown should succeed");
	assert!(!subsystem.is_running());
}

#[test]
fn test_basic_logging() {
	let backend = MockBackend::new();
	let backend_clone = backend.clone();

	let (mut subsystem, _handle) = LoggingBuilder::new()
		.with_backend(Box::new(backend))
		.buffer_capacity(100)
		.batch_size(10)
		.flush_interval(Duration::from_millis(50))
		.build_for_test();

	subsystem.start().expect("Failed to start");

	// Send some logs
	for i in 0..5 {
		log(Record::new(Info, "test", format!("Test log {}", i)));
	}

	wait_for(
		|| backend_clone.get_logs().len() == 5,
		"Should receive 5 logs",
	);

	subsystem.shutdown().expect("Failed to shutdown");
}

#[test]
fn test_buffer_overflow_handling() {
	let backend = MockBackend::new();
	let backend_clone = backend.clone();

	let (mut subsystem, _handle) = LoggingBuilder::new()
		.with_backend(Box::new(backend))
		.buffer_capacity(10) // Small buffer
		.batch_size(5)
		.flush_interval(Duration::from_secs(10)) // Long interval
		.build_for_test();

	subsystem.start().expect("Failed to start");

	// Send more logs than buffer capacity
	for i in 0..20 {
		log(Record::new(Info, "test", format!("Overflow test {}", i)));
	}

	// Wait for logs to be buffered
	wait_for(|| subsystem.buffered_count() > 0, "Logs should be buffered");

	// Force flush
	subsystem.flush().expect("Failed to flush");

	let logs = backend_clone.get_logs();
	// Some logs may be dropped due to buffer overflow
	assert!(logs.len() <= 20);

	// Check metrics
	let metrics = subsystem.metrics();
	assert!(metrics.total_dropped > 0 || logs.len() == 20);

	subsystem.shutdown().expect("Failed to shutdown");
}

#[test]
fn test_concurrent_logging() {
	let backend = MockBackend::new();
	let backend_clone = backend.clone();

	let (mut subsystem, handle) = LoggingBuilder::new()
		.with_backend(Box::new(backend))
		.buffer_capacity(1000)
		.batch_size(100)
		.flush_interval(Duration::from_millis(50))
		.build_for_test();

	subsystem.start().expect("Failed to start");

	// Get the sender to share with threads
	let sender = handle.sender().clone();

	// Spawn multiple threads logging concurrently
	let handles: Vec<_> = (0..5)
		.map(|thread_id| {
			let thread_sender = sender.clone();
			thread::spawn(move || {
				// Set the mock logger for this thread
				use reifydb_core::interface::subsystem::logging::mock::set_mock_logger;
				set_mock_logger(thread_sender);

				for i in 0..20 {
					log(Record::new(
						Info,
						"test",
						format!(
							"Thread {} log {}",
							thread_id, i
						),
					));
					thread::sleep(Duration::from_micros(
						100,
					));
				}
			})
		})
		.collect();

	// Wait for all threads
	for handle in handles {
		handle.join().unwrap();
	}

	// Flush and check
	subsystem.flush().expect("Failed to flush");

	// Wait for all logs to be processed
	wait_for(
		|| backend_clone.get_logs().len() == 100,
		"Should receive all 100 logs from 5 threads",
	);

	let logs = backend_clone.get_logs();
	assert_eq!(logs.len(), 100); // 5 threads * 20 logs

	subsystem.shutdown().expect("Failed to shutdown");
}

#[test]
fn test_backend_failure_resilience() {
	let backend = MockBackend::new();
	let backend_clone = backend.clone();

	let (mut subsystem, _handle) = LoggingBuilder::new()
		.with_backend(Box::new(backend))
		.buffer_capacity(100)
		.batch_size(10)
		.flush_interval(Duration::from_millis(50))
		.build_for_test();

	subsystem.start().expect("Failed to start");

	// Send logs normally and wait for them to be processed
	for i in 0..5 {
		log(Record::new(Info, "test", format!("Before failure {}", i)));
	}

	// Wait for the first batch to be processed
	wait_for(
		|| backend_clone.get_logs().len() >= 5,
		"Should process initial logs before failure",
	);
	
	let logs_before_failure = backend_clone.get_logs().len();

	// Make backend fail
	backend_clone.set_fail_on_write(true);

	// These logs should not crash the system (they will be buffered internally)
	for i in 0..5 {
		log(Record::new(
			LogLevel::Error,
			"test",
			format!("During failure {}", i),
		));
	}

	// Give some time for failed writes to be attempted
	thread::sleep(Duration::from_millis(100));

	// Re-enable backend
	backend_clone.set_fail_on_write(false);

	// Should resume normal operation
	for i in 0..5 {
		log(Record::new(Info, "test", format!("After failure {}", i)));
	}

	// Force a flush to ensure all buffered logs are processed
	subsystem.flush().expect("Failed to flush");

	// Wait for logs after recovery to be written
	// We should have the initial logs plus the recovery logs (not the failed ones)
	wait_for(
		|| backend_clone.get_logs().len() >= logs_before_failure + 5,
		"Should have processed recovery logs",
	);

	let final_logs = backend_clone.get_logs();
	// We should have at least 10 logs (5 before + 5 after, failed logs are dropped)
	assert!(
		final_logs.len() >= 10,
		"Expected at least 10 logs, got {}",
		final_logs.len()
	);

	subsystem.shutdown().expect("Failed to shutdown");
}

#[test]
fn test_dynamic_backend_management() {
	let backend1 = MockBackend::new();
	let backend2 = MockBackend::new();
	let backend1_clone = backend1.clone();
	let backend2_clone = backend2.clone();

	let (mut subsystem, _handle) = LoggingBuilder::new()
		.with_backend(Box::new(backend1))
		.buffer_capacity(100)
		.flush_interval(Duration::from_millis(50))
		.build_for_test();

	subsystem.start().expect("Failed to start");

	// Log with first backend
	for i in 0..5 {
		log(Record::new(Info, "test", format!("First backend {}", i)));
	}

	// Wait for logs to be buffered before flushing
	wait_for(|| subsystem.buffered_count() > 0, "Logs should be buffered");

	subsystem.flush().expect("Failed to flush");
	wait_for(
		|| backend1_clone.get_logs().len() == 5,
		"First backend should have 5 logs",
	);

	// Add second backend
	subsystem.add_backend(Box::new(backend2));

	// Log with both backends
	for i in 0..5 {
		log(Record::new(Info, "test", format!("Both backends {}", i)));
	}

	// Wait for logs to be buffered
	wait_for(|| subsystem.buffered_count() > 0, "Logs should be buffered");

	subsystem.flush().expect("Failed to flush");
	wait_for(
		|| backend1_clone.get_logs().len() == 10,
		"First backend should have 10 logs",
	);
	wait_for(
		|| backend2_clone.get_logs().len() == 5,
		"Second backend should have 5 logs",
	);

	// Clear all backends
	subsystem.clear_backends();

	// Logs should still be accepted but not written anywhere
	for i in 0..5 {
		log(Record::new(Info, "test", format!("No backends {}", i)));
	}

	subsystem.flush().expect("Failed to flush");

	// Backend counts should not change
	assert_eq!(backend1_clone.get_logs().len(), 10);
	assert_eq!(backend2_clone.get_logs().len(), 5);

	subsystem.shutdown().expect("Failed to shutdown");
}

#[test]
fn test_flush_on_shutdown() {
	let backend = MockBackend::new();
	let backend_clone = backend.clone();

	let (mut subsystem, _handle) = LoggingBuilder::new()
		.with_backend(Box::new(backend))
		.buffer_capacity(100)
		.flush_interval(Duration::from_secs(10)) // Long interval
		.build_for_test();

	subsystem.start().expect("Failed to start");

	// Send logs
	for i in 0..10 {
		log(Record::new(
			Info,
			"test",
			format!("Shutdown flush test {}", i),
		));
	}

	// Wait for ALL logs to be buffered (not just some)
	// This ensures the logging thread has received all 10 logs
	wait_for(
		|| subsystem.buffered_count() == 10,
		"All 10 logs should be buffered before shutdown",
	);

	// Shutdown immediately (before flush interval)
	subsystem.shutdown().expect("Failed to shutdown");

	// All logs should be flushed on shutdown
	wait_for(
		|| backend_clone.get_logs().len() == 10,
		"All 10 logs should be flushed on shutdown",
	);

	let logs = backend_clone.get_logs();
	assert_eq!(logs.len(), 10);
}

#[test]
fn test_performance_under_load() {
	let backend = MockBackend::new();

	let (mut subsystem, _handle) = LoggingBuilder::new()
		.with_backend(Box::new(backend))
		.buffer_capacity(10000)
		.batch_size(1000)
		.flush_interval(Duration::from_millis(100))
		.build_for_test();

	subsystem.start().expect("Failed to start");

	let start = Instant::now();

	// Send many logs quickly
	for i in 0..10000 {
		log(Record::new(Info, "test", format!("Load test {}", i)));
	}

	let send_duration = start.elapsed();

	// Sending should be fast (< 100ms for 10k logs)
	assert!(
		send_duration < Duration::from_millis(100),
		"Sending took {:?}",
		send_duration
	);

	// Wait for processing
	thread::sleep(Duration::from_millis(500));

	let metrics = subsystem.metrics();
	assert!(metrics.total_processed >= 9000); // Most should be processed

	subsystem.shutdown().expect("Failed to shutdown");
}

// Removed test_restart_subsystem since subsystems are no longer restartable
// after shutdown

#[test]
fn test_different_log_levels() {
	let backend = MockBackend::new();
	let backend_clone = backend.clone();

	let (mut subsystem, _handle) = LoggingBuilder::new()
		.with_backend(Box::new(backend))
		.buffer_capacity(100)
		.level(LogLevel::Trace)
		.build_for_test();

	subsystem.start().expect("Failed to start");

	// Send logs of different levels
	log(Record::new(LogLevel::Trace, "test", "Trace message"));
	log(Record::new(LogLevel::Debug, "test", "Debug message"));
	log(Record::new(Info, "test", "Info message"));
	log(Record::new(LogLevel::Warn, "test", "Warn message"));
	log(Record::new(LogLevel::Error, "test", "Error message"));

	// Wait for logs to be buffered
	wait_for(
		|| subsystem.buffered_count() >= 5,
		"Should buffer 5 logs of different levels",
	);

	subsystem.flush().expect("Failed to flush");

	let logs = backend_clone.get_logs();
	assert_eq!(logs.len(), 5);

	// Verify all levels are present
	let levels: Vec<_> = logs.iter().map(|r| r.level).collect();
	assert!(levels.contains(&LogLevel::Trace));
	assert!(levels.contains(&LogLevel::Debug));
	assert!(levels.contains(&Info));
	assert!(levels.contains(&LogLevel::Warn));
	assert!(levels.contains(&LogLevel::Error));

	subsystem.shutdown().expect("Failed to shutdown");
}
