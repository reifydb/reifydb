// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Tests for the schedule_every functionality in the worker subsystem

use std::{
	sync::{
		Arc, Mutex,
		atomic::{AtomicUsize, Ordering},
	},
	thread,
	time::Duration,
};

use reifydb_catalog::MaterializedCatalog;
use reifydb_core::{event::EventBus, interceptor::StandardInterceptorFactory};
use reifydb_engine::StandardEngine;
use reifydb_store_transaction::TransactionStore;
use reifydb_sub_api::{ClosureTask, Priority, Scheduler, Subsystem};
use reifydb_sub_worker::{WorkerConfig, WorkerSubsystem};
use reifydb_transaction::{cdc::TransactionCdc, multi::TransactionMultiVersion, single::TransactionSingleVersion};
use reifydb_type::{diagnostic::internal, error};

fn create_test_engine() -> StandardEngine {
	let store = TransactionStore::testing_memory();
	let eventbus = EventBus::new();
	let single = TransactionSingleVersion::svl(store.clone(), eventbus.clone());
	let cdc = TransactionCdc::new(store.clone());
	let multi = TransactionMultiVersion::serializable(store, single.clone(), eventbus.clone());

	StandardEngine::new(
		multi,
		single,
		cdc,
		eventbus,
		Box::new(StandardInterceptorFactory::default()),
		MaterializedCatalog::new(),
	)
}

#[test]
fn test_schedule_every_basic_interval_execution() {
	let engine = create_test_engine();
	let mut instance = WorkerSubsystem::with_config_and_engine(WorkerConfig::default(), engine);
	assert!(instance.start().is_ok());

	let counter = Arc::new(AtomicUsize::new(0));
	let counter_clone = Arc::clone(&counter);

	// Schedule a task to run every 30ms
	let task = Box::new(ClosureTask::new("interval_task", Priority::Normal, move |_ctx| {
		counter_clone.fetch_add(1, Ordering::Relaxed);
		Ok(())
	}));

	let handle = instance.schedule_every(Duration::from_millis(30), task).unwrap();

	// Wait for executions with retry logic
	let mut attempts = 0;
	let max_attempts = 20; // 20 * 10ms = 200ms max wait
	while counter.load(Ordering::Relaxed) < 3 && attempts < max_attempts {
		thread::sleep(Duration::from_millis(10));
		attempts += 1;
	}

	// Should have executed at least 3 times
	let count = counter.load(Ordering::Relaxed);
	assert!(count >= 3, "Expected at least 3 executions, got {} after {} attempts", count, attempts);

	// Cancel the task
	assert!(instance.cancel(handle).is_ok());

	// Give time for any in-flight tasks to finish
	thread::sleep(Duration::from_millis(50));

	let count_after_cancel = counter.load(Ordering::Relaxed);

	// Wait a bit more to ensure no more executions
	thread::sleep(Duration::from_millis(100));

	// Should not have executed more after cancellation
	assert_eq!(counter.load(Ordering::Relaxed), count_after_cancel, "Task should not execute after cancellation");

	assert!(instance.shutdown().is_ok());
}

#[test]
fn test_schedule_every_priority_ordering() {
	// Tests that high priority intervals get executed before low priority when
	// ready
	let engine = create_test_engine();
	let mut instance = WorkerSubsystem::with_config_and_engine(
		WorkerConfig {
			num_workers: 1, // Single worker to ensure strict ordering
			max_queue_size: 100,
			scheduler_interval: Duration::from_millis(10),
			task_timeout_warning: Duration::from_secs(1),
		},
		engine,
	);

	assert!(instance.start().is_ok());

	let execution_order = Arc::new(Mutex::new(Vec::new()));

	// Create tasks with different priorities
	let high_order = Arc::clone(&execution_order);
	let high_task = Box::new(ClosureTask::new("high_priority_interval", Priority::High, move |_ctx| {
		high_order.lock().unwrap().push("high");
		Ok(())
	}));

	let low_order = Arc::clone(&execution_order);
	let low_task = Box::new(ClosureTask::new("low_priority_interval", Priority::Low, move |_ctx| {
		low_order.lock().unwrap().push("low");
		Ok(())
	}));

	// Schedule both with the same interval
	let _high_handle = instance.schedule_every(Duration::from_millis(50), high_task).unwrap();
	let _low_handle = instance.schedule_every(Duration::from_millis(50), low_task).unwrap();

	// Wait for a few executions
	thread::sleep(Duration::from_millis(200));

	let order = execution_order.lock().unwrap();

	// Both should have executed
	assert!(!order.is_empty(), "Tasks should have executed");

	// When both are ready at the same time, high should come before low
	// Check the first few executions
	if order.len() >= 2 {
		// Find first occurrence of each
		let first_high = order.iter().position(|s| *s == "high");
		let first_low = order.iter().position(|s| *s == "low");

		if let (Some(high_pos), Some(low_pos)) = (first_high, first_low) {
			// If they were ready at the same time (close positions), high
			// should come first
			if (high_pos as isize - low_pos as isize).abs() == 1 {
				assert!(
					high_pos < low_pos,
					"High priority should execute before low when both are ready"
				);
			}
		}
	}

	assert!(instance.shutdown().is_ok());
}

#[test]
fn test_schedule_every_cancellation() {
	let engine = create_test_engine();
	let mut instance = WorkerSubsystem::with_config_and_engine(WorkerConfig::default(), engine);
	assert!(instance.start().is_ok());

	let counter = Arc::new(AtomicUsize::new(0));
	let counter_clone = Arc::clone(&counter);

	// Schedule a task
	let task = Box::new(ClosureTask::new("test_task", Priority::Normal, move |_ctx| {
		counter_clone.fetch_add(1, Ordering::Relaxed);
		Ok(())
	}));

	let handle = instance.schedule_every(Duration::from_millis(20), task).unwrap();

	// Let it run a few times
	thread::sleep(Duration::from_millis(100));
	let count_before_cancel = counter.load(Ordering::Relaxed);
	assert!(count_before_cancel > 0, "Task should have executed at least once");

	// Cancel the task
	assert!(instance.cancel(handle).is_ok());

	// Wait to ensure no more executions
	thread::sleep(Duration::from_millis(100));
	let count_after_cancel = counter.load(Ordering::Relaxed);

	// Should be the same or at most one more (if one was in flight)
	assert!(count_after_cancel <= count_before_cancel + 1, "Task should stop executing after cancellation");

	assert!(instance.shutdown().is_ok());
}

#[test]
fn test_schedule_every_multiple_intervals() {
	let engine = create_test_engine();
	let mut instance = WorkerSubsystem::with_config_and_engine(
		WorkerConfig {
			num_workers: 2,
			max_queue_size: 100,
			scheduler_interval: Duration::from_millis(10),
			task_timeout_warning: Duration::from_secs(1),
		},
		engine,
	);
	assert!(instance.start().is_ok());

	// Schedule two tasks at different intervals
	let counter1 = Arc::new(AtomicUsize::new(0));
	let counter2 = Arc::new(AtomicUsize::new(0));

	let counter1_clone = Arc::clone(&counter1);
	let task1 = Box::new(ClosureTask::new("high_priority_interval", Priority::High, move |_ctx| {
		counter1_clone.fetch_add(1, Ordering::Relaxed);
		Ok(())
	}));

	let counter2_clone = Arc::clone(&counter2);
	let task2 = Box::new(ClosureTask::new("normal_priority_interval", Priority::Normal, move |_ctx| {
		counter2_clone.fetch_add(1, Ordering::Relaxed);
		Ok(())
	}));

	// Schedule at different rates
	let _handle1 = instance.schedule_every(Duration::from_millis(30), task1).unwrap();
	let _handle2 = instance.schedule_every(Duration::from_millis(60), task2).unwrap();

	// Wait for executions
	thread::sleep(Duration::from_millis(200));

	let count1 = counter1.load(Ordering::Relaxed);
	let count2 = counter2.load(Ordering::Relaxed);

	// Task1 (30ms interval) should execute roughly twice as often as Task2
	// (60ms)
	assert!(count1 > 0, "Task1 should have executed");
	assert!(count2 > 0, "Task2 should have executed");

	// Rough check - task1 should execute more frequently
	// Allow for timing variance
	assert!(
		count1 >= count2,
		"Task1 (30ms) should execute at least as often as Task2 (60ms). Count1: {}, Count2: {}",
		count1,
		count2
	);

	assert!(instance.shutdown().is_ok());
}

#[test]
fn test_schedule_every_task_failure_recovery() {
	let engine = create_test_engine();
	let mut instance = WorkerSubsystem::with_config_and_engine(WorkerConfig::default(), engine);
	assert!(instance.start().is_ok());

	let counter = Arc::new(AtomicUsize::new(0));
	let counter_clone = Arc::clone(&counter);

	// Schedule a task that fails on even executions
	let task = Box::new(ClosureTask::new("failing_task", Priority::Normal, move |_ctx| {
		let count = counter_clone.fetch_add(1, Ordering::Relaxed);
		if count % 2 == 0 {
			Ok(())
		} else {
			Err(error!(internal("test error")))
		}
	}));

	let _handle = instance.schedule_every(Duration::from_millis(30), task).unwrap();

	// Wait for several executions
	thread::sleep(Duration::from_millis(200));

	// Should continue executing despite failures
	let final_count = counter.load(Ordering::Relaxed);
	assert!(final_count >= 3, "Task should continue executing despite failures. Count: {}", final_count);

	assert!(instance.shutdown().is_ok());
}
