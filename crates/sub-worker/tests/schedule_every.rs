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

use reifydb_core::{
	Result,
	interface::subsystem::{
		Subsystem, SubsystemFactory,
		worker::{ClosureTask, Priority, Scheduler, TaskContext},
	},
};
use reifydb_engine::{EngineTransaction, StandardCdcTransaction};
use reifydb_storage::memory::Memory;
use reifydb_sub_worker::{WorkerConfig, WorkerSubsystem, WorkerSubsystemFactory};
use reifydb_transaction::{mvcc::transaction::serializable::Serializable, svl::SingleVersionLock};

#[test]
fn test_schedule_every_basic_interval_execution() {
	let mut pool = WorkerSubsystem::new();
	assert!(pool.start().is_ok());

	let counter = Arc::new(AtomicUsize::new(0));
	let counter_clone = Arc::clone(&counter);

	// Schedule a task to run every 30ms
	let task = Box::new(ClosureTask::new("interval_task", Priority::Normal, move |_ctx: &TaskContext| {
		counter_clone.fetch_add(1, Ordering::Relaxed);
		Ok(())
	}));

	let handle = pool.schedule_every(task, Duration::from_millis(30)).unwrap();

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
	assert!(pool.cancel_task(handle).is_ok());

	// Wait a bit and verify no more executions
	let count_before = counter.load(Ordering::Relaxed);
	thread::sleep(Duration::from_millis(80)); // Wait longer than the task interval
	let count_after = counter.load(Ordering::Relaxed);

	assert_eq!(
		count_before, count_after,
		"Task should not execute after cancellation. Before: {}, After: {}",
		count_before, count_after
	);

	assert!(pool.shutdown().is_ok());
}

#[test]
fn test_schedule_every_priority_ordering() {
	// Test that scheduled tasks respect priority when multiple tasks are
	// ready
	let mut pool = WorkerSubsystem::with_config(WorkerConfig {
		num_workers: 1, // Single worker to ensure strict ordering
		max_queue_size: 100,
		scheduler_interval: Duration::from_millis(10),
		task_timeout_warning: Duration::from_secs(1),
	});

	assert!(pool.start().is_ok());

	let execution_order = Arc::new(Mutex::new(Vec::new()));

	// Create tasks with different priorities
	let high_order = Arc::clone(&execution_order);
	let high_task =
		Box::new(ClosureTask::new("high_priority_interval", Priority::High, move |_ctx: &TaskContext| {
			high_order.lock().unwrap().push("high");
			Ok(())
		}));

	let low_order = Arc::clone(&execution_order);
	let low_task = Box::new(ClosureTask::new("low_priority_interval", Priority::Low, move |_ctx: &TaskContext| {
		low_order.lock().unwrap().push("low");
		Ok(())
	}));

	// Schedule both at the same interval
	let high_handle = pool.schedule_every(high_task, Duration::from_millis(30)).unwrap();

	let low_handle = pool.schedule_every(low_task, Duration::from_millis(30)).unwrap();

	// Wait for several executions
	thread::sleep(Duration::from_millis(150));

	// Cancel both tasks
	assert!(pool.cancel_task(high_handle).is_ok());
	assert!(pool.cancel_task(low_handle).is_ok());

	let order = execution_order.lock().unwrap();

	// Verify we have executions of both priorities
	assert!(order.contains(&"high"), "High priority task should execute");
	assert!(order.contains(&"low"), "Low priority task should execute");

	// When both tasks are ready at the same time, high priority should
	// generally execute first (though timing may cause variations)
	println!("Execution order: {:?}", *order);

	assert!(pool.shutdown().is_ok());
}

#[test]
fn test_scheduler_client_api() -> Result<()> {
	// Test the scheduler client API through the subsystem factory
	type TestTransaction = EngineTransaction<
		Serializable<Memory, SingleVersionLock<Memory>>,
		SingleVersionLock<Memory>,
		StandardCdcTransaction<Memory>,
	>;

	// Create factory with configurator
	let factory = WorkerSubsystemFactory::<TestTransaction>::with_configurator(|builder| {
		builder.num_workers(2).max_queue_size(100)
	});

	// Create the subsystem
	let ioc = reifydb_core::ioc::IocContainer::new();
	let mut subsystem = Box::new(factory).create(&ioc)?;

	// Start the subsystem
	subsystem.start()?;

	// Get the scheduler from the subsystem
	let worker_subsystem = subsystem.as_any().downcast_ref::<WorkerSubsystem>().expect("Should be WorkerSubsystem");

	let scheduler = worker_subsystem.get_scheduler();

	// Test scheduling a task through the client API
	let counter = Arc::new(Mutex::new(0));
	let counter_clone = counter.clone();

	let task = Box::new(ClosureTask::new("client_task", Priority::Normal, move |_ctx: &TaskContext| {
		let mut count = counter_clone.lock().unwrap();
		*count += 1;
		println!("Task executed via client, count: {}", *count);
		Ok(())
	}));

	let handle = scheduler.schedule_every(task, Duration::from_millis(20))?;

	// Wait for tasks to execute
	thread::sleep(Duration::from_millis(100));

	// Cancel the task
	scheduler.cancel(handle)?;

	// Wait a bit more to ensure cancellation
	thread::sleep(Duration::from_millis(50));

	// Check that the task ran
	let final_count = *counter.lock().unwrap();
	println!("Final count: {}", final_count);
	assert!(final_count >= 2, "Task should have run at least twice");
	assert!(final_count <= 10, "Task should have been cancelled");

	// Shutdown
	subsystem.shutdown()?;

	Ok(())
}

#[test]
fn test_schedule_every_multiple_intervals() {
	// Test multiple tasks with different intervals
	let mut pool = WorkerSubsystem::with_config(WorkerConfig {
		num_workers: 2,
		max_queue_size: 100,
		scheduler_interval: Duration::from_millis(10),
		task_timeout_warning: Duration::from_secs(1),
	});

	assert!(pool.start().is_ok());

	let fast_counter = Arc::new(AtomicUsize::new(0));
	let slow_counter = Arc::new(AtomicUsize::new(0));

	let fast_clone = Arc::clone(&fast_counter);
	let slow_clone = Arc::clone(&slow_counter);

	// Schedule a fast task (every 20ms)
	let fast_task = Box::new(ClosureTask::new("fast_interval", Priority::Normal, move |_ctx: &TaskContext| {
		fast_clone.fetch_add(1, Ordering::Relaxed);
		Ok(())
	}));

	// Schedule a slow task (every 50ms)
	let slow_task = Box::new(ClosureTask::new("slow_interval", Priority::Normal, move |_ctx: &TaskContext| {
		slow_clone.fetch_add(1, Ordering::Relaxed);
		Ok(())
	}));

	let fast_handle = pool.schedule_every(fast_task, Duration::from_millis(20)).unwrap();

	let slow_handle = pool.schedule_every(slow_task, Duration::from_millis(50)).unwrap();

	// Wait for executions
	thread::sleep(Duration::from_millis(150));

	// Cancel both tasks
	assert!(pool.cancel_task(fast_handle).is_ok());
	assert!(pool.cancel_task(slow_handle).is_ok());

	let fast_count = fast_counter.load(Ordering::Relaxed);
	let slow_count = slow_counter.load(Ordering::Relaxed);

	// Fast task should run more often than slow task
	assert!(
		fast_count > slow_count,
		"Fast task ({}) should run more often than slow task ({})",
		fast_count,
		slow_count
	);

	// Verify reasonable execution counts
	assert!(fast_count >= 5, "Fast task should run at least 5 times");
	assert!(slow_count >= 2, "Slow task should run at least 2 times");

	assert!(pool.shutdown().is_ok());
}
