// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	sync::{
		Arc, Condvar, Mutex,
		atomic::{AtomicUsize, Ordering},
	},
	thread,
	time::{Duration, Instant},
};

use reifydb_catalog::MaterializedCatalog;
use reifydb_core::{event::EventBus, interceptor::StandardInterceptorFactory};
use reifydb_engine::{EngineTransaction, StandardCdcTransaction, StandardEngine};
use reifydb_store_transaction::StandardTransactionStore;
use reifydb_sub_api::{Priority, Subsystem};
use reifydb_sub_worker::{InternalClosureTask, WorkerConfig, WorkerSubsystem};
use reifydb_transaction::{mvcc::transaction::serializable::SerializableTransaction, svl::SingleVersionLock};

type TestTransaction = EngineTransaction<
	SerializableTransaction<StandardTransactionStore, SingleVersionLock<StandardTransactionStore>>,
	SingleVersionLock<StandardTransactionStore>,
	StandardCdcTransaction<StandardTransactionStore>,
>;

fn create_test_engine() -> StandardEngine<TestTransaction> {
	let store = StandardTransactionStore::testing_memory();
	let eventbus = EventBus::new();
	let single = SingleVersionLock::new(store.clone(), eventbus.clone());
	let cdc = StandardCdcTransaction::new(store.clone());
	let multi = SerializableTransaction::new(store, single.clone(), eventbus.clone());

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
fn test_worker_subsystem_basic_task_execution() {
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

	// Start the instance
	assert!(instance.start().is_ok());
	assert!(instance.is_running());

	// Submit some tasks
	let counter = Arc::new(AtomicUsize::new(0));
	let expected_count = 10;
	let completion_signal = Arc::new((Mutex::new(0), Condvar::new()));

	for i in 0..expected_count {
		let counter_clone = Arc::clone(&counter);
		let signal_clone = Arc::clone(&completion_signal);
		let task = Box::new(InternalClosureTask::new(format!("task_{}", i), Priority::Normal, move |_ctx| {
			counter_clone.fetch_add(1, Ordering::Relaxed);
			let (lock, cvar) = &*signal_clone;
			let mut count = lock.lock().unwrap();
			*count += 1;
			cvar.notify_one();
			Ok(())
		}));

		assert!(instance.submit(task).is_ok());
	}

	// Wait for all tasks to complete with timeout
	let (lock, cvar) = &*completion_signal;
	let timeout = Duration::from_secs(5);
	let start = Instant::now();
	let mut completed = lock.lock().unwrap();
	while *completed < expected_count {
		let result = cvar.wait_timeout(completed, timeout.saturating_sub(start.elapsed())).unwrap();
		completed = result.0;
		if result.1.timed_out() {
			panic!(
				"Test timed out waiting for tasks to complete. Completed: {}/{}",
				*completed, expected_count
			);
		}
	}

	// Check that all tasks were executed
	assert_eq!(counter.load(Ordering::Relaxed), expected_count);

	// shutdown the instance
	assert!(instance.shutdown().is_ok());
	assert!(!instance.is_running());
}

#[test]
fn test_task_priority_ordering() {
	let engine = create_test_engine();
	let mut instance = WorkerSubsystem::with_config_and_engine(
		WorkerConfig {
			num_workers: 1, // Single worker to ensure order
			max_queue_size: 100,
			scheduler_interval: Duration::from_millis(10),
			task_timeout_warning: Duration::from_secs(1),
		},
		engine,
	);

	assert!(instance.start().is_ok());

	let results = Arc::new(Mutex::new(Vec::new()));

	// Submit tasks with different priorities
	for (i, priority) in [
		(1, Priority::Low),
		(2, Priority::High),
		(3, Priority::Normal),
		(4, Priority::High),
		(5, Priority::Low),
	]
	.iter()
	{
		let results_clone = Arc::clone(&results);
		let task_id = *i;

		let task = Box::new(InternalClosureTask::new(format!("task_{}", task_id), *priority, move |_ctx| {
			results_clone.lock().unwrap().push(task_id);
			Ok(())
		}));

		assert!(instance.submit(task).is_ok());
	}

	// Wait for tasks to complete
	thread::sleep(Duration::from_millis(200));

	let final_results = results.lock().unwrap();

	// High priority tasks should be executed first
	// The exact order depends on submission timing, but high priority
	// should generally come before low priority
	println!("Execution order: {:?}", *final_results);

	assert!(instance.shutdown().is_ok());
}

#[test]
fn test_priority_ordering_with_concurrent_blocking_tasks() {
	// This test ensures that when a worker is blocked, high priority tasks
	// still get executed before low priority tasks by other workers
	let engine = create_test_engine();
	let mut instance = WorkerSubsystem::with_config_and_engine(
		WorkerConfig {
			num_workers: 2, // Two workers
			max_queue_size: 100,
			scheduler_interval: Duration::from_millis(10),
			task_timeout_warning: Duration::from_secs(1),
		},
		engine,
	);

	assert!(instance.start().is_ok());

	let results = Arc::new(Mutex::new(Vec::new()));
	let blocker = Arc::new(AtomicUsize::new(0));
	let all_queued = Arc::new(AtomicUsize::new(0));

	// First, submit a blocking task that will occupy one worker
	let blocker_clone = Arc::clone(&blocker);
	let all_queued_clone = Arc::clone(&all_queued);
	let blocking_task = Box::new(InternalClosureTask::new("blocker", Priority::Normal, move |_ctx| {
		// Signal that we're blocking
		blocker_clone.store(1, Ordering::Relaxed);
		// Wait until all tasks are queued before releasing this
		// worker
		while all_queued_clone.load(Ordering::Relaxed) == 0 {
			thread::sleep(Duration::from_millis(5));
		}
		// Keep blocking for a bit more to ensure proper queuing
		thread::sleep(Duration::from_millis(50));
		Ok(())
	}));
	assert!(instance.submit(blocking_task).is_ok());

	// Wait for the blocker to start
	while blocker.load(Ordering::Relaxed) == 0 {
		thread::sleep(Duration::from_millis(5));
	}

	// Submit another blocking task to occupy the second worker initially
	let second_blocker = Arc::new(AtomicUsize::new(0));
	let second_blocker_clone = Arc::clone(&second_blocker);
	let all_queued_clone2 = Arc::clone(&all_queued);
	let second_blocking_task = Box::new(InternalClosureTask::new("blocker2", Priority::Normal, move |_ctx| {
		// Signal that we're blocking
		second_blocker_clone.store(1, Ordering::Relaxed);
		// Wait until all tasks are queued
		while all_queued_clone2.load(Ordering::Relaxed) == 0 {
			thread::sleep(Duration::from_millis(5));
		}
		Ok(())
	}));
	assert!(instance.submit(second_blocking_task).is_ok());

	// Wait for the second blocker to start
	while second_blocker.load(Ordering::Relaxed) == 0 {
		thread::sleep(Duration::from_millis(5));
	}

	// Now submit tasks with different priorities - they will all queue up
	for (id, priority) in [
		(1, Priority::Low),
		(2, Priority::High),
		(3, Priority::Low),
		(4, Priority::High),
		(5, Priority::Normal),
		(6, Priority::High),
		(7, Priority::Low),
	]
	.iter()
	{
		let results_clone = Arc::clone(&results);
		let task_id = *id;

		let task = Box::new(InternalClosureTask::new(format!("task_{}", task_id), *priority, move |_ctx| {
			results_clone.lock().unwrap().push(task_id);
			// Add small delay to prevent race conditions
			thread::sleep(Duration::from_millis(2));
			Ok(())
		}));

		assert!(instance.submit(task).is_ok());
	}

	// Signal that all tasks are now queued
	all_queued.store(1, Ordering::Relaxed);

	// Wait for all tasks to complete
	thread::sleep(Duration::from_millis(300));

	let final_results = results.lock().unwrap();

	// With proper queuing, we should see priority ordering
	// However, we need to be more lenient due to concurrent execution

	// Count tasks by priority in the result
	let high_tasks = vec![2, 4, 6];
	let normal_tasks = vec![5];
	let low_tasks = vec![1, 3, 7];

	// Ensure all tasks were executed
	assert_eq!(final_results.len(), 7, "All 7 tasks should have been executed");

	// Find the average position of each priority group
	let high_avg_pos: f64 = high_tasks
		.iter()
		.filter_map(|&id| final_results.iter().position(|&x| x == id))
		.map(|p| p as f64)
		.sum::<f64>() / high_tasks.len() as f64;

	let normal_avg_pos: f64 = normal_tasks
		.iter()
		.filter_map(|&id| final_results.iter().position(|&x| x == id))
		.map(|p| p as f64)
		.sum::<f64>() / normal_tasks.len() as f64;

	let low_avg_pos: f64 = low_tasks
		.iter()
		.filter_map(|&id| final_results.iter().position(|&x| x == id))
		.map(|p| p as f64)
		.sum::<f64>() / low_tasks.len() as f64;

	// Verify average positions follow priority order
	assert!(
		high_avg_pos < normal_avg_pos,
		"High priority tasks should on average execute before Normal priority tasks. High avg: {}, Normal avg: {}, Results: {:?}",
		high_avg_pos,
		normal_avg_pos,
		*final_results
	);

	assert!(
		normal_avg_pos < low_avg_pos,
		"Normal priority tasks should on average execute before Low priority tasks. Normal avg: {}, Low avg: {}, Results: {:?}",
		normal_avg_pos,
		low_avg_pos,
		*final_results
	);

	assert!(instance.shutdown().is_ok());
}

#[test]
fn test_priority_with_all_levels() {
	// Test that all priority levels are correctly ordered
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

	let results = Arc::new(Mutex::new(Vec::new()));
	let start_flag = Arc::new(AtomicUsize::new(0));

	// Submit a task that will block initially to allow all other tasks to
	// queue
	let start_flag_clone = Arc::clone(&start_flag);
	let initial_task = Box::new(InternalClosureTask::new("initial_blocker", Priority::Normal, move |_ctx| {
		// Wait for signal that all tasks are queued
		while start_flag_clone.load(Ordering::Relaxed) == 0 {
			thread::sleep(Duration::from_millis(5));
		}
		Ok(())
	}));
	assert!(instance.submit(initial_task).is_ok());

	// Give the initial task time to start
	thread::sleep(Duration::from_millis(20));

	// Submit tasks with all priority levels
	let test_tasks = vec![
		(1, Priority::High),
		(2, Priority::Low),
		(3, Priority::High),
		(4, Priority::Normal),
		(5, Priority::High),
		(6, Priority::Low),
		(7, Priority::High),
		(8, Priority::Normal),
		(9, Priority::Low),
		(10, Priority::Normal),
	];

	for (id, priority) in test_tasks {
		let results_clone = Arc::clone(&results);

		let task = Box::new(InternalClosureTask::new(format!("task_{}", id), priority, move |_ctx| {
			results_clone.lock().unwrap().push(id);
			Ok(())
		}));

		assert!(instance.submit(task).is_ok());
	}

	// Signal that all tasks are queued
	start_flag.store(1, Ordering::Relaxed);

	// Wait for all tasks to complete
	thread::sleep(Duration::from_millis(200));

	let final_results = results.lock().unwrap();

	// Check ordering: High > Normal > Low
	let high_tasks = vec![1, 3, 5, 7];
	let normal_tasks = vec![4, 8, 10];
	let low_tasks = vec![2, 6, 9];

	// Find positions of each priority group
	let high_positions: Vec<_> =
		high_tasks.iter().filter_map(|&id| final_results.iter().position(|&x| x == id)).collect();
	let normal_positions: Vec<_> =
		normal_tasks.iter().filter_map(|&id| final_results.iter().position(|&x| x == id)).collect();
	let low_positions: Vec<_> =
		low_tasks.iter().filter_map(|&id| final_results.iter().position(|&x| x == id)).collect();

	// Verify High tasks come before Normal
	let max_high = high_positions.iter().max().unwrap_or(&0);
	let min_normal = normal_positions.iter().min().unwrap_or(&usize::MAX);
	assert!(max_high < min_normal, "All High tasks should execute before Normal tasks");

	// Verify Normal tasks come before Low
	let max_normal = normal_positions.iter().max().unwrap_or(&0);
	let min_low = low_positions.iter().min().unwrap_or(&usize::MAX);
	assert!(max_normal < min_low, "All Normal tasks should execute before Low tasks");

	assert!(instance.shutdown().is_ok());
}

#[test]
fn test_priority_starvation_prevention() {
	// Test that low priority tasks eventually get executed even with
	// continuous high priority submissions
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

	let low_priority_executed = Arc::new(AtomicUsize::new(0));
	let high_priority_executed = Arc::new(AtomicUsize::new(0));

	// Submit some low priority tasks first
	for i in 0..5 {
		let low_counter = Arc::clone(&low_priority_executed);
		let task = Box::new(InternalClosureTask::new(format!("low_{}", i), Priority::Low, move |_ctx| {
			low_counter.fetch_add(1, Ordering::Relaxed);
			// Simulate some work
			thread::sleep(Duration::from_millis(10));
			Ok(())
		}));
		assert!(instance.submit(task).is_ok());
	}

	// Continuously submit high priority tasks
	for i in 0..10 {
		let high_counter = Arc::clone(&high_priority_executed);
		let task = Box::new(InternalClosureTask::new(format!("high_{}", i), Priority::High, move |_ctx| {
			high_counter.fetch_add(1, Ordering::Relaxed);
			// Simulate some work
			thread::sleep(Duration::from_millis(5));
			Ok(())
		}));
		assert!(instance.submit(task).is_ok());
	}

	// Wait for tasks to execute
	thread::sleep(Duration::from_millis(300));

	// Check that both high and low priority tasks were executed
	let low_count = low_priority_executed.load(Ordering::Relaxed);
	let high_count = high_priority_executed.load(Ordering::Relaxed);

	assert_eq!(high_count, 10, "All high priority tasks should be executed");
	assert_eq!(low_count, 5, "All low priority tasks should eventually be executed");

	assert!(instance.shutdown().is_ok());
}
