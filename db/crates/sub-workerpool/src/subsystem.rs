// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Priority Worker Pool Subsystem
//!
//! A centralized thread pool for managing all background work with
//! priority-based scheduling. This subsystem provides efficient resource
//! utilization by sharing worker threads between different background tasks

use std::{
	any::Any,
	collections::BinaryHeap,
	sync::{
		Arc, Condvar, Mutex,
		atomic::{AtomicBool, AtomicUsize, Ordering},
	},
	thread::{self, JoinHandle},
	time::Duration,
};

pub use reifydb_core::interface::subsystem::workerpool::Priority;
use reifydb_core::{
	Result,
	interface::{
		subsystem::{
			HealthStatus, Subsystem,
			workerpool::{TaskHandle, WorkerPool},
		},
		version::{ComponentType, HasVersion, SystemVersion},
	},
	log_debug, log_warn,
};

use crate::{
	scheduler::TaskScheduler,
	task::{ClosureTask, PoolTask, PrioritizedTask},
	worker::Worker,
};

/// Configuration for the worker pool
#[derive(Debug, Clone)]
pub struct WorkerPoolConfig {
	/// Number of worker threads
	pub num_workers: usize,
	/// Maximum number of queued tasks
	pub max_queue_size: usize,
	/// How often to check for periodic tasks
	pub scheduler_interval: Duration,
	/// Maximum time a task can run before warning
	pub task_timeout_warning: Duration,
}

impl Default for WorkerPoolConfig {
	fn default() -> Self {
		Self {
			num_workers: 1,
			max_queue_size: 10000,
			scheduler_interval: Duration::from_millis(10),
			task_timeout_warning: Duration::from_secs(30),
		}
	}
}

/// Statistics about the worker pool
#[derive(Debug, Default)]
pub struct PoolStats {
	pub tasks_completed: AtomicUsize,
	pub tasks_failed: AtomicUsize,
	pub tasks_queued: AtomicUsize,
	pub active_workers: AtomicUsize,
}

/// Priority Worker Pool Subsystem
pub struct WorkerPoolSubsystem {
	config: WorkerPoolConfig,
	running: Arc<AtomicBool>,
	stats: Arc<PoolStats>,

	// Task priority queue
	task_queue: Arc<Mutex<BinaryHeap<PrioritizedTask>>>,
	task_condvar: Arc<Condvar>,

	// Worker threads
	workers: Vec<Worker>,

	// Scheduler for periodic tasks
	scheduler: Arc<Mutex<TaskScheduler>>,
	scheduler_condvar: Arc<Condvar>, // Wake scheduler when tasks are added
	scheduler_handle: Option<JoinHandle<()>>,
}

impl WorkerPoolSubsystem {
	/// Create a new worker pool with default configuration
	pub fn new() -> Self {
		Self::with_config(WorkerPoolConfig::default())
	}

	/// Create a new worker pool with custom configuration
	pub fn with_config(config: WorkerPoolConfig) -> Self {
		let max_queue_size = config.max_queue_size;
		Self {
			config,
			running: Arc::new(AtomicBool::new(false)),
			stats: Arc::new(PoolStats::default()),
			task_queue: Arc::new(Mutex::new(
				BinaryHeap::with_capacity(max_queue_size),
			)),
			task_condvar: Arc::new(Condvar::new()),
			workers: Vec::new(),
			scheduler: Arc::new(Mutex::new(TaskScheduler::new())),
			scheduler_condvar: Arc::new(Condvar::new()),
			scheduler_handle: None,
		}
	}

	/// Submit a one-time task to the pool
	pub fn submit(&self, task: Box<dyn PoolTask>) -> Result<()> {
		if !self.running.load(Ordering::Relaxed) {
			panic!("Worker pool is not running");
		}

		{
			let mut queue = self.task_queue.lock().unwrap();

			// Check if queue is full
			if queue.len() >= self.config.max_queue_size {
				panic!(
					"Task queue is full. Consider increasing max_queue_size or reducing task submission rate"
				);
			}

			queue.push(PrioritizedTask::new(task));
			self.stats.tasks_queued.fetch_add(1, Ordering::Relaxed);
		}

		// Notify a waiting worker
		self.task_condvar.notify_one();
		Ok(())
	}

	/// Schedule a periodic task
	pub fn schedule_periodic(
		&self,
		task: Box<dyn PoolTask>,
		interval: Duration,
		priority: Priority,
	) -> Result<TaskHandle> {
		let mut scheduler = self.scheduler.lock().unwrap();
		let handle =
			scheduler.schedule_periodic(task, interval, priority);
		drop(scheduler);

		// Wake up the scheduler thread
		self.scheduler_condvar.notify_one();

		Ok(handle)
	}

	/// Cancel a scheduled task
	pub fn cancel_task(&self, handle: TaskHandle) -> Result<()> {
		let mut scheduler = self.scheduler.lock().unwrap();
		scheduler.cancel(handle);
		Ok(())
	}

	/// Get current pool statistics
	pub fn stats(&self) -> &PoolStats {
		&self.stats
	}

	/// Get number of active workers
	pub fn active_workers(&self) -> usize {
		self.stats.active_workers.load(Ordering::Relaxed)
	}

	/// Get number of queued tasks
	pub fn queued_tasks(&self) -> usize {
		self.task_queue.lock().unwrap().len()
	}

	/// Start the scheduler thread
	fn start_scheduler(&mut self) {
		let scheduler = Arc::clone(&self.scheduler);
		let scheduler_condvar = Arc::clone(&self.scheduler_condvar);
		let task_queue = Arc::clone(&self.task_queue);
		let task_condvar = Arc::clone(&self.task_condvar);
		let running = Arc::clone(&self.running);
		let stats = Arc::clone(&self.stats);
		let max_queue_size = self.config.max_queue_size;

		let handle = thread::Builder::new()
            .name("worker-pool-scheduler".to_string())
            .spawn(move || {
                while running.load(Ordering::Relaxed) {
                    let mut sched = scheduler.lock().unwrap();

                    // Wait until we have scheduled tasks or need to check for ready tasks
                    if sched.task_count() == 0 {
                        // No scheduled tasks, wait for notification with timeout
                        let result = scheduler_condvar.wait_timeout(
                            sched,
                            Duration::from_millis(100)
                        ).unwrap();
                        sched = result.0;

                        // Check again if we should exit
                        if !running.load(Ordering::Relaxed) {
                            break;
                        }
                    }

                    // Check what tasks are ready
                    let ready_tasks = sched.get_ready_tasks();

                    // Calculate wait time until next task
                    let wait_duration = if let Some(next_time) = sched.next_run_time() {
                        let now = std::time::Instant::now();
                        if next_time > now {
                            next_time - now
                        } else {
                            Duration::from_millis(0)
                        }
                    } else {
                        // No scheduled tasks, wait indefinitely
                        Duration::from_secs(3600)
                    };

                    drop(sched);

                    // Submit ready tasks to the work queue
                    if !ready_tasks.is_empty() {
                        let mut queue = task_queue.lock().unwrap();

                        for task in ready_tasks {
                            if queue.len() >= max_queue_size {
                                log_warn!("Scheduler: Queue full, dropping scheduled task");
                                break;
                            }

                            queue.push(PrioritizedTask::new(task));
                            stats.tasks_queued.fetch_add(1, Ordering::Relaxed);
                        }

                        drop(queue);
                        task_condvar.notify_all();
                    }

                    // Wait until the next task is ready or we get a notification
                    if wait_duration > Duration::from_millis(0) {
                        let sched = scheduler.lock().unwrap();
                        let _ = scheduler_condvar.wait_timeout(sched, wait_duration);
                    }
                }
            })
            .expect("Failed to create scheduler thread");

		self.scheduler_handle = Some(handle);
	}
}

impl Subsystem for WorkerPoolSubsystem {
	fn name(&self) -> &'static str {
		"WorkerPool"
	}

	fn start(&mut self) -> Result<()> {
		if self.running.load(Ordering::Relaxed) {
			return Ok(()); // Already running
		}

		self.running.store(true, Ordering::Relaxed);

		// Start worker threads
		for i in 0..self.config.num_workers {
			let mut worker = Worker::new(
				i,
				Arc::clone(&self.task_queue),
				Arc::clone(&self.task_condvar),
				Arc::clone(&self.running),
				Arc::clone(&self.stats),
				self.config.task_timeout_warning,
			);
			worker.start();
			self.workers.push(worker);
		}

		// Start scheduler thread
		self.start_scheduler();

		log_debug!("Started with {} workers", self.config.num_workers);

		Ok(())
	}

	fn shutdown(&mut self) -> Result<()> {
		if !self.running.load(Ordering::Relaxed) {
			return Ok(()); // Already stopped
		}

		log_debug!("Shutting down...");
		self.running.store(false, Ordering::Relaxed);

		// Wake scheduler so it can exit
		self.scheduler_condvar.notify_all();

		// Wake all worker threads repeatedly to ensure they exit
		// This handles the race condition where a thread might miss the
		// first notify
		for _ in 0..3 {
			self.task_condvar.notify_all();
			std::thread::sleep(Duration::from_millis(1));
		}

		// Stop scheduler
		if let Some(handle) = self.scheduler_handle.take() {
			let _ = handle.join();
		}

		// Stop all workers
		for worker in self.workers.drain(..) {
			worker.shutdown();
		}

		log_debug!("Shutdown complete");
		Ok(())
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::Relaxed)
	}

	fn health_status(&self) -> HealthStatus {
		if !self.is_running() {
			return HealthStatus::Unknown;
		}

		let active = self.active_workers();
		let queued = self.queued_tasks();

		if active == 0 && queued > 0 {
			// No workers but tasks queued - failed
			HealthStatus::Failed {
				description:
					"No active workers but tasks are queued"
						.into(),
			}
		} else if queued > self.config.max_queue_size / 2 {
			// Queue getting full - degraded
			HealthStatus::Degraded {
				description: format!(
					"Task queue is {}% full",
					(queued * 100)
						/ self.config.max_queue_size
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

impl HasVersion for WorkerPoolSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "sub-workerpool".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description:
				"Priority-based task worker pool subsystem"
					.to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

impl Drop for WorkerPoolSubsystem {
	fn drop(&mut self) {
		let _ = self.shutdown();
	}
}

impl WorkerPool for WorkerPoolSubsystem {
	fn schedule_periodic(
		&self,
		name: String,
		task: Box<dyn Fn() -> Result<bool> + Send + Sync>,
		interval: Duration,
	) -> Result<TaskHandle> {
		// Create a closure task that wraps the provided function
		let closure_task = Box::new(ClosureTask::new(
			name,
			Priority::Normal,
			move |_ctx| {
				// Execute the task and convert the result
				match task() {
					Ok(_) => Ok(()),
					Err(e) => panic!(
						"Task execution error: {:?}",
						e
					),
				}
			},
		));

		// Schedule the periodic task
		self.schedule_periodic(closure_task, interval, Priority::Normal)
	}

	fn cancel(&self, handle: TaskHandle) -> Result<()> {
		self.cancel_task(handle)
	}
}
