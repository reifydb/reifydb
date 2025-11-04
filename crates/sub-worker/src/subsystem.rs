// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Priority Worker Pool Subsystem
//!
//! A centralized thread pool for managing all background work with
//! priority-based scheduling. This subsystem provides efficient resource
//! utilization by sharing worker threads between different background tasks

use std::{
	any::Any,
	collections::{BinaryHeap, VecDeque},
	sync::{
		Arc, Condvar, Mutex,
		atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
		mpsc::{self, Receiver, Sender},
	},
	thread::{self, JoinHandle},
	time::Duration,
};

use reifydb_core::{
	Result,
	interface::version::{ComponentType, HasVersion, SystemVersion},
	log_debug, log_warn,
};
use reifydb_engine::StandardEngine;
pub use reifydb_sub_api::Priority;
use reifydb_sub_api::{BoxedOnceTask, BoxedTask, HealthStatus, Scheduler, SchedulerService, Subsystem, TaskHandle};

use crate::{
	client::{SchedulerClient, SchedulerRequest, SchedulerResponse},
	scheduler::{OnceTaskAdapter, SchedulableTaskAdapter, TaskScheduler},
	task::{PoolTask, PrioritizedTask},
	tracker::TaskTracker,
};

/// Configuration for the worker pool
#[derive(Debug, Clone)]
pub struct WorkerConfig {
	/// Number of worker threads
	pub num_workers: usize,
	/// Maximum number of queued tasks
	pub max_queue_size: usize,
	/// How often to check for periodic tasks
	pub scheduler_interval: Duration,
	/// Maximum time a task can run before warning
	pub task_timeout_warning: Duration,
}

impl Default for WorkerConfig {
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
pub struct WorkerSubsystem {
	config: WorkerConfig,
	running: Arc<AtomicBool>,
	stats: Arc<PoolStats>,

	// Rayon execution
	thread_pool: Option<Arc<rayon::ThreadPool>>,
	dispatcher_handle: Option<JoinHandle<()>>,

	// Task tracking
	task_tracker: Arc<TaskTracker>,

	// Task priority queue
	task_queue: Arc<Mutex<BinaryHeap<PrioritizedTask>>>,
	task_condvar: Arc<Condvar>,

	// Scheduler for periodic tasks
	scheduler: Arc<Mutex<TaskScheduler>>,
	scheduler_condvar: Arc<Condvar>, // Wake scheduler when tasks are added
	scheduler_handle: Option<JoinHandle<()>>,

	// Scheduler request receiver (set during factory creation) - wrapped
	// in Mutex for Sync
	scheduler_receiver: Arc<Mutex<Option<Receiver<(SchedulerRequest, Sender<SchedulerResponse>)>>>>,

	// Pending requests queue for requests made before start()
	pending_requests: Arc<Mutex<VecDeque<(SchedulerRequest, Sender<SchedulerResponse>)>>>,

	// Next handle ID for generating handles when queuing
	next_handle: Arc<AtomicU64>,

	// Scheduler client for external access
	scheduler_client: Arc<dyn Scheduler>,

	// Engine for task execution
	engine: StandardEngine,
}

impl WorkerSubsystem {
	pub fn new(config: WorkerConfig, engine: StandardEngine) -> Self {
		let pending_requests = Arc::new(Mutex::new(VecDeque::new()));
		let next_handle = Arc::new(AtomicU64::new(1));
		let running = Arc::new(AtomicBool::new(false));

		let (sender, receiver) = mpsc::channel();

		let scheduler_client = Arc::new(SchedulerClient::new(
			sender,
			Arc::clone(&pending_requests),
			Arc::clone(&next_handle),
			Arc::clone(&running),
		));

		let max_queue_size = config.max_queue_size;
		Self {
			config,
			running,
			stats: Arc::new(PoolStats::default()),
			thread_pool: None,
			dispatcher_handle: None,
			task_tracker: Arc::new(TaskTracker::new()),
			task_queue: Arc::new(Mutex::new(BinaryHeap::with_capacity(max_queue_size))),
			task_condvar: Arc::new(Condvar::new()),
			scheduler: Arc::new(Mutex::new(TaskScheduler::new())),
			scheduler_condvar: Arc::new(Condvar::new()),
			scheduler_handle: None,
			scheduler_receiver: Arc::new(Mutex::new(Some(receiver))),
			pending_requests,
			next_handle,
			scheduler_client,
			engine,
		}
	}

	/// Get the scheduler client
	pub fn get_scheduler(&self) -> SchedulerService {
		SchedulerService(self.scheduler_client.clone())
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

	/// Schedule a task to run at fixed intervals (internal)
	fn schedule_every_internal(
		&self,
		task: Box<dyn PoolTask>,
		interval: Duration,
		priority: Priority,
	) -> Result<TaskHandle> {
		let mut scheduler = self.scheduler.lock().unwrap();
		let handle = scheduler.schedule_every_internal(task, interval, priority);
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

	/// Dispatcher thread that bridges priority queue and Rayon
	fn run_dispatcher(
		pool: Arc<rayon::ThreadPool>,
		queue: Arc<Mutex<BinaryHeap<PrioritizedTask>>>,
		condvar: Arc<Condvar>,
		tracker: Arc<TaskTracker>,
		stats: Arc<PoolStats>,
		running: Arc<AtomicBool>,
		engine: StandardEngine,
	) {
		log_debug!("Dispatcher thread started");

		while running.load(Ordering::Relaxed) {
			// Wait for tasks in priority queue
			let task = {
				let mut queue_guard = queue.lock().unwrap();

				// Wait for tasks or shutdown signal
				while queue_guard.is_empty() && running.load(Ordering::Relaxed) {
					let (guard, timeout_result) =
						condvar.wait_timeout(queue_guard, Duration::from_millis(100)).unwrap();
					queue_guard = guard;

					// Check state periodically even on timeout
					if timeout_result.timed_out() {
						continue;
					}
				}

				queue_guard.pop()
			};

			if let Some(prioritized_task) = task {
				// Update stats
				stats.tasks_queued.fetch_sub(1, Ordering::Relaxed);

				// Register task with tracker
				let (task_id, cancel_token) = tracker.register(None);

				// Submit to Rayon for execution
				let tracker_clone = Arc::clone(&tracker);
				let stats_clone = Arc::clone(&stats);
				let engine_clone = engine.clone();

				pool.spawn(move || {
					// Check cancellation before starting
					if cancel_token.is_cancelled() {
						tracker_clone.complete(task_id);
						return;
					}

					// Update stats
					stats_clone.active_workers.fetch_add(1, Ordering::Relaxed);

					// Create task context
					let ctx = crate::task::InternalTaskContext {
						cancel_token: Some(cancel_token.clone()),
						engine: engine_clone,
					};

					// Execute task
					let start = std::time::Instant::now();
					let result = prioritized_task.task.execute(&ctx);
					let duration = start.elapsed();

					// Log slow tasks
					if duration > Duration::from_secs(5) {
						log_warn!(
							"Task '{}' took {:?} to execute",
							prioritized_task.task.name(),
							duration
						);
					}

					// Update stats based on result
					match result {
						Ok(_) => {
							stats_clone.tasks_completed.fetch_add(1, Ordering::Relaxed);
						}
						Err(e) => {
							log_warn!(
								"Task '{}' failed: {}",
								prioritized_task.task.name(),
								e
							);
							stats_clone.tasks_failed.fetch_add(1, Ordering::Relaxed);
						}
					}

					stats_clone.active_workers.fetch_sub(1, Ordering::Relaxed);
					tracker_clone.complete(task_id);
				});
			}
		}

		// Drain remaining tasks on shutdown
		Self::drain_queue(pool, queue, tracker, stats, engine);

		log_debug!("Dispatcher thread stopped");
	}

	/// Drain queue during shutdown
	fn drain_queue(
		pool: Arc<rayon::ThreadPool>,
		queue: Arc<Mutex<BinaryHeap<PrioritizedTask>>>,
		tracker: Arc<TaskTracker>,
		stats: Arc<PoolStats>,
		engine: StandardEngine,
	) {
		log_debug!("Draining task queue during shutdown");

		loop {
			let task = {
				let mut queue_guard = queue.lock().unwrap();
				queue_guard.pop()
			};

			match task {
				Some(prioritized_task) => {
					stats.tasks_queued.fetch_sub(1, Ordering::Relaxed);

					// Still execute tasks during shutdown for graceful completion
					let (task_id, _) = tracker.register(None);
					let tracker_clone = Arc::clone(&tracker);
					let stats_clone = Arc::clone(&stats);
					let engine_clone = engine.clone();

					pool.spawn(move || {
						let ctx = crate::task::InternalTaskContext {
							cancel_token: None,
							engine: engine_clone,
						};
						let _ = prioritized_task.task.execute(&ctx);
						stats_clone.tasks_completed.fetch_add(1, Ordering::Relaxed);
						tracker_clone.complete(task_id);
					});
				}
				None => break,
			}
		}
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
		let scheduler_receiver = Arc::clone(&self.scheduler_receiver);
		let pending_requests = Arc::clone(&self.pending_requests);
		let next_handle = Arc::clone(&self.next_handle);
		let engine = self.engine.clone();

		let handle = thread::Builder::new()
			.name("worker-scheduler".to_string())
			.spawn(move || {
				// Process any pending requests that were queued before start
				{
					let mut pending = pending_requests.lock().unwrap();
					let mut sched = scheduler.lock().unwrap();

					// Set the scheduler's next handle to match what we've been using
					sched.set_next_handle(next_handle.load(Ordering::Relaxed));

					while let Some((request, response_tx)) = pending.pop_front() {
						let response = match request {
							SchedulerRequest::ScheduleEvery {
								task,
								interval,
							} => {
								let adapter = Box::new(SchedulableTaskAdapter::new(
									task,
									engine.clone(),
								));
								let priority = adapter.priority();
								let handle = sched.schedule_every_internal(
									adapter, interval, priority,
								);
								SchedulerResponse::TaskScheduled(handle)
							}
							SchedulerRequest::Submit {
								task,
								priority: _,
							} => {
								let adapter = Box::new(OnceTaskAdapter::new(
									task,
									engine.clone(),
								));
								// Submit directly to task queue
								drop(sched);
								{
									let mut queue = task_queue.lock().unwrap();
									if queue.len() < max_queue_size {
										queue.push(PrioritizedTask::new(
											adapter,
										));
										stats.tasks_queued.fetch_add(
											1,
											Ordering::Relaxed,
										);
										task_condvar.notify_one();
									}
								}
								sched = scheduler.lock().unwrap();
								SchedulerResponse::TaskSubmitted
							}
							SchedulerRequest::Cancel {
								handle,
							} => {
								sched.cancel(handle);
								SchedulerResponse::TaskCancelled
							}
						};
						// Send response (receiver might be gone if client didn't wait)
						let _ = response_tx.send(response);
					}
					drop(sched);
					drop(pending);
				}

				while running.load(Ordering::Relaxed) {
					// Check for scheduler requests if receiver is available
					{
						let receiver_guard = scheduler_receiver.lock().unwrap();
						if let Some(ref receiver) = *receiver_guard {
							while let Ok((request, response_tx)) = receiver.try_recv() {
								let mut sched = scheduler.lock().unwrap();
								let response = match request {
									SchedulerRequest::ScheduleEvery {
										task,
										interval,
									} => {
										// Create adapter from SchedulableTask
										// to PoolTask
										let adapter = Box::new(
											SchedulableTaskAdapter::new(
												task,
												engine.clone(),
											),
										);
										let priority = adapter.priority();
										let handle = sched
											.schedule_every_internal(
												adapter, interval,
												priority,
											);
										SchedulerResponse::TaskScheduled(handle)
									}
									SchedulerRequest::Submit {
										task,
										priority: _,
									} => {
										let adapter =
											Box::new(OnceTaskAdapter::new(
												task,
												engine.clone(),
											));
										drop(sched);

										{
											let mut queue = task_queue
												.lock()
												.unwrap();
											if queue.len() < max_queue_size
											{
												queue.push(PrioritizedTask::new(adapter));
												stats.tasks_queued.fetch_add(1, Ordering::Relaxed);
												task_condvar
													.notify_one();
											}
										}
										sched = scheduler.lock().unwrap();
										SchedulerResponse::TaskSubmitted
									}
									SchedulerRequest::Cancel {
										handle,
									} => {
										sched.cancel(handle);
										SchedulerResponse::TaskCancelled
									}
								};
								drop(sched);
								// Send response back
								let _ = response_tx.send(response);
							}
						}
					}

					let mut sched = scheduler.lock().unwrap();

					// Wait until we have scheduled tasks or need to check for ready tasks
					if sched.task_count() == 0 {
						// No scheduled tasks, wait for notification with timeout
						let result = scheduler_condvar
							.wait_timeout(sched, Duration::from_millis(1))
							.unwrap();

						sched = result.0;

						// Check again if we should exit
						if !running.load(Ordering::Relaxed) {
							break;
						}

						// Drop the lock and continue to check for new requests
						drop(sched);
						continue;
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
								log_warn!(
									"Scheduler: Queue full, dropping scheduled task"
								);
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

impl Subsystem for WorkerSubsystem {
	fn name(&self) -> &'static str {
		"Worker"
	}

	fn start(&mut self) -> Result<()> {
		if self.running.load(Ordering::Relaxed) {
			return Ok(()); // Already running
		}

		log_debug!("Starting worker subsystem with {} workers", self.config.num_workers);

		// Create Rayon thread pool
		let pool = rayon::ThreadPoolBuilder::new()
			.num_threads(self.config.num_workers)
			.thread_name(|i| format!("rayon-worker-{}", i))
			.panic_handler(|panic_info| {
				log_warn!("Worker thread panicked: {:?}", panic_info);
			})
			.build()
			.map_err(|e| {
				reifydb_core::error!(reifydb_core::diagnostic::internal(format!(
					"Failed to create thread pool: {}",
					e
				)))
			})?;

		self.thread_pool = Some(Arc::new(pool));
		self.running.store(true, Ordering::Relaxed);

		// Start dispatcher thread
		{
			let pool = Arc::clone(self.thread_pool.as_ref().unwrap());
			let queue = Arc::clone(&self.task_queue);
			let condvar = Arc::clone(&self.task_condvar);
			let tracker = Arc::clone(&self.task_tracker);
			let stats = Arc::clone(&self.stats);
			let running = Arc::clone(&self.running);
			let engine = self.engine.clone();

			let handle = thread::Builder::new()
				.name("worker-dispatcher".to_string())
				.spawn(move || {
					Self::run_dispatcher(pool, queue, condvar, tracker, stats, running, engine)
				})
				.map_err(|e| {
					reifydb_core::error!(reifydb_core::diagnostic::internal(format!(
						"Failed to spawn dispatcher thread: {}",
						e
					)))
				})?;

			self.dispatcher_handle = Some(handle);
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

		log_debug!("Shutting down worker subsystem...");

		// Signal all threads to stop
		self.running.store(false, Ordering::Relaxed);

		// Wake up dispatcher
		self.task_condvar.notify_all();

		// Wake up scheduler
		self.scheduler_condvar.notify_all();

		// Join scheduler thread
		if let Some(handle) = self.scheduler_handle.take() {
			let _ = handle.join();
		}

		// Join dispatcher thread
		if let Some(handle) = self.dispatcher_handle.take() {
			let _ = handle.join();
		}

		// Wait for in-flight tasks to complete
		let timeout = Duration::from_secs(30);
		if !self.task_tracker.wait_for_completion(timeout) {
			log_warn!(
				"Timeout waiting for tasks to complete. {} tasks still running",
				self.task_tracker.active_count()
			);
		}

		// Rayon thread pool will be dropped automatically
		self.thread_pool = None;

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
				description: "No active workers but tasks are queued".into(),
			}
		} else if queued > self.config.max_queue_size / 2 {
			// Queue getting full - degraded
			HealthStatus::Degraded {
				description: format!(
					"Task queue is {}% full",
					(queued * 100) / self.config.max_queue_size
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

impl HasVersion for WorkerSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "sub-worker".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Priority-based task worker pool subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

impl Drop for WorkerSubsystem {
	fn drop(&mut self) {
		let _ = self.shutdown();
	}
}

impl Scheduler for WorkerSubsystem {
	fn every(&self, interval: Duration, task: BoxedTask) -> reifydb_core::Result<TaskHandle> {
		let adapter = Box::new(SchedulableTaskAdapter::new(task, self.engine.clone()));
		let priority = adapter.priority();
		self.schedule_every_internal(adapter, interval, priority)
	}

	fn cancel(&self, handle: TaskHandle) -> Result<()> {
		self.cancel_task(handle)
	}

	fn once(&self, task: BoxedOnceTask) -> reifydb_core::Result<()> {
		let adapter = Box::new(OnceTaskAdapter::new(task, self.engine.clone()));
		WorkerSubsystem::submit(self, adapter)
	}
}
