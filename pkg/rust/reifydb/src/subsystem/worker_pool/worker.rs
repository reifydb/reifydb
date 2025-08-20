// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{PoolStats, PoolTask, PrioritizedTask, TaskContext};
use reifydb_core::{log_info, log_trace, log_warn};
use std::{
    collections::BinaryHeap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering}, Arc, Condvar,
        Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// A worker thread in the pool
pub struct Worker {
	id: usize,
	task_queue: Arc<Mutex<BinaryHeap<PrioritizedTask>>>,
	task_condvar: Arc<Condvar>,
	running: Arc<AtomicBool>,
	stats: Arc<PoolStats>,
	timeout_warning: Duration,
	handle: Option<JoinHandle<()>>,
	task_counter: Arc<AtomicU64>,
}

impl Worker {
	pub fn new(
		id: usize,
		task_queue: Arc<Mutex<BinaryHeap<PrioritizedTask>>>,
		task_condvar: Arc<Condvar>,
		running: Arc<AtomicBool>,
		stats: Arc<PoolStats>,
		timeout_warning: Duration,
	) -> Self {
		Self {
			id,
			task_queue,
			task_condvar,
			running,
			stats,
			timeout_warning,
			handle: None,
			task_counter: Arc::new(AtomicU64::new(0)),
		}
	}

	/// Start the worker thread
	pub fn start(&mut self) {
		let id = self.id;
		let task_queue = Arc::clone(&self.task_queue);
		let task_condvar = Arc::clone(&self.task_condvar);
		let running = Arc::clone(&self.running);
		let stats = Arc::clone(&self.stats);
		let timeout_warning = self.timeout_warning;
		let task_counter = Arc::clone(&self.task_counter);

		let handle = thread::Builder::new()
			.name(format!("worker-pool-{}", id))
			.spawn(move || {
				Self::run_worker(
					id,
					task_queue,
					task_condvar,
					running,
					stats,
					timeout_warning,
					task_counter,
				);
			})
			.expect("Failed to create worker thread");

		self.handle = Some(handle);
	}

	/// Stop the worker thread
	pub fn stop(mut self) {
		if let Some(handle) = self.handle.take() {
			// Worker will stop when running becomes false
			let _ = handle.join();
		}
	}

	/// Main worker loop
	fn run_worker(
		id: usize,
		task_queue: Arc<Mutex<BinaryHeap<PrioritizedTask>>>,
		task_condvar: Arc<Condvar>,
		running: Arc<AtomicBool>,
		stats: Arc<PoolStats>,
		timeout_warning: Duration,
		task_counter: Arc<AtomicU64>,
	) {
		stats.active_workers.fetch_add(1, Ordering::Relaxed);

		log_trace!("Worker {} started", id);

		while running.load(Ordering::Relaxed) {
			// Get task from priority queue
			let task = {
				let mut queue = task_queue.lock().unwrap();

				loop {
					if let Some(prioritized_task) =
						queue.pop()
					{
						break Some(
							prioritized_task.task
						);
					}

					if !running.load(Ordering::Relaxed) {
						break None;
					}

					// Wait for new tasks with short timeout
					// to check running flag
					let result = task_condvar
						.wait_timeout(
							queue,
							Duration::from_millis(
								10,
							),
						)
						.unwrap();
					queue = result.0;

					// Check running flag after timeout
					if !running.load(Ordering::Relaxed) {
						break None;
					}
				}
			};

			if let Some(task) = task {
				Self::execute_task(
					id,
					task,
					&stats,
					timeout_warning,
					&task_counter,
				);
			}
		}

		// Drain remaining tasks before shutting down
		{
			let mut queue = task_queue.lock().unwrap();
			while let Some(prioritized_task) = queue.pop() {
				Self::execute_task(
					id,
					prioritized_task.task,
					&stats,
					timeout_warning,
					&task_counter,
				);
			}
		}

		stats.active_workers.fetch_sub(1, Ordering::Relaxed);
		log_info!("Worker {} stopped", id);
	}

	/// Execute a single task
	fn execute_task(
		worker_id: usize,
		task: Box<dyn PoolTask>,
		stats: &Arc<PoolStats>,
		timeout_warning: Duration,
		task_counter: &Arc<AtomicU64>,
	) {
		let task_id = task_counter.fetch_add(1, Ordering::Relaxed);
		let task_name = task.name();
		let start_time = Instant::now();

		stats.tasks_queued.fetch_sub(1, Ordering::Relaxed);

		let ctx = TaskContext {
			task_id,
			worker_id,
			start_time,
		};

		// Execute the task
		let mut retries = 0;
		let max_retries = if task.can_retry() {
			task.max_retries()
		} else {
			0
		};

		loop {
			match task.execute(&ctx) {
				Ok(_) => {
					stats.tasks_completed.fetch_add(
						1,
						Ordering::Relaxed,
					);

					let elapsed = start_time.elapsed();
					if elapsed > timeout_warning {
						log_warn!(
							"Task '{}' took {:?} (exceeded warning threshold)",
							task_name, elapsed
						);
					}

					break;
				}
				Err(e) => {
					if retries < max_retries {
						retries += 1;
						log_warn!(
							"Task '{}' failed, retrying ({}/{}): {}",
							task_name,
							retries,
							max_retries,
							e
						);
						// Small backoff before retry
						thread::sleep(
							Duration::from_millis(
								100 * retries
									as u64,
							),
						);
					} else {
						stats.tasks_failed.fetch_add(
							1,
							Ordering::Relaxed,
						);
						log_warn!(
							"Task '{}' failed after {} retries: {}",
							task_name, retries, e
						);
						break;
					}
				}
			}
		}
	}
}

impl Drop for Worker {
	fn drop(&mut self) {
		// Ensure thread is joined on drop
		if let Some(handle) = self.handle.take() {
			let _ = handle.join();
		}
	}
}
