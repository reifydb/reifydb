// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	cmp,
	collections::{BinaryHeap, HashMap},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	time::{Duration, Instant},
};

use reifydb_core::interface::subsystem::workerpool::{Priority, TaskHandle};

use super::task::{PoolTask, ScheduledTask};

/// Manages scheduled and periodic tasks
pub struct TaskScheduler {
	/// Next task handle ID
	next_handle: AtomicU64,

	/// All scheduled tasks by handle
	tasks: HashMap<TaskHandle, ScheduledTask>,

	/// Priority queue of tasks by next run time
	queue: BinaryHeap<ScheduledTaskRef>,
}

/// Reference to a scheduled task in the priority queue
struct ScheduledTaskRef {
	handle: TaskHandle,
	next_run: Instant,
	priority: Priority,
}

impl PartialEq for ScheduledTaskRef {
	fn eq(&self, other: &Self) -> bool {
		self.next_run == other.next_run && self.priority == other.priority
	}
}

impl Eq for ScheduledTaskRef {}

impl PartialOrd for ScheduledTaskRef {
	fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for ScheduledTaskRef {
	fn cmp(&self, other: &Self) -> cmp::Ordering {
		// Earlier time first
		match other.next_run.cmp(&self.next_run) {
			cmp::Ordering::Equal => {
				// Higher priority first
				self.priority.cmp(&other.priority)
			}
			other => other,
		}
	}
}

impl TaskScheduler {
	pub fn new() -> Self {
		Self {
			next_handle: AtomicU64::new(1),
			tasks: HashMap::new(),
			queue: BinaryHeap::new(),
		}
	}

	/// Schedule a one-time task to run at a specific time
	pub fn schedule_at(&mut self, task: Box<dyn PoolTask>, run_at: Instant, priority: Priority) -> TaskHandle {
		let handle = TaskHandle::from(self.next_handle.fetch_add(1, Ordering::Relaxed));

		let scheduled = ScheduledTask::new(handle, task, run_at, None, priority);

		self.queue.push(ScheduledTaskRef {
			handle,
			next_run: run_at,
			priority,
		});

		self.tasks.insert(handle, scheduled);
		handle
	}

	/// Schedule a one-time task to run after a delay
	pub fn schedule_after(&mut self, task: Box<dyn PoolTask>, delay: Duration, priority: Priority) -> TaskHandle {
		self.schedule_at(task, Instant::now() + delay, priority)
	}

	/// Schedule a periodic task
	pub fn schedule_periodic(
		&mut self,
		task: Box<dyn PoolTask>,
		interval: Duration,
		priority: Priority,
	) -> TaskHandle {
		let handle = TaskHandle::from(self.next_handle.fetch_add(1, Ordering::Relaxed));
		let next_run = Instant::now() + interval;

		let scheduled = ScheduledTask::new(handle, task, next_run, Some(interval), priority);

		self.queue.push(ScheduledTaskRef {
			handle,
			next_run,
			priority,
		});

		self.tasks.insert(handle, scheduled);
		handle
	}

	/// Cancel a scheduled task
	pub fn cancel(&mut self, handle: TaskHandle) {
		self.tasks.remove(&handle);
		// Note: The task reference remains in the queue but will be
		// ignored when popped since it's no longer in the tasks map
	}

	/// Get all tasks that are ready to run
	pub fn get_ready_tasks(&mut self) -> Vec<Box<dyn PoolTask>> {
		let now = Instant::now();
		let mut ready = Vec::new();

		// Pop all tasks that are ready
		while let Some(task_ref) = self.queue.peek() {
			if task_ref.next_run > now {
				break; // No more tasks ready
			}

			let task_ref = self.queue.pop().unwrap();

			// Check if task still exists (might have been
			// cancelled)
			if let Some(mut scheduled) = self.tasks.remove(&task_ref.handle) {
				let shared_task = SharedTask::new(scheduled.task.clone());
				ready.push(Box::new(shared_task) as Box<dyn PoolTask>);

				if let Some(interval) = scheduled.interval {
					scheduled.next_run = now + interval;

					self.queue.push(ScheduledTaskRef {
						handle: task_ref.handle,
						next_run: scheduled.next_run,
						priority: scheduled.priority,
					});

					self.tasks.insert(task_ref.handle, scheduled);
				}
			}
		}

		ready
	}

	/// Get the next scheduled run time
	pub fn next_run_time(&self) -> Option<Instant> {
		self.queue.peek().map(|t| t.next_run)
	}

	/// Get number of scheduled tasks
	pub fn task_count(&self) -> usize {
		self.tasks.len()
	}
}

/// Wrapper to share tasks safely across threads
struct SharedTask(Arc<dyn PoolTask>);

impl SharedTask {
	fn new(task: Arc<dyn PoolTask>) -> Self {
		Self(task)
	}
}

impl PoolTask for SharedTask {
	fn execute(&self, ctx: &super::task::TaskContext) -> crate::Result<()> {
		self.0.execute(ctx)
	}

	fn priority(&self) -> Priority {
		self.0.priority()
	}

	fn name(&self) -> &str {
		self.0.name()
	}

	fn can_retry(&self) -> bool {
		self.0.can_retry()
	}

	fn max_retries(&self) -> usize {
		self.0.max_retries()
	}
}
