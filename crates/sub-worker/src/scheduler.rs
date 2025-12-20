// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	cmp,
	collections::{BinaryHeap, HashMap},
	sync::{
		Arc, Mutex,
		atomic::{AtomicU64, Ordering},
	},
	time::{Duration, Instant},
};

use reifydb_core::Result;
use reifydb_engine::StandardEngine;
use reifydb_sub_api::{BoxedOnceTask, BoxedTask, Priority, TaskContext as CoreTaskContext, TaskHandle};

use crate::task::{InternalTaskContext, PoolTask, ScheduledTask};

/// Adapter from SchedulableTask to PoolTask
pub(crate) struct SchedulableTaskAdapter {
	task: BoxedTask,
	engine: StandardEngine,
}

impl SchedulableTaskAdapter {
	pub(crate) fn new(task: BoxedTask, engine: StandardEngine) -> Self {
		Self {
			task,
			engine,
		}
	}
}

impl PoolTask for SchedulableTaskAdapter {
	fn execute(&self, _ctx: &InternalTaskContext) -> Result<()> {
		let core_ctx = CoreTaskContext::new(self.engine.clone());
		self.task.execute(&core_ctx)
	}

	fn priority(&self) -> Priority {
		self.task.priority()
	}

	fn name(&self) -> &str {
		self.task.name()
	}
}

/// Adapter from OnceTask to PoolTask
///
/// This adapter wraps a BoxedOnceTask in a Mutex so it can be
/// executed once through the PoolTask interface. After execution,
/// the task is consumed.
pub(crate) struct OnceTaskAdapter {
	task: Mutex<Option<BoxedOnceTask>>,
	engine: StandardEngine,
}

impl OnceTaskAdapter {
	pub(crate) fn new(task: BoxedOnceTask, engine: StandardEngine) -> Self {
		Self {
			task: Mutex::new(Some(task)),
			engine,
		}
	}
}

impl PoolTask for OnceTaskAdapter {
	fn execute(&self, _ctx: &InternalTaskContext) -> Result<()> {
		// Take the task out of the Mutex. This can only be done once.
		let task = self.task.lock().unwrap().take();
		if let Some(task) = task {
			let core_ctx = CoreTaskContext::new(self.engine.clone());
			task.execute_once(&core_ctx)
		} else {
			panic!("OnceTask already executed");
		}
	}

	fn priority(&self) -> Priority {
		self.task.lock().unwrap().as_ref().map(|t| t.priority()).unwrap_or(Priority::Normal)
	}

	fn name(&self) -> &str {
		// Name is called before execute, so task should still be present
		// Return a generic name since we can't borrow from Mutex
		"once-task"
	}
}

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

	/// Set the next handle ID to use (for synchronization with
	/// pre-generated handles)
	pub fn set_next_handle(&self, handle_id: u64) {
		self.next_handle.store(handle_id, Ordering::Relaxed);
	}

	/// Schedule a task to run at fixed intervals (internal implementation)
	pub fn schedule_every_internal(
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
	fn execute(&self, ctx: &InternalTaskContext) -> crate::Result<()> {
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
