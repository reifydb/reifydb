// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicUsize, Ordering},
	},
	time::{Duration, Instant},
};

use crossbeam_skiplist::SkipMap;
use reifydb_core::{Result, error};
use reifydb_sub_api::TaskHandle;

/// Unique identifier for a task
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TaskId(usize);

/// Token for cancelling a task
#[derive(Clone)]
pub struct CancellationToken {
	cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
	pub(crate) fn new() -> Self {
		Self {
			cancelled: Arc::new(AtomicBool::new(false)),
		}
	}

	#[allow(dead_code)]
	pub(crate) fn cancel(&self) {
		self.cancelled.store(true, Ordering::SeqCst);
	}

	pub(crate) fn is_cancelled(&self) -> bool {
		self.cancelled.load(Ordering::SeqCst)
	}
}

/// Tracks task lifecycle for cancellation and monitoring
pub(crate) struct TaskTracker {
	/// Next task ID to assign
	next_id: AtomicUsize,

	/// Active tasks with cancellation tokens
	active_tasks: SkipMap<TaskId, CancellationToken>,

	/// Map from TaskHandle to TaskId for cancellation
	handle_to_id: SkipMap<TaskHandle, TaskId>,

	/// Count of in-flight tasks for shutdown
	in_flight_count: AtomicUsize,
}

impl TaskTracker {
	pub(crate) fn new() -> Self {
		Self {
			next_id: AtomicUsize::new(1),
			active_tasks: SkipMap::new(),
			handle_to_id: SkipMap::new(),
			in_flight_count: AtomicUsize::new(0),
		}
	}

	/// Register a new task and get its ID and cancellation token
	pub(crate) fn register(&self, handle: Option<TaskHandle>) -> (TaskId, CancellationToken) {
		let id = TaskId(self.next_id.fetch_add(1, Ordering::SeqCst));
		let token = CancellationToken::new();

		self.active_tasks.insert(id, token.clone());
		self.in_flight_count.fetch_add(1, Ordering::SeqCst);

		if let Some(handle) = handle {
			self.handle_to_id.insert(handle, id);
		}

		(id, token)
	}

	/// Mark task as completed
	pub(crate) fn complete(&self, id: TaskId) {
		self.active_tasks.remove(&id);
		self.in_flight_count.fetch_sub(1, Ordering::SeqCst);
	}

	/// Cancel a task by handle
	#[allow(dead_code)]
	pub(crate) fn cancel(&self, handle: TaskHandle) -> Result<()> {
		if let Some(entry) = self.handle_to_id.remove(&handle) {
			let task_id = *entry.value();
			if let Some(task_entry) = self.active_tasks.get(&task_id) {
				task_entry.value().cancel();
				return Ok(());
			}
		}
		Err(error!(reifydb_core::diagnostic::internal("Task not found")))
	}

	/// Wait for all tasks to complete (for shutdown)
	pub(crate) fn wait_for_completion(&self, timeout: Duration) -> bool {
		let start = Instant::now();
		while self.in_flight_count.load(Ordering::SeqCst) > 0 {
			if start.elapsed() > timeout {
				return false;
			}
			std::thread::sleep(Duration::from_millis(10));
		}
		true
	}

	/// Get count of active tasks
	pub(crate) fn active_count(&self) -> usize {
		self.in_flight_count.load(Ordering::SeqCst)
	}
}
