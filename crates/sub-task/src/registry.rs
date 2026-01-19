use std::{sync::Arc, time::Instant};

use dashmap::DashMap;

use crate::task::{ScheduledTask, TaskId};

/// Entry in the task registry tracking execution state
#[derive(Debug, Clone)]
pub struct TaskEntry {
	/// The task definition
	pub task: Arc<ScheduledTask>,
	/// When the task should next execute
	pub next_execution: Instant,
}

/// Thread-safe registry of scheduled tasks
pub type TaskRegistry = Arc<DashMap<TaskId, TaskEntry>>;

/// Information about a task for status queries
#[derive(Debug, Clone)]
pub struct TaskInfo {
	pub id: TaskId,
	pub name: String,
	pub next_execution: Instant,
}

impl TaskInfo {
	pub fn from_entry(id: TaskId, entry: &TaskEntry) -> Self {
		Self {
			id,
			name: entry.task.name.clone(),
			next_execution: entry.next_execution,
		}
	}
}
