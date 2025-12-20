// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	cmp::Ordering,
	sync::Arc,
	time::{Duration, Instant},
};

use reifydb_core::Result;
use reifydb_engine::StandardEngine;
use reifydb_sub_api::{Priority, TaskHandle};

use crate::tracker::CancellationToken;

/// Internal context provided to tasks during execution
pub struct InternalTaskContext {
	/// Cancellation token for this task
	pub cancel_token: Option<CancellationToken>,
	/// Engine for database access
	pub engine: StandardEngine,
}

impl InternalTaskContext {
	/// Check if task has been cancelled
	pub fn is_cancelled(&self) -> bool {
		self.cancel_token.as_ref().map(|t| t.is_cancelled()).unwrap_or(false)
	}
}

/// Trait for tasks that can be executed by the worker pool
pub trait PoolTask: Send + Sync {
	/// Execute the task
	fn execute(&self, ctx: &InternalTaskContext) -> Result<()>;

	/// Get the priority of this task
	fn priority(&self) -> Priority {
		Priority::Normal
	}

	/// Get a name/description for this task for debugging
	fn name(&self) -> &str {
		"unnamed_task"
	}

	/// Whether this task can be retried on failure
	fn can_retry(&self) -> bool {
		false
	}

	/// Maximum number of retries if can_retry is true
	fn max_retries(&self) -> usize {
		3
	}
}

/// Internal representation of a scheduled task
pub(crate) struct ScheduledTask {
	pub handle: TaskHandle,
	pub task: Arc<dyn PoolTask>,
	pub next_run: Instant,
	pub interval: Option<Duration>,
	pub priority: Priority,
}

impl ScheduledTask {
	pub fn new(
		handle: TaskHandle,
		task: Box<dyn PoolTask>,
		next_run: Instant,
		interval: Option<Duration>,
		priority: Priority,
	) -> Self {
		Self {
			handle,
			task: Arc::from(task),
			next_run,
			interval,
			priority,
		}
	}
}

/// Wrapper for periodic tasks
pub struct PeriodicTask {
	inner: Arc<dyn PoolTask>,
	interval: Duration,
	priority: Priority,
}

impl PeriodicTask {
	pub fn new(task: Arc<dyn PoolTask>, interval: Duration, priority: Priority) -> Self {
		Self {
			inner: task,
			interval,
			priority,
		}
	}
}

impl PoolTask for PeriodicTask {
	fn execute(&self, ctx: &InternalTaskContext) -> Result<()> {
		self.inner.execute(ctx)
	}

	fn priority(&self) -> Priority {
		self.priority
	}

	fn name(&self) -> &str {
		self.inner.name()
	}
}

/// A prioritized task wrapper for the queue
pub struct PrioritizedTask {
	pub task: Box<dyn PoolTask>,
	pub priority: Priority,
	pub submitted_at: Instant,
}

impl PrioritizedTask {
	pub fn new(task: Box<dyn PoolTask>) -> Self {
		let priority = task.priority();
		Self {
			task,
			priority,
			submitted_at: Instant::now(),
		}
	}
}

impl PartialEq for PrioritizedTask {
	fn eq(&self, other: &Self) -> bool {
		self.priority == other.priority && self.submitted_at == other.submitted_at
	}
}

impl Eq for PrioritizedTask {}

impl PartialOrd for PrioritizedTask {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for PrioritizedTask {
	fn cmp(&self, other: &Self) -> Ordering {
		// Higher priority first (High=2 should be greater than Low=0)
		match self.priority.cmp(&other.priority) {
			Ordering::Equal => {
				// Earlier submitted first (FIFO within same
				// priority) Reverse this so earlier tasks
				// are "greater" and pop first
				other.submitted_at.cmp(&self.submitted_at)
			}
			other => other,
		}
	}
}

/// Simple closure-based implementation for PoolTask (internal use)
pub struct InternalClosureTask<F>
where
	F: Fn(&InternalTaskContext) -> Result<()> + Send + Sync,
{
	name: String,
	priority: Priority,
	closure: F,
}

impl<F> InternalClosureTask<F>
where
	F: Fn(&InternalTaskContext) -> Result<()> + Send + Sync,
{
	pub fn new(name: impl Into<String>, priority: Priority, closure: F) -> Self {
		Self {
			name: name.into(),
			priority,
			closure,
		}
	}
}

impl<F> PoolTask for InternalClosureTask<F>
where
	F: Fn(&InternalTaskContext) -> Result<()> + Send + Sync,
{
	fn execute(&self, ctx: &InternalTaskContext) -> Result<()> {
		(self.closure)(ctx)
	}

	fn priority(&self) -> Priority {
		self.priority
	}

	fn name(&self) -> &str {
		&self.name
	}
}
