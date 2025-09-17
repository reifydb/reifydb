// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Worker interface for centralized task management
//!
//! This module provides the interface for a global worker pool that can be used
//! by various components to schedule and manage background tasks.

use std::{
	fmt::{self, Display, Formatter},
	ops::Deref,
	time::Duration,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
	Low = 0,
	Normal = 1,
	High = 2,
}

/// Handle to a scheduled task in the worker pool
#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct TaskHandle(pub u64);

impl Display for TaskHandle {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for TaskHandle {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for TaskHandle {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<TaskHandle> for u64 {
	fn from(value: TaskHandle) -> Self {
		value.0
	}
}

impl From<u64> for TaskHandle {
	fn from(value: u64) -> Self {
		TaskHandle(value)
	}
}

#[derive(Clone, Default)]
pub struct TaskContext {
	// For now, keeping this simple without engine reference
	// The engine can be passed through other means if needed
}

impl TaskContext {
	pub fn new() -> Self {
		Self::default()
	}
}

pub trait SchedulableTask: Send + Sync {
	fn execute(&self, ctx: &TaskContext) -> reifydb_core::Result<()>;
	fn name(&self) -> &str;
	fn priority(&self) -> Priority;
}

pub type BoxedTask = Box<dyn SchedulableTask>;

/// Adapter to convert a closure into a SchedulableTask
pub struct ClosureTask<F>
where
	F: Fn(&TaskContext) -> reifydb_core::Result<()> + Send + Sync,
{
	name: String,
	priority: Priority,
	task: F,
}

impl<F> ClosureTask<F>
where
	F: Fn(&TaskContext) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(name: impl Into<String>, priority: Priority, task: F) -> Self {
		Self {
			name: name.into(),
			priority,
			task,
		}
	}
}

impl<F> SchedulableTask for ClosureTask<F>
where
	F: Fn(&TaskContext) -> reifydb_core::Result<()> + Send + Sync,
{
	fn execute(&self, ctx: &TaskContext) -> reifydb_core::Result<()> {
		(self.task)(ctx)
	}

	fn name(&self) -> &str {
		&self.name
	}

	fn priority(&self) -> Priority {
		self.priority
	}
}

pub trait Scheduler: Send + Sync {
	/// Schedule a task to run at fixed intervals
	///
	/// The task will be scheduled to run every `interval` duration.
	/// The next execution time is calculated when the task is picked up
	/// for execution (not when it completes). This means if a task takes
	/// longer than its interval, multiple instances may be queued.
	fn schedule_every(&self, task: BoxedTask, interval: Duration) -> reifydb_core::Result<TaskHandle>;

	/// Cancel a scheduled task
	fn cancel(&self, handle: TaskHandle) -> reifydb_core::Result<()>;
}
