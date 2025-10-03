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

use reifydb_engine::StandardEngine;

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

#[derive(Clone)]
pub struct TaskContext {
	engine: StandardEngine,
}

impl TaskContext {
	pub fn new(engine: StandardEngine) -> Self {
		Self {
			engine,
		}
	}

	pub fn engine(&self) -> &StandardEngine {
		&self.engine
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

/// Macro for creating tasks with less boilerplate
///
/// # Examples
///
/// ```ignore
/// // Minimal - anonymous task with Normal priority
/// let task = task!(|ctx| {
///     // task body
///     Ok(())
/// });
///
/// // With name only (Normal priority)
/// let task = task!("my_task", |ctx| {
///     // task body
///     Ok(())
/// });
///
/// // With name and priority
/// let task = task!("my_task", High, |ctx| {
///     // task body
///     Ok(())
/// });
///
/// // With priority only (anonymous task)
/// let task = task!(Low, |ctx| {
///     // task body
///     Ok(())
/// });
///
/// // With move semantics (works with all patterns)
/// let captured = 42;
/// let task = task!("my_task", move |ctx| {
///     println!("Captured: {}", captured);
///     Ok(())
/// });
/// ```
#[macro_export]
macro_rules! task {
	// Pattern: just closure (unnamed task with Normal priority)
	($closure:expr) => {
		Box::new($crate::ClosureTask::new("unnamed", $crate::Priority::Normal, $closure))
	};

	// Pattern: Priority literal (Low/Normal/High), closure - unnamed task
	(Low, $closure:expr) => {
		Box::new($crate::ClosureTask::new("unnamed", $crate::Priority::Low, $closure))
	};
	(Normal, $closure:expr) => {
		Box::new($crate::ClosureTask::new("unnamed", $crate::Priority::Normal, $closure))
	};
	(High, $closure:expr) => {
		Box::new($crate::ClosureTask::new("unnamed", $crate::Priority::High, $closure))
	};

	// Pattern: name (string literal), closure (Normal priority)
	($name:literal, $closure:expr) => {
		Box::new($crate::ClosureTask::new($name, $crate::Priority::Normal, $closure))
	};

	// Pattern: Priority literal, name (string literal), closure
	(Low, $name:literal, $closure:expr) => {
		Box::new($crate::ClosureTask::new($name, $crate::Priority::Low, $closure))
	};
	(Normal, $name:literal, $closure:expr) => {
		Box::new($crate::ClosureTask::new($name, $crate::Priority::Normal, $closure))
	};
	(High, $name:literal, $closure:expr) => {
		Box::new($crate::ClosureTask::new($name, $crate::Priority::High, $closure))
	};

	// Pattern: name (string literal), Priority literal, closure
	($name:literal, Low, $closure:expr) => {
		Box::new($crate::ClosureTask::new($name, $crate::Priority::Low, $closure))
	};
	($name:literal, Normal, $closure:expr) => {
		Box::new($crate::ClosureTask::new($name, $crate::Priority::Normal, $closure))
	};
	($name:literal, High, $closure:expr) => {
		Box::new($crate::ClosureTask::new($name, $crate::Priority::High, $closure))
	};

	// Pattern: Priority value (expr), closure - for when Priority is imported
	($priority:expr, $closure:expr) => {
		Box::new($crate::ClosureTask::new("unnamed", $priority, $closure))
	};

	// Pattern: name (expr), Priority value (expr), closure
	($name:expr, $priority:expr, $closure:expr) => {
		Box::new($crate::ClosureTask::new($name, $priority, $closure))
	};
}

pub trait Scheduler: Send + Sync {
	/// Schedule a task to run at fixed intervals
	///
	/// The task will be scheduled to run every `interval` duration.
	/// The next execution time is calculated when the task is picked up
	/// for execution (not when it completes). This means if a task takes
	/// longer than its interval, multiple instances may be queued.
	fn schedule_every(&self, interval: Duration, task: BoxedTask) -> reifydb_core::Result<TaskHandle>;

	/// Cancel a scheduled task
	fn cancel(&self, handle: TaskHandle) -> reifydb_core::Result<()>;
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::Priority::{High, Low, Normal};

	#[test]
	fn test_task_macro_minimal() {
		// Test minimal syntax: just closure (unnamed task with Normal priority)
		let task: BoxedTask = task!(|_ctx| { Ok(()) });

		assert_eq!(task.name(), "unnamed");
		assert_eq!(task.priority(), Normal);
	}

	#[test]
	fn test_task_macro_with_name() {
		// Test with name only (Normal priority)
		let task: BoxedTask = task!("test_task", |_ctx| { Ok(()) });

		assert_eq!(task.name(), "test_task");
		assert_eq!(task.priority(), Normal);
	}

	#[test]
	fn test_task_macro_with_priority() {
		// Test with priority only (unnamed task)
		let task: BoxedTask = task!(High, |_ctx| { Ok(()) });

		assert_eq!(task.name(), "unnamed");
		assert_eq!(task.priority(), High);
	}

	#[test]
	fn test_task_macro_priority_name() {
		// Test with priority first, then name
		let task: BoxedTask = task!(Low, "priority_first", |_ctx| { Ok(()) });

		assert_eq!(task.name(), "priority_first");
		assert_eq!(task.priority(), Low);
	}

	#[test]
	fn test_task_macro_name_priority() {
		// Test with name first, then priority
		let task: BoxedTask = task!("name_first", High, |_ctx| { Ok(()) });

		assert_eq!(task.name(), "name_first");
		assert_eq!(task.priority(), High);
	}

	#[test]
	fn test_task_macro_with_move_closure() {
		// Test with move closure and captured variables
		let captured_value = 42;
		let task: BoxedTask = task!("move_task", move |_ctx| {
			// Use captured value to ensure move semantics work
			let _val = captured_value;
			Ok(())
		});

		assert_eq!(task.name(), "move_task");
		assert_eq!(task.priority(), Normal);
	}

	#[test]
	fn test_task_macro_all_priorities() {
		// Test all priority levels
		let low_task: BoxedTask = task!(Low, |_ctx| { Ok(()) });
		let normal_task: BoxedTask = task!(Normal, |_ctx| { Ok(()) });
		let high_task: BoxedTask = task!(High, |_ctx| { Ok(()) });

		assert_eq!(low_task.priority(), Low);
		assert_eq!(normal_task.priority(), Normal);
		assert_eq!(high_task.priority(), High);
	}
}
