// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Worker pool interface for centralized task management
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

/// Interface for the worker pool (to avoid circular dependencies)
pub trait WorkerPool: Send + Sync {
	/// Schedule a periodic task
	fn schedule_periodic(
		&self,
		name: String,
		task: Box<dyn Fn() -> crate::Result<bool> + Send + Sync>,
		interval: Duration,
	) -> crate::Result<TaskHandle>;

	/// Cancel a scheduled task
	fn cancel(&self, handle: TaskHandle) -> crate::Result<()>;
}
