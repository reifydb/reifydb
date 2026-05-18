// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	fmt,
	sync::atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(u64);

impl TaskId {
	pub fn new() -> Self {
		static COUNTER: AtomicU64 = AtomicU64::new(1);
		Self(COUNTER.fetch_add(1, Ordering::Relaxed))
	}
}

impl Default for TaskId {
	fn default() -> Self {
		Self::new()
	}
}

impl fmt::Display for TaskId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "task-{}", self.0)
	}
}
