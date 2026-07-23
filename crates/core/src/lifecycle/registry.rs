// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem::take, sync::Arc};

use reifydb_runtime::sync::mutex::Mutex;

use crate::lifecycle::task::LifecycleTask;

#[derive(Clone)]
pub struct LifecycleRegistry {
	tasks: Arc<Mutex<Vec<Box<dyn LifecycleTask>>>>,
}

impl LifecycleRegistry {
	pub fn new() -> Self {
		Self {
			tasks: Arc::new(Mutex::new(Vec::new())),
		}
	}

	pub fn register(&self, task: Box<dyn LifecycleTask>) {
		self.tasks.lock().push(task);
	}

	pub fn take(&self) -> Vec<Box<dyn LifecycleTask>> {
		take(&mut *self.tasks.lock())
	}

	pub fn is_empty(&self) -> bool {
		self.tasks.lock().is_empty()
	}

	pub fn len(&self) -> usize {
		self.tasks.lock().len()
	}
}

impl Default for LifecycleRegistry {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::duration::Duration;

	use super::*;
	use crate::lifecycle::progress::Progress;

	struct CountingTask {
		remaining: usize,
		slices: Arc<Mutex<usize>>,
	}

	impl LifecycleTask for CountingTask {
		fn name(&self) -> &'static str {
			"counting"
		}

		fn interval(&self) -> Duration {
			Duration::from_seconds(1).unwrap()
		}

		fn run_slice(&mut self) -> Progress {
			*self.slices.lock() += 1;
			if self.remaining > 0 {
				self.remaining -= 1;
			}
			if self.remaining == 0 {
				Progress::Exhausted
			} else {
				Progress::Yielded
			}
		}
	}

	#[test]
	fn registry_collects_and_drains_tasks() {
		let registry = LifecycleRegistry::new();
		assert!(registry.is_empty(), "a fresh registry holds no tasks");
		registry.register(Box::new(CountingTask {
			remaining: 1,
			slices: Arc::new(Mutex::new(0)),
		}));
		assert!(!registry.is_empty(), "a registered task must be visible before the subsystem drains it");
		let taken = registry.take();
		assert_eq!(taken.len(), 1, "take must hand every registered task to the subsystem exactly once");
		assert!(registry.is_empty(), "take must empty the registry so the subsystem is the sole owner");
	}

	#[test]
	fn run_slice_reports_yielded_until_work_is_exhausted() {
		// The lifecycle actor reschedules a catch-up only while a task reports Yielded; a task that
		// under-reports (returns Exhausted with work left) would silently strand its backlog until the
		// next full interval, so the budget/yield contract is the load-bearing invariant here.
		let slices = Arc::new(Mutex::new(0usize));
		let mut task = CountingTask {
			remaining: 3,
			slices: slices.clone(),
		};
		assert_eq!(task.run_slice(), Progress::Yielded, "first of three slices still has work left");
		assert_eq!(task.run_slice(), Progress::Yielded, "second slice still has work left");
		assert_eq!(task.run_slice(), Progress::Exhausted, "the final slice must report Exhausted");
		assert_eq!(*slices.lock(), 3, "each slice must run exactly once");
	}
}
