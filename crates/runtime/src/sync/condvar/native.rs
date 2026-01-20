// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Native Condvar implementation using parking_lot.

use std::time::Duration;

use crate::sync::mutex::native::MutexGuard;

/// Result of a timed wait on a condition variable.
pub struct WaitTimeoutResult {
	timed_out: bool,
}

impl WaitTimeoutResult {
	/// Returns whether the wait timed out.
	pub fn timed_out(&self) -> bool {
		self.timed_out
	}
}

/// A condition variable for coordinating threads.
///
/// Native implementation wraps parking_lot::Condvar.
#[derive(Debug)]
pub struct Condvar {
	inner: parking_lot::Condvar,
}

impl Condvar {
	/// Creates a new condition variable.
	pub fn new() -> Self {
		Self {
			inner: parking_lot::Condvar::new(),
		}
	}

	/// Blocks the current thread until notified.
	pub fn wait<'a, T>(&self, guard: &mut MutexGuard<'a, T>) {
		self.inner.wait(&mut guard.inner);
	}

	/// Blocks the current thread until notified or the timeout expires.
	pub fn wait_for<'a, T>(&self, guard: &mut MutexGuard<'a, T>, timeout: Duration) -> WaitTimeoutResult {
		let result = self.inner.wait_for(&mut guard.inner, timeout);
		WaitTimeoutResult {
			timed_out: result.timed_out(),
		}
	}

	/// Wakes up one blocked thread.
	pub fn notify_one(&self) {
		self.inner.notify_one();
	}

	/// Wakes up all blocked threads.
	pub fn notify_all(&self) {
		self.inner.notify_all();
	}
}

impl Default for Condvar {
	fn default() -> Self {
		Self::new()
	}
}
