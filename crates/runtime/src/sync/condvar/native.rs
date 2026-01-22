// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Native Condvar implementation using parking_lot.

use std::time::Duration;

use crate::sync::mutex::MutexGuard;

/// Native condition variable implementation wrapping parking_lot::Condvar.
#[derive(Debug)]
pub struct CondvarInner {
	inner: parking_lot::Condvar,
}

impl CondvarInner {
	/// Creates a new condition variable.
	pub fn new() -> Self {
		Self {
			inner: parking_lot::Condvar::new(),
		}
	}

	/// Blocks the current thread until notified.
	pub fn wait<'a, T>(&self, guard: &mut MutexGuard<'a, T>) {
		self.inner.wait(&mut guard.inner.inner);
	}

	/// Blocks the current thread until notified or the timeout expires.
	/// Returns true if timed out.
	pub fn wait_for<'a, T>(&self, guard: &mut MutexGuard<'a, T>, timeout: Duration) -> bool {
		self.inner.wait_for(&mut guard.inner.inner, timeout).timed_out()
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

impl Default for CondvarInner {
	fn default() -> Self {
		Self::new()
	}
}
