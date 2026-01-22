// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Condvar synchronization primitive.

use std::time::Duration;

use crate::sync::mutex::MutexGuard;

#[cfg(reifydb_target = "native")]
pub mod native;
#[cfg(reifydb_target = "wasm")]
pub mod wasm;

cfg_if::cfg_if! {
	if #[cfg(reifydb_target = "native")] {
		type CondvarInner = native::CondvarInner;
	} else {
		type CondvarInner = wasm::CondvarInner;
	}
}

/// Result of a timed wait on a condition variable.
pub struct WaitTimeoutResult {
	timed_out: bool,
}

impl WaitTimeoutResult {
	/// Returns whether the wait timed out.
	#[inline]
	pub fn timed_out(&self) -> bool {
		self.timed_out
	}
}

/// A condition variable for coordinating threads.
#[derive(Debug)]
pub struct Condvar {
	inner: CondvarInner,
}

impl Condvar {
	/// Creates a new condition variable.
	#[inline]
	pub fn new() -> Self {
		Self {
			inner: CondvarInner::new(),
		}
	}

	/// Blocks the current thread until notified.
	#[inline]
	pub fn wait<'a, T>(&self, guard: &mut MutexGuard<'a, T>) {
		self.inner.wait(guard);
	}

	/// Blocks the current thread until notified or the timeout expires.
	#[inline]
	pub fn wait_for<'a, T>(&self, guard: &mut MutexGuard<'a, T>, timeout: Duration) -> WaitTimeoutResult {
		let timed_out = self.inner.wait_for(guard, timeout);
		WaitTimeoutResult {
			timed_out,
		}
	}

	/// Wakes up one blocked thread.
	#[inline]
	pub fn notify_one(&self) {
		self.inner.notify_one();
	}

	/// Wakes up all blocked threads.
	#[inline]
	pub fn notify_all(&self) {
		self.inner.notify_all();
	}
}

impl Default for Condvar {
	#[inline]
	fn default() -> Self {
		Self::new()
	}
}
