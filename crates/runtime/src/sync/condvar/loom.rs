// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use loom::sync::Condvar;

use crate::sync::mutex::MutexGuard;

#[derive(Debug)]
pub struct CondvarInner {
	inner: Condvar,
}

impl CondvarInner {
	pub fn new() -> Self {
		Self {
			inner: Condvar::new(),
		}
	}

	pub fn wait<'a, T>(&self, guard: &mut MutexGuard<'a, T>) {
		let taken = guard.inner.take();
		let restored = self.inner.wait(taken).expect("loom mutex poisoned");
		guard.inner.restore(restored);
	}

	pub fn wait_for<'a, T>(&self, guard: &mut MutexGuard<'a, T>, timeout: Duration) -> bool {
		let taken = guard.inner.take();
		let (restored, result) = self.inner.wait_timeout(taken, timeout).expect("loom mutex poisoned");
		guard.inner.restore(restored);
		result.timed_out()
	}

	pub fn notify_one(&self) {
		self.inner.notify_one();
	}

	pub fn notify_all(&self) {
		self.inner.notify_all();
	}
}

impl Default for CondvarInner {
	fn default() -> Self {
		Self::new()
	}
}
