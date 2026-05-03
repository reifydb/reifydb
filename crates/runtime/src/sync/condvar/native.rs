// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use parking_lot::Condvar;

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
		self.inner.wait(&mut guard.inner.inner);
	}

	pub fn wait_for<'a, T>(&self, guard: &mut MutexGuard<'a, T>, timeout: Duration) -> bool {
		self.inner.wait_for(&mut guard.inner.inner, timeout).timed_out()
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
