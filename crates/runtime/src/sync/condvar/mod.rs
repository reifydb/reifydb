// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use cfg_if::cfg_if;

use crate::sync::mutex::MutexGuard;

#[cfg(not(reifydb_single_threaded))]
pub mod native;
#[cfg(reifydb_single_threaded)]
pub mod wasm;

cfg_if! {
	if #[cfg(not(reifydb_single_threaded))] {
		type CondvarInner = native::CondvarInner;
	} else {
		type CondvarInner = wasm::CondvarInner;
	}
}

pub struct WaitTimeoutResult {
	timed_out: bool,
}

impl WaitTimeoutResult {
	#[inline]
	pub fn timed_out(&self) -> bool {
		self.timed_out
	}
}

#[derive(Debug)]
pub struct Condvar {
	inner: CondvarInner,
}

impl Condvar {
	#[inline]
	pub fn new() -> Self {
		Self {
			inner: CondvarInner::new(),
		}
	}

	#[inline]
	pub fn wait<'a, T>(&self, guard: &mut MutexGuard<'a, T>) {
		self.inner.wait(guard);
	}

	#[inline]
	pub fn wait_for<'a, T>(&self, guard: &mut MutexGuard<'a, T>, timeout: Duration) -> WaitTimeoutResult {
		let timed_out = self.inner.wait_for(guard, timeout);
		WaitTimeoutResult {
			timed_out,
		}
	}

	#[inline]
	pub fn notify_one(&self) {
		self.inner.notify_one();
	}

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
