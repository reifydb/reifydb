// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use cfg_if::cfg_if;
use reifydb_value::value::duration::Duration;

use crate::sync::mutex::MutexGuard;

#[cfg(all(not(reifydb_single_threaded), not(loom)))]
pub mod host;
#[cfg(loom)]
pub mod loom;
#[cfg(reifydb_single_threaded)]
pub mod wasm;

cfg_if! {
	if #[cfg(loom)] {
		type CondvarInner = loom::CondvarInner;
	} else if #[cfg(not(reifydb_single_threaded))] {
		type CondvarInner = host::CondvarInner;
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
		let timed_out = self.inner.wait_for(guard, timeout.to_std());
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
