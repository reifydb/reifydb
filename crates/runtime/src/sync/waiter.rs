// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use crate::sync::{condvar::Condvar, mutex::Mutex};

/// Handle for waiting on a specific version to complete
#[derive(Debug)]
pub struct WaiterHandle {
	notified: Mutex<bool>,
	condvar: Condvar,
}

impl Default for WaiterHandle {
	fn default() -> Self {
		Self::new()
	}
}

impl WaiterHandle {
	pub fn new() -> Self {
		Self {
			notified: Mutex::new(false),
			condvar: Condvar::new(),
		}
	}

	pub fn notify(&self) {
		let mut guard = self.notified.lock();
		*guard = true;
		self.condvar.notify_one();
	}

	pub fn wait_timeout(&self, timeout: Duration) -> bool {
		let mut guard = self.notified.lock();
		if *guard {
			return true;
		}
		!self.condvar.wait_for(&mut guard, timeout).timed_out()
	}
}
