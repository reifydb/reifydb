// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt;

use reifydb_value::value::duration::Duration;

use crate::sync::{condvar::Condvar, mutex::Mutex};

pub struct WaiterHandle {
	notified: Mutex<bool>,
	condvar: Condvar,
	on_notify: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl fmt::Debug for WaiterHandle {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("WaiterHandle").finish_non_exhaustive()
	}
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
			on_notify: Mutex::new(None),
		}
	}

	pub fn with_callback(callback: Box<dyn FnOnce() + Send>) -> Self {
		Self {
			notified: Mutex::new(false),
			condvar: Condvar::new(),
			on_notify: Mutex::new(Some(callback)),
		}
	}

	pub fn notify(&self) {
		let mut guard = self.notified.lock();
		*guard = true;
		self.condvar.notify_one();
		drop(guard);
		if let Some(callback) = self.on_notify.lock().take() {
			callback();
		}
	}

	pub fn wait_timeout(&self, timeout: Duration) -> bool {
		let mut guard = self.notified.lock();
		if *guard {
			return true;
		}
		!self.condvar.wait_for(&mut guard, timeout).timed_out()
	}
}

#[cfg(test)]
mod tests {
	use std::sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	};

	use super::*;

	#[test]
	fn callback_fires_exactly_once() {
		let count = Arc::new(AtomicUsize::new(0));
		let c = count.clone();
		let waiter = WaiterHandle::with_callback(Box::new(move || {
			c.fetch_add(1, Ordering::SeqCst);
		}));

		waiter.notify();
		waiter.notify();

		assert_eq!(count.load(Ordering::SeqCst), 1, "one-shot callback must fire exactly once");
		assert!(
			waiter.wait_timeout(Duration::from_milliseconds(0).unwrap()),
			"an already-notified waiter returns immediately"
		);
	}
}
