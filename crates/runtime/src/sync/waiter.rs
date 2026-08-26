// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt, sync::Arc};

use reifydb_value::value::duration::Duration;

#[cfg(not(reifydb_single_threaded))]
use crate::context::clock::{MockClock, TimerId};
use crate::{
	context::clock::{Clock, TimerWake},
	sync::{condvar::Condvar, mutex::Mutex},
};

#[cfg(not(reifydb_single_threaded))]
const MOCK_PARK_BACKSTOP: Duration = Duration::from_seconds_const(30);

struct Signal {
	notified: Mutex<bool>,
	condvar: Condvar,
}

impl Signal {
	fn new() -> Self {
		Self {
			notified: Mutex::new(false),
			condvar: Condvar::new(),
		}
	}
}

impl TimerWake for Signal {
	fn wake(&self) {
		let _guard = self.notified.lock();
		self.condvar.notify_all();
	}
}

pub struct WaiterHandle {
	signal: Arc<Signal>,
	#[cfg_attr(reifydb_single_threaded, allow(dead_code))]
	clock: Option<Clock>,
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
			signal: Arc::new(Signal::new()),
			clock: None,
			on_notify: Mutex::new(None),
		}
	}

	pub fn on_clock(clock: Clock) -> Self {
		Self {
			signal: Arc::new(Signal::new()),
			clock: Some(clock),
			on_notify: Mutex::new(None),
		}
	}

	pub fn with_callback(callback: Box<dyn FnOnce() + Send>) -> Self {
		Self {
			signal: Arc::new(Signal::new()),
			clock: None,
			on_notify: Mutex::new(Some(callback)),
		}
	}

	pub fn notify(&self) {
		let mut guard = self.signal.notified.lock();
		*guard = true;
		self.signal.condvar.notify_one();
		drop(guard);
		if let Some(callback) = self.on_notify.lock().take() {
			callback();
		}
	}

	pub fn wait_timeout(&self, timeout: Duration) -> bool {
		#[cfg(not(reifydb_single_threaded))]
		if let Some(mock) = self.clock.as_ref().and_then(Clock::as_mock) {
			return self.wait_until_virtual_deadline(mock, timeout);
		}

		let mut guard = self.signal.notified.lock();
		if *guard {
			return true;
		}
		!self.signal.condvar.wait_for(&mut guard, timeout).timed_out()
	}

	#[cfg(not(reifydb_single_threaded))]
	fn wait_until_virtual_deadline(&self, mock: &MockClock, timeout: Duration) -> bool {
		let deadline = mock.now().to_nanos().saturating_add(nanos_of(timeout));
		let _timer = TimerGuard::register(mock, deadline, self.signal.clone());

		let mut guard = self.signal.notified.lock();
		loop {
			if *guard {
				return true;
			}
			if mock.now().to_nanos() >= deadline {
				return false;
			}
			if self.signal.condvar.wait_for(&mut guard, MOCK_PARK_BACKSTOP).timed_out() {
				panic!("mock clock never advanced past the park deadline");
			}
		}
	}
}

#[cfg(not(reifydb_single_threaded))]
fn nanos_of(timeout: Duration) -> u64 {
	timeout.to_std().as_nanos().min(u64::MAX as u128) as u64
}

#[cfg(not(reifydb_single_threaded))]
struct TimerGuard<'a> {
	clock: &'a MockClock,
	id: TimerId,
}

#[cfg(not(reifydb_single_threaded))]
impl<'a> TimerGuard<'a> {
	fn register(clock: &'a MockClock, deadline_nanos: u64, signal: Arc<Signal>) -> Self {
		Self {
			clock,
			id: clock.register_timer(deadline_nanos, signal),
		}
	}
}

#[cfg(not(reifydb_single_threaded))]
impl Drop for TimerGuard<'_> {
	fn drop(&mut self) {
		self.clock.cancel_timer(self.id);
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
