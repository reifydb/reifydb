// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{
	ops::Deref,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

#[cfg(feature = "native")]
use crossbeam_channel::{Receiver, Sender, unbounded};

#[cfg(feature = "native")]
use reifydb_runtime::sync::condvar::native::Condvar;
#[cfg(feature = "wasm")]
use reifydb_runtime::sync::condvar::wasm::Condvar;

#[cfg(feature = "native")]
use reifydb_runtime::sync::mutex::native::Mutex;
#[cfg(feature = "wasm")]
use reifydb_runtime::sync::mutex::wasm::Mutex;
use reifydb_core::util::wait_group::WaitGroup;

/// Closer holds the two things we need to close a task and wait for it to
/// finish: a channel to tell the task to shut down, and a WaitGroup with
/// which to wait for it to finish shutting down.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Closer(Arc<CloserInner>);

impl Deref for Closer {
	type Target = CloserInner;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

#[cfg(feature = "native")]
#[derive(Debug)]
pub struct CloserInner {
	wg: WaitGroup,
	shutdown_tx: Sender<()>,
	pub(crate) shutdown_rx: Receiver<()>,
	shutdown_condvar: Condvar,
	shutdown_mutex: Mutex<bool>,
	signaled: AtomicBool,
	initial_count: usize,
}

#[cfg(feature = "wasm")]
#[derive(Debug)]
pub struct CloserInner {
	wg: WaitGroup,
	shutdown_condvar: Condvar,
	shutdown_mutex: Mutex<bool>,
	signaled: AtomicBool,
	#[allow(dead_code)]
	initial_count: usize,
}

impl CloserInner {
	#[cfg(feature = "native")]
	fn new() -> Self {
		let (shutdown_tx, shutdown_rx) = unbounded();
		Self {
			wg: WaitGroup::new(),
			shutdown_tx,
			shutdown_rx,
			shutdown_condvar: Condvar::new(),
			shutdown_mutex: Mutex::new(false),
			signaled: AtomicBool::new(false),
			initial_count: 0,
		}
	}

	#[cfg(feature = "wasm")]
	fn new() -> Self {
		Self {
			wg: WaitGroup::new(),
			shutdown_condvar: Condvar::new(),
			shutdown_mutex: Mutex::new(false),
			signaled: AtomicBool::new(false),
			initial_count: 0,
		}
	}

	#[cfg(feature = "native")]
	fn with(initial: usize) -> Self {
		let (shutdown_tx, shutdown_rx) = unbounded();
		Self {
			wg: WaitGroup::from(initial),
			shutdown_tx,
			shutdown_rx,
			shutdown_condvar: Condvar::new(),
			shutdown_mutex: Mutex::new(false),
			signaled: AtomicBool::new(false),
			initial_count: initial,
		}
	}

	#[cfg(feature = "wasm")]
	fn with(initial: usize) -> Self {
		Self {
			wg: WaitGroup::from(initial),
			shutdown_condvar: Condvar::new(),
			shutdown_mutex: Mutex::new(false),
			signaled: AtomicBool::new(false),
			initial_count: initial,
		}
	}
}

impl Default for Closer {
	fn default() -> Self {
		Self(Arc::new(CloserInner::new()))
	}
}

impl Closer {
	/// Constructs a new [`Closer`], with an initial count on the
	/// [`WaitGroup`].
	pub fn new(initial: usize) -> Self {
		Self(Arc::new(CloserInner::with(initial)))
	}

	/// Calls [`WaitGroup::done`] on the [`WaitGroup`].
	pub fn done(&self) {
		self.wg.done();
	}

	/// Signals the shutdown.
	#[cfg(feature = "native")]
	pub fn signal(&self) {
		// Only signal once
		if !self.signaled.swap(true, Ordering::AcqRel) {
			let mut guard = self.shutdown_mutex.lock();
			*guard = true;
			drop(guard);
			self.shutdown_condvar.notify_all();
			// Send shutdown signal to all waiting threads
			// We need to send one message per initial thread count
			for _ in 0..self.initial_count {
				let _ = self.shutdown_tx.send(());
			}
		}
	}

	/// Signals the shutdown.
	#[cfg(feature = "wasm")]
	pub fn signal(&self) {
		// Only signal once
		if !self.signaled.swap(true, Ordering::AcqRel) {
			let mut guard = self.shutdown_mutex.lock();
			*guard = true;
			drop(guard);
			self.shutdown_condvar.notify_all();
		}
	}

	/// Waits on the [`WaitGroup`]. (It waits for the Closer's initial
	/// value, [`Closer::add_running`], and [`Closer::done`]
	pub fn wait(&self) {
		self.wg.wait();
	}

	/// Calls [`Closer::signal`], then [`Closer::wait`].
	pub fn signal_and_wait(&self) {
		self.signal();
		self.wait();
	}

	/// Waits for the shutdown signal with a timeout.
	/// Returns true if shutdown was signaled, false if timeout occurred.
	pub fn wait_shutdown(&self, timeout: Duration) -> bool {
		let mut guard = self.shutdown_mutex.lock();
		if *guard {
			return true;
		}
		self.shutdown_condvar.wait_for(&mut guard, timeout).timed_out()
	}

	/// Returns true if signal() has been called.
	pub fn is_signaled(&self) -> bool {
		self.signaled.load(Ordering::Acquire)
	}
}

#[cfg(test)]
pub mod tests {
	use std::time::Duration;

	use crate::multi::watermark::closer::Closer;

	#[test]
	fn test_multiple_singles() {
		let closer = Closer::default();
		closer.signal();
		closer.signal();
		closer.signal_and_wait();

		let closer = Closer::new(1);
		closer.done();
		closer.signal_and_wait();
		closer.signal_and_wait();
		closer.signal();
	}

	#[test]
	#[cfg(feature = "native")]
	fn test_closer_single() {
		let closer = Closer::new(1);
		let tc = closer.clone();
		std::thread::spawn(move || {
			let rx = tc.shutdown_rx.clone();
			let _ = rx.recv();
			tc.done();
		});
		// Give tasks time to start
		std::thread::sleep(Duration::from_millis(10));
		closer.signal_and_wait();
	}

	#[test]
	#[cfg(feature = "native")]
	fn test_closer_many() {
		use crossbeam_channel::unbounded;

		let (tx, rx) = unbounded();

		// Create closer with count matching number of threads
		let c = Closer::new(10);

		for _ in 0..10 {
			let c = c.clone();
			let tx = tx.clone();
			std::thread::spawn(move || {
				let shutdown_rx = c.shutdown_rx.clone();
				// Wait for signal
				let _ = shutdown_rx.recv();
				tx.send(()).unwrap();
				// Signal that this thread is done
				c.done();
			});
		}

		// Give tasks time to start
		std::thread::sleep(Duration::from_millis(10));

		c.signal_and_wait();

		for _ in 0..10 {
			rx.recv_timeout(Duration::from_millis(100)).expect("timeout or channel closed");
		}
	}
}
