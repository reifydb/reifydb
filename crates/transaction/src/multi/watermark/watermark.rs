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
	fmt::Debug,
	ops::Deref,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	time::Duration,
};

use reifydb_core::common::CommitVersion;

#[cfg(feature = "native")]
use reifydb_runtime::sync::condvar::native::Condvar;
#[cfg(feature = "wasm")]
use reifydb_runtime::sync::condvar::wasm::Condvar;

#[cfg(feature = "native")]
use reifydb_runtime::sync::mutex::native::Mutex;
#[cfg(feature = "wasm")]
use reifydb_runtime::sync::mutex::wasm::Mutex;

#[cfg(feature = "native")]
use reifydb_runtime::worker::native::WorkerThread;

use tracing::instrument;

use crate::multi::watermark::closer::Closer;

// WASM: use direct processing with RefCell
#[cfg(feature = "wasm")]
use std::cell::RefCell;

// WASM-only: Sync wrapper for RefCell (safe because WASM is single-threaded)
#[cfg(feature = "wasm")]
pub(crate) struct SyncRefCell<T>(RefCell<T>);

#[cfg(feature = "wasm")]
unsafe impl<T> Sync for SyncRefCell<T> {}

#[cfg(feature = "wasm")]
impl<T> SyncRefCell<T> {
	fn new(value: T) -> Self {
		Self(RefCell::new(value))
	}

	fn borrow_mut(&self) -> std::cell::RefMut<'_, T> {
		self.0.borrow_mut()
	}
}

// Native implementation uses crossbeam channels
#[cfg(feature = "native")]
use crossbeam_channel::{Sender, unbounded};

#[cfg(feature = "native")]
pub struct WatermarkInner {
	pub(crate) done_until: AtomicU64,
	pub(crate) last_index: AtomicU64,
	pub(crate) tx: Sender<Mark>,
	// Worker thread is stored but not used directly - it's managed via Drop
	pub(crate) _worker: Mutex<Option<WorkerThread<()>>>,
}

// WASM implementation uses direct processing
#[cfg(feature = "wasm")]
pub struct WatermarkInner {
	pub(crate) done_until: AtomicU64,
	pub(crate) last_index: AtomicU64,
	pub(crate) processor: SyncRefCell<crate::multi::watermark::process::WatermarkProcessor>,
}

#[derive(Debug)]
pub(crate) struct Mark {
	pub(crate) version: u64,
	pub(crate) waiter: Option<Arc<WaiterHandle>>,
	pub(crate) done: bool,
}

/// Handle for waiting on a specific version to complete
#[derive(Debug)]
pub(crate) struct WaiterHandle {
	notified: Mutex<bool>,
	condvar: Condvar,
}

impl WaiterHandle {
	#[allow(dead_code)]
	fn new() -> Self {
		Self {
			notified: Mutex::new(false),
			condvar: Condvar::new(),
		}
	}

	pub(crate) fn notify(&self) {
		let mut guard = self.notified.lock();
		*guard = true;
		self.condvar.notify_one();
	}

	#[allow(dead_code)]
	fn wait_timeout(&self, timeout: Duration) -> bool {
		let mut guard = self.notified.lock();
		if *guard {
			return true;
		}
		!self.condvar.wait_for(&mut guard, timeout).timed_out()
	}
}

/// WaterMark is used to keep track of the minimum un-finished index. Typically,
/// an index k becomes finished or "done" according to a WaterMark once
/// `done(k)` has been called
///  1. as many times as `begin(k)` has, AND
///  2. a positive number of times.
pub struct WaterMark(Arc<WatermarkInner>);

impl Debug for WaterMark {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("WaterMark")
			.field("done_until", &self.done_until.load(Ordering::Relaxed))
			.field("last_index", &self.last_index.load(Ordering::Relaxed))
			.finish()
	}
}

impl Deref for WaterMark {
	type Target = WatermarkInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl WaterMark {
	/// Create a new WaterMark with given name and closer.
	#[cfg(feature = "native")]
	#[instrument(name = "transaction::watermark::new", level = "debug", skip(closer), fields(task_name = %task_name))]
	pub fn new(task_name: String, closer: Closer) -> Self {
		let (tx, rx) = unbounded();

		let inner = Arc::new(WatermarkInner {
			done_until: AtomicU64::new(0),
			last_index: AtomicU64::new(0),
			tx,
			_worker: Mutex::new(None),
		});

		let processing_inner = inner.clone();

		// Spawn worker thread using WorkerThread abstraction
		// We use WorkerThread<()> since we manage our own crossbeam channel
		let worker = WorkerThread::spawn(task_name, move |_receiver| {
			processing_inner.process(rx, closer);
		});

		*inner._worker.lock() = Some(worker);

		Self(inner)
	}

	/// Create a new WaterMark with given name and closer.
	#[cfg(feature = "wasm")]
	#[instrument(name = "transaction::watermark::new", level = "debug", skip(closer, _task_name))]
	pub fn new(_task_name: String, closer: Closer) -> Self {
		use crate::multi::watermark::process::WatermarkProcessor;

		let inner = Arc::new(WatermarkInner {
			done_until: AtomicU64::new(0),
			last_index: AtomicU64::new(0),
			processor: SyncRefCell::new(WatermarkProcessor::new(closer)),
		});

		Self(inner)
	}

	/// Sets the last index to the given value.
	#[cfg(feature = "native")]
	#[instrument(name = "transaction::watermark::begin", level = "trace", skip(self), fields(version = version.0))]
	pub fn begin(&self, version: CommitVersion) {
		// Update last_index to the maximum
		self.last_index.fetch_max(version.0, Ordering::SeqCst);

		let _ = self.tx.send(Mark {
			version: version.0,
			waiter: None,
			done: false,
		});
	}

	/// Sets the last index to the given value.
	#[cfg(feature = "wasm")]
	#[instrument(name = "transaction::watermark::begin", level = "trace", skip(self), fields(version = version.0))]
	pub fn begin(&self, version: CommitVersion) {
		// Update last_index to the maximum
		self.last_index.fetch_max(version.0, Ordering::SeqCst);

		self.processor.borrow_mut().process_mark(Mark {
			version: version.0,
			waiter: None,
			done: false,
		}, &self.done_until);
	}

	/// Sets a single version as done.
	#[cfg(feature = "native")]
	#[instrument(name = "transaction::watermark::done", level = "trace", skip(self), fields(index = version.0))]
	pub fn done(&self, version: CommitVersion) {
		let _ = self.tx.send(Mark {
			version: version.0,
			waiter: None,
			done: true,
		});
	}

	/// Sets a single version as done.
	#[cfg(feature = "wasm")]
	#[instrument(name = "transaction::watermark::done", level = "trace", skip(self), fields(index = version.0))]
	pub fn done(&self, version: CommitVersion) {
		self.processor.borrow_mut().process_mark(Mark {
			version: version.0,
			waiter: None,
			done: true,
		}, &self.done_until);
	}

	/// Returns the maximum index that has the property that all indices
	/// less than or equal to it are done.
	pub fn done_until(&self) -> CommitVersion {
		CommitVersion(self.done_until.load(Ordering::SeqCst))
	}

	/// Waits until the given index is marked as done with a default
	/// timeout.
	pub fn wait_for_mark(&self, index: u64) {
		self.wait_for_mark_timeout(CommitVersion(index), Duration::from_secs(30));
	}

	/// Waits until the given index is marked as done with a specified
	/// timeout.
	#[cfg(feature = "native")]
	pub fn wait_for_mark_timeout(&self, index: CommitVersion, timeout: Duration) -> bool {
		if self.done_until.load(Ordering::SeqCst) >= index.0 {
			return true;
		}

		let waiter = Arc::new(WaiterHandle::new());

		if self.tx
			.send(Mark {
				version: index.0,
				waiter: Some(waiter.clone()),
				done: false,
			})
			.is_err()
		{
			// Channel closed
			return false;
		}

		// Wait with timeout using condvar
		waiter.wait_timeout(timeout)
	}

	/// Waits until the given index is marked as done with a specified
	/// timeout.
	#[cfg(feature = "wasm")]
	pub fn wait_for_mark_timeout(&self, index: CommitVersion, _timeout: Duration) -> bool {
		// In WASM, processing is synchronous so marks are immediately processed
		// Just check if the version is already done
		self.done_until.load(Ordering::SeqCst) >= index.0
	}
}

#[cfg(test)]
pub mod tests {
	use std::{
		sync::atomic::AtomicUsize,
		thread::sleep,
		time::Duration,
	};

	#[cfg(feature = "native")]
	use reifydb_runtime::time::native::Instant;
	#[cfg(feature = "wasm")]
	use reifydb_runtime::time::wasm::Instant;

	use super::*;
	use crate::multi::watermark::OLD_VERSION_THRESHOLD;

	#[test]
	fn test_basic() {
		init_and_close(|_| {});
	}

	#[test]
	fn test_begin_done() {
		init_and_close(|watermark| {
			watermark.begin(CommitVersion(1));
			watermark.begin(CommitVersion(2));
			watermark.begin(CommitVersion(3));

			watermark.done(CommitVersion(1));
			watermark.done(CommitVersion(2));
			watermark.done(CommitVersion(3));
		});
	}

	#[test]
	fn test_wait_for_mark() {
		init_and_close(|watermark| {
			watermark.begin(CommitVersion(1));
			watermark.begin(CommitVersion(2));
			watermark.begin(CommitVersion(3));

			watermark.done(CommitVersion(2));
			watermark.done(CommitVersion(3));

			assert_eq!(watermark.done_until(), 0);

			watermark.done(CommitVersion(1));
			watermark.wait_for_mark(1);
			watermark.wait_for_mark(3);
			assert_eq!(watermark.done_until(), 3);
		});
	}

	#[test]
	fn test_done_until() {
		init_and_close(|watermark| {
			watermark.done_until.store(1, Ordering::SeqCst);
			assert_eq!(watermark.done_until(), 1);
		});
	}

	#[test]
	fn test_high_concurrency() {
		let closer = Closer::new(1);
		let watermark = Arc::new(WaterMark::new("concurrent".into(), closer.clone()));

		const NUM_TASKS: usize = 50;
		const OPS_PER_TASK: usize = 100;

		let mut handles = vec![];

		// Spawn tasks that perform concurrent begin/done operations
		for task_id in 0..NUM_TASKS {
			let wm = watermark.clone();
			let handle = std::thread::spawn(move || {
				for i in 0..OPS_PER_TASK {
					let version = CommitVersion((task_id * OPS_PER_TASK + i) as u64 + 1);
					wm.begin(version);
					wm.done(version);
				}
			});
			handles.push(handle);
		}

		for handle in handles {
			handle.join().unwrap();
		}

		sleep(Duration::from_millis(100));

		// Verify the watermark progressed
		let final_done = watermark.done_until();
		assert!(final_done.0 > 0, "Watermark should have progressed");

		closer.signal_and_wait();
	}

	#[test]
	fn test_concurrent_wait_for_mark() {
		let closer = Closer::new(1);
		let watermark = Arc::new(WaterMark::new("wait_concurrent".into(), closer.clone()));
		let success_count = Arc::new(AtomicUsize::new(0));

		// Start some versions
		for i in 1..=10 {
			watermark.begin(CommitVersion(i));
		}

		let mut handles = vec![];

		// Spawn tasks that wait for marks
		for version in 1..=10 {
			let wm = watermark.clone();
			let counter = success_count.clone();
			let handle = std::thread::spawn(move || {
				// Use timeout to avoid hanging if something goes wrong
				if wm.wait_for_mark_timeout(CommitVersion(version), Duration::from_secs(5)) {
					counter.fetch_add(1, Ordering::Relaxed);
				}
			});
			handles.push(handle);
		}

		// Give tasks time to start waiting
		sleep(Duration::from_millis(50));

		// Complete the versions
		for i in 1..=10 {
			watermark.done(CommitVersion(i));
		}

		for handle in handles {
			handle.join().unwrap();
		}

		// All waits should have succeeded
		assert_eq!(success_count.load(Ordering::Relaxed), 10);

		closer.signal_and_wait();
	}

	#[test]
	fn test_old_version_rejection() {
		init_and_close(|watermark| {
			// Advance done_until significantly
			for i in 1..=100 {
				watermark.begin(CommitVersion(i));
				watermark.done(CommitVersion(i));
			}

			// Wait for processing
			sleep(Duration::from_millis(50));

			let done_until = watermark.done_until();
			assert!(done_until.0 >= 50, "Should have processed many versions");

			// Try to wait for a very old version (should return immediately)
			let very_old = done_until.0.saturating_sub(OLD_VERSION_THRESHOLD + 10);
			let start = Instant::now();
			watermark.wait_for_mark(very_old);
			let elapsed = start.elapsed();

			// Should return almost immediately (< 10ms)
			assert!(elapsed.as_millis() < 10, "Old version wait should return immediately");
		});
	}

	#[test]
	fn test_timeout_behavior() {
		init_and_close(|watermark| {
			// Begin but don't complete a version
			watermark.begin(CommitVersion(1));

			// Wait with short timeout
			let start = Instant::now();
			let result = watermark.wait_for_mark_timeout(CommitVersion(1), Duration::from_millis(100));
			let elapsed = start.elapsed();

			// Should timeout and return false
			assert!(!result, "Should timeout waiting for uncompleted version");
			assert!(
				elapsed.as_millis() >= 100 && elapsed.as_millis() < 200,
				"Should respect timeout duration"
			);
		});
	}

	#[test]
	fn test_out_of_order_begin() {
		// Test that begin() calls can arrive out of order with gap-tolerant processing
		init_and_close(|watermark| {
			// Begin versions out of order
			watermark.begin(CommitVersion(3));
			watermark.begin(CommitVersion(1));
			watermark.begin(CommitVersion(2));

			// Complete in order
			watermark.done(CommitVersion(1));
			watermark.done(CommitVersion(2));
			watermark.done(CommitVersion(3));

			// Wait for processing
			sleep(Duration::from_millis(50));

			// Watermark should advance to 3
			let done = watermark.done_until();
			assert_eq!(done.0, 3, "Watermark should advance to 3, got {}", done.0);
		});
	}

	#[test]
	fn test_orphaned_done_before_begin() {
		// Test that done() arriving before begin() is handled correctly
		init_and_close(|watermark| {
			// done() arrives before begin() - this is an "orphaned" done
			watermark.done(CommitVersion(1));

			// Wait a bit for processing
			sleep(Duration::from_millis(20));

			// Watermark should NOT advance yet (begin hasn't arrived)
			assert_eq!(watermark.done_until().0, 0);

			// Now begin() arrives
			watermark.begin(CommitVersion(1));

			// Wait for processing
			sleep(Duration::from_millis(50));

			// Now watermark should advance
			let done = watermark.done_until();
			assert_eq!(done.0, 1, "Watermark should advance to 1 after begin, got {}", done.0);
		});
	}

	#[test]
	fn test_mixed_out_of_order() {
		// Test complex out-of-order scenario
		init_and_close(|watermark| {
			// Interleaved begin/done in various orders
			watermark.begin(CommitVersion(2));
			watermark.done(CommitVersion(3)); // orphaned
			watermark.begin(CommitVersion(1));
			watermark.done(CommitVersion(1));
			watermark.begin(CommitVersion(3));
			watermark.done(CommitVersion(2));

			// Wait for processing
			sleep(Duration::from_millis(50));

			// All versions complete, watermark should be at 3
			let done = watermark.done_until();
			assert_eq!(done.0, 3, "Watermark should advance to 3, got {}", done.0);
		});
	}

	fn init_and_close<F>(f: F)
	where
		F: FnOnce(Arc<WaterMark>),
	{
		let closer = Closer::new(1);

		let watermark = Arc::new(WaterMark::new("watermark".into(), closer.clone()));
		f(watermark);

		sleep(Duration::from_millis(10));
		closer.signal_and_wait();
	}
}
