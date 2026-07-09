// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt,
	fmt::Debug,
	result::Result,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_core::common::CommitVersion;
use reifydb_runtime::sync::{mutex::Mutex, waiter::WaiterHandle};
use reifydb_value::value::duration::Duration;
use tracing::instrument;

use super::state::{WatermarkShared, WatermarkState};

pub struct WaterMark {
	state: Mutex<WatermarkState>,
	shared: WatermarkShared,
}

impl Debug for WaterMark {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("WaterMark")
			.field("done_until", &self.shared.done_until.load(Ordering::Relaxed))
			.field("last_index", &self.shared.last_index.load(Ordering::Relaxed))
			.finish()
	}
}

impl WaterMark {
	#[instrument(name = "transaction::watermark::new", level = "debug", fields(task_name = %task_name))]
	pub fn new(task_name: String) -> Self {
		Self {
			state: Mutex::new(WatermarkState::new()),
			shared: WatermarkShared {
				done_until: AtomicU64::new(0),
				last_index: AtomicU64::new(0),
			},
		}
	}

	#[instrument(name = "transaction::watermark::register_in_flight", level = "trace", skip(self), fields(version = version.0))]
	pub fn register_in_flight(&self, version: CommitVersion) {
		self.shared.last_index.fetch_max(version.0, Ordering::SeqCst);

		let mut to_notify = Vec::new();
		{
			let mut state = self.state.lock();
			state.process_begin(version.0, &self.shared.done_until, &mut to_notify);
		}
		for waiter in to_notify {
			waiter.notify();
		}
	}

	pub fn register_in_flight_with<E>(
		&self,
		version_fn: impl FnOnce() -> Result<CommitVersion, E>,
	) -> Result<CommitVersion, E> {
		let mut to_notify = Vec::new();
		let version = {
			let mut state = self.state.lock();
			let version = version_fn()?;
			self.shared.last_index.fetch_max(version.0, Ordering::SeqCst);
			state.process_begin(version.0, &self.shared.done_until, &mut to_notify);
			version
		};
		for waiter in to_notify {
			waiter.notify();
		}
		Ok(version)
	}

	#[instrument(name = "transaction::watermark::mark_finished", level = "trace", skip(self), fields(index = version.0))]
	pub fn mark_finished(&self, version: CommitVersion) {
		let mut to_notify = Vec::new();
		{
			let mut state = self.state.lock();
			state.process_done(version.0, &self.shared.done_until, &mut to_notify);
		}
		for waiter in to_notify {
			waiter.notify();
		}
	}

	pub fn done_until(&self) -> CommitVersion {
		CommitVersion(self.shared.done_until.load(Ordering::SeqCst))
	}

	pub fn last_index(&self) -> CommitVersion {
		CommitVersion(self.shared.last_index.load(Ordering::SeqCst))
	}

	pub fn advance_to(&self, version: CommitVersion) {
		self.shared.last_index.fetch_max(version.0, Ordering::SeqCst);
		self.shared.done_until.fetch_max(version.0, Ordering::SeqCst);
	}

	pub fn wait_for_mark(&self, index: u64) {
		self.wait_for_mark_timeout(CommitVersion(index), Duration::from_seconds(30).unwrap());
	}

	pub fn register_mark_waiter(&self, index: CommitVersion, waiter: Arc<WaiterHandle>) -> bool {
		let current_done = self.shared.done_until.load(Ordering::SeqCst);
		if current_done >= index.0 {
			waiter.notify();
			return true;
		}

		let mut to_notify = Vec::new();
		{
			let mut state = self.state.lock();
			state.register_waiter(index.0, waiter, &self.shared.done_until, &mut to_notify);
		}
		for waiter in to_notify {
			waiter.notify();
		}
		true
	}

	pub fn wait_for_mark_timeout(&self, index: CommitVersion, timeout: Duration) -> bool {
		let waiter = Arc::new(WaiterHandle::new());
		if !self.register_mark_waiter(index, waiter.clone()) {
			return false;
		}
		waiter.wait_timeout(timeout)
	}

	pub fn notify_on_mark(&self, index: CommitVersion, callback: Box<dyn FnOnce() + Send>) {
		let waiter = Arc::new(WaiterHandle::with_callback(callback));
		let _ = self.register_mark_waiter(index, waiter);
	}
}

#[cfg(test)]
pub mod tests {
	use std::{sync::atomic::AtomicUsize, thread, thread::sleep};

	use reifydb_runtime::context::clock::Clock;

	use super::*;
	use crate::multi::watermark::OLD_VERSION_THRESHOLD;

	#[test]
	fn test_basic() {
		let _watermark = WaterMark::new("watermark".into());
	}

	#[test]
	fn test_begin_done() {
		let watermark = WaterMark::new("watermark".into());
		watermark.register_in_flight(CommitVersion(1));
		watermark.register_in_flight(CommitVersion(2));
		watermark.register_in_flight(CommitVersion(3));

		watermark.mark_finished(CommitVersion(1));
		watermark.mark_finished(CommitVersion(2));
		watermark.mark_finished(CommitVersion(3));

		assert_eq!(watermark.done_until().0, 3);
	}

	#[test]
	fn register_in_flight_with_holds_frontier_below_the_acquired_version() {
		let watermark = WaterMark::new("watermark".into());
		watermark.register_in_flight(CommitVersion(1));
		watermark.register_in_flight(CommitVersion(2));
		watermark.register_in_flight(CommitVersion(3));
		watermark.mark_finished(CommitVersion(1));
		watermark.mark_finished(CommitVersion(2));
		watermark.mark_finished(CommitVersion(3));
		assert_eq!(watermark.done_until().0, 3);

		let acquired = watermark.register_in_flight_with(|| Ok::<_, ()>(CommitVersion(4))).unwrap();
		assert_eq!(acquired.0, 4, "the version computed inside the lock is returned");
		assert!(
			watermark.done_until().0 < 4,
			"a freshly acquired read snapshot must hold the frontier below it so its history cannot be evicted"
		);

		watermark.mark_finished(CommitVersion(4));
		assert_eq!(watermark.done_until().0, 4, "the frontier advances once the snapshot finishes");
	}

	#[test]
	fn test_wait_for_mark() {
		let watermark = WaterMark::new("watermark".into());
		watermark.register_in_flight(CommitVersion(1));
		watermark.register_in_flight(CommitVersion(2));
		watermark.register_in_flight(CommitVersion(3));

		watermark.mark_finished(CommitVersion(2));
		watermark.mark_finished(CommitVersion(3));

		assert_eq!(watermark.done_until().0, 0);

		watermark.mark_finished(CommitVersion(1));
		watermark.wait_for_mark(1);
		watermark.wait_for_mark(3);
		assert_eq!(watermark.done_until().0, 3);
	}

	#[test]
	fn test_done_until() {
		let watermark = WaterMark::new("watermark".into());
		watermark.shared.done_until.store(1, Ordering::SeqCst);
		assert_eq!(watermark.done_until().0, 1);
	}

	#[test]
	fn test_high_concurrency() {
		let watermark = Arc::new(WaterMark::new("concurrent".into()));

		const NUM_TASKS: usize = 50;
		const OPS_PER_TASK: usize = 100;

		let mut handles = vec![];

		for task_id in 0..NUM_TASKS {
			let wm = watermark.clone();
			let handle = thread::spawn(move || {
				for i in 0..OPS_PER_TASK {
					let version = CommitVersion((task_id * OPS_PER_TASK + i) as u64 + 1);
					wm.register_in_flight(version);
					wm.mark_finished(version);
				}
			});
			handles.push(handle);
		}

		for handle in handles {
			handle.join().unwrap();
		}

		let final_done = watermark.done_until();
		assert!(final_done.0 > 0, "Watermark should have progressed");
	}

	#[test]
	fn test_concurrent_wait_for_mark() {
		let watermark = Arc::new(WaterMark::new("wait_concurrent".into()));
		let success_count = Arc::new(AtomicUsize::new(0));

		for i in 1..=10 {
			watermark.register_in_flight(CommitVersion(i));
		}

		let mut handles = vec![];

		for version in 1..=10 {
			let wm = watermark.clone();
			let counter = success_count.clone();
			let handle = thread::spawn(move || {
				if wm.wait_for_mark_timeout(CommitVersion(version), Duration::from_seconds(5).unwrap())
				{
					counter.fetch_add(1, Ordering::Relaxed);
				}
			});
			handles.push(handle);
		}

		sleep(Duration::from_milliseconds(50).unwrap().to_std());

		for i in 1..=10 {
			watermark.mark_finished(CommitVersion(i));
		}

		for handle in handles {
			handle.join().unwrap();
		}

		assert_eq!(success_count.load(Ordering::Relaxed), 10);
	}

	#[test]
	fn test_old_version_rejection() {
		let watermark = WaterMark::new("watermark".into());

		for i in 1..=100 {
			watermark.register_in_flight(CommitVersion(i));
			watermark.mark_finished(CommitVersion(i));
		}

		let reached = watermark.wait_for_mark_timeout(CommitVersion(100), Duration::from_seconds(5).unwrap());
		assert!(reached, "Should have processed all 100 versions");
		let done_until = watermark.done_until();

		let very_old = done_until.0.saturating_sub(OLD_VERSION_THRESHOLD + 10);
		let clock = Clock::Real;
		let start = clock.instant();
		watermark.wait_for_mark(very_old);
		let elapsed = start.elapsed();

		assert!(elapsed.as_millis() < 10, "Old version wait should return immediately");
	}

	#[test]
	fn test_timeout_behavior() {
		let watermark = WaterMark::new("watermark".into());
		watermark.register_in_flight(CommitVersion(1));

		let clock = Clock::Real;
		let start = clock.instant();
		let result =
			watermark.wait_for_mark_timeout(CommitVersion(1), Duration::from_milliseconds(100).unwrap());
		let elapsed = start.elapsed();

		assert!(!result, "Should timeout waiting for uncompleted version");
		assert!(elapsed.as_millis() >= 100 && elapsed.as_millis() < 200, "Should respect timeout duration");
	}

	#[test]
	fn test_out_of_order_begin() {
		let watermark = WaterMark::new("watermark".into());

		watermark.register_in_flight(CommitVersion(3));
		watermark.register_in_flight(CommitVersion(1));
		watermark.register_in_flight(CommitVersion(2));

		watermark.mark_finished(CommitVersion(1));
		watermark.mark_finished(CommitVersion(2));
		watermark.mark_finished(CommitVersion(3));

		let reached = watermark.wait_for_mark_timeout(CommitVersion(3), Duration::from_seconds(5).unwrap());
		assert!(reached, "Timed out waiting for watermark to advance to 3");
		assert_eq!(watermark.done_until().0, 3, "Watermark should advance to 3");
	}

	#[test]
	fn test_orphaned_done_before_begin() {
		let watermark = WaterMark::new("watermark".into());

		// done() arrives before begin() - orphaned; watermark must NOT advance.
		watermark.mark_finished(CommitVersion(1));
		assert_eq!(watermark.done_until().0, 0);

		// begin() arrives; watermark advances synchronously.
		watermark.register_in_flight(CommitVersion(1));
		assert_eq!(watermark.done_until().0, 1, "Watermark should advance to 1 after begin");
	}

	#[test]
	fn test_mixed_out_of_order() {
		let watermark = WaterMark::new("watermark".into());

		watermark.register_in_flight(CommitVersion(2));
		watermark.mark_finished(CommitVersion(3)); // orphaned
		watermark.register_in_flight(CommitVersion(1));
		watermark.mark_finished(CommitVersion(1));
		watermark.register_in_flight(CommitVersion(3));
		watermark.mark_finished(CommitVersion(2));

		let reached = watermark.wait_for_mark_timeout(CommitVersion(3), Duration::from_seconds(5).unwrap());
		assert!(reached, "Timed out waiting for watermark to advance to 3");
		assert_eq!(watermark.done_until().0, 3, "Watermark should advance to 3");
	}

	#[test]
	fn test_notify_on_mark_event_driven() {
		let watermark = Arc::new(WaterMark::new("notify_on_mark".into()));
		let fired = Arc::new(AtomicUsize::new(0));

		watermark.register_in_flight(CommitVersion(1));

		let f = fired.clone();
		watermark.notify_on_mark(
			CommitVersion(1),
			Box::new(move || {
				f.fetch_add(1, Ordering::SeqCst);
			}),
		);

		assert_eq!(fired.load(Ordering::SeqCst), 0, "callback must not fire before the mark is reached");

		watermark.mark_finished(CommitVersion(1));
		assert_eq!(fired.load(Ordering::SeqCst), 1, "callback fires once when the mark advances");

		let f2 = fired.clone();
		watermark.notify_on_mark(
			CommitVersion(1),
			Box::new(move || {
				f2.fetch_add(1, Ordering::SeqCst);
			}),
		);
		assert_eq!(
			fired.load(Ordering::SeqCst),
			2,
			"callback fires immediately if the mark is already reached"
		);
	}

	#[test]
	fn test_notify_callback_reentrancy_no_deadlock() {
		// A notify_on_mark callback that re-enters the same watermark must not
		// deadlock: callbacks run after the state lock is released.
		let watermark = Arc::new(WaterMark::new("reentrant".into()));
		let reentered = Arc::new(AtomicUsize::new(0));

		watermark.register_in_flight(CommitVersion(1));

		let wm = watermark.clone();
		let flag = reentered.clone();
		watermark.notify_on_mark(
			CommitVersion(1),
			Box::new(move || {
				let _ = wm.done_until();
				wm.register_in_flight(CommitVersion(2));
				wm.mark_finished(CommitVersion(2));
				flag.fetch_add(1, Ordering::SeqCst);
			}),
		);

		// Advancing to 1 fires the callback synchronously; a deadlock on the
		// watermark lock would hang this call.
		watermark.mark_finished(CommitVersion(1));

		assert_eq!(reentered.load(Ordering::SeqCst), 1, "re-entrant callback ran to completion");
		assert_eq!(watermark.done_until().0, 2, "re-entrant version advanced the watermark");
	}
}
