// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	fmt,
	fmt::Debug,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	time::Duration,
};

use reifydb_core::{actors::watermark::WatermarkMessage, common::CommitVersion};
use reifydb_runtime::{
	actor::{mailbox::ActorRef, system::ActorSpawner},
	sync::waiter::WaiterHandle,
};
use tracing::instrument;

use super::actor::{WatermarkActor, WatermarkShared};

pub struct WaterMark {
	actor: ActorRef<WatermarkMessage>,
	shared: Arc<WatermarkShared>,
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
	#[instrument(name = "transaction::watermark::new", level = "debug", skip(spawner), fields(task_name = %task_name))]
	pub fn new(task_name: String, spawner: &ActorSpawner) -> Self {
		let shared = Arc::new(WatermarkShared {
			done_until: AtomicU64::new(0),
			last_index: AtomicU64::new(0),
		});

		let actor = WatermarkActor {
			shared: shared.clone(),
		};
		let actor_ref = spawner.spawn_system(&task_name, actor).actor_ref().clone();

		Self {
			actor: actor_ref,
			shared,
		}
	}

	#[instrument(name = "transaction::watermark::register_in_flight", level = "trace", skip(self), fields(version = version.0))]
	pub fn register_in_flight(&self, version: CommitVersion) {
		self.shared.last_index.fetch_max(version.0, Ordering::SeqCst);

		let _ = self.actor.send(WatermarkMessage::Begin {
			version: version.0,
		});
	}

	#[instrument(name = "transaction::watermark::mark_finished", level = "trace", skip(self), fields(index = version.0))]
	pub fn mark_finished(&self, version: CommitVersion) {
		let _ = self.actor.send(WatermarkMessage::Done {
			version: version.0,
		});
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
		self.wait_for_mark_timeout(CommitVersion(index), Duration::from_secs(30));
	}

	pub fn register_mark_waiter(&self, index: CommitVersion, waiter: Arc<WaiterHandle>) -> bool {
		let current_done = self.shared.done_until.load(Ordering::SeqCst);
		if current_done >= index.0 {
			waiter.notify();
			return true;
		}

		self.actor
			.send(WatermarkMessage::WaitFor {
				version: index.0,
				waiter,
			})
			.is_ok()
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
	use std::{sync::atomic::AtomicUsize, thread, thread::sleep, time::Duration};

	use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock, pool::Pools};

	use super::*;
	use crate::multi::watermark::OLD_VERSION_THRESHOLD;

	#[test]
	fn test_basic() {
		init_and_close(|_| {});
	}

	#[test]
	fn test_begin_done() {
		init_and_close(|watermark| {
			watermark.register_in_flight(CommitVersion(1));
			watermark.register_in_flight(CommitVersion(2));
			watermark.register_in_flight(CommitVersion(3));

			watermark.mark_finished(CommitVersion(1));
			watermark.mark_finished(CommitVersion(2));
			watermark.mark_finished(CommitVersion(3));
		});
	}

	#[test]
	fn test_wait_for_mark() {
		init_and_close(|watermark| {
			watermark.register_in_flight(CommitVersion(1));
			watermark.register_in_flight(CommitVersion(2));
			watermark.register_in_flight(CommitVersion(3));

			watermark.mark_finished(CommitVersion(2));
			watermark.mark_finished(CommitVersion(3));

			assert_eq!(watermark.done_until(), 0);

			watermark.mark_finished(CommitVersion(1));
			watermark.wait_for_mark(1);
			watermark.wait_for_mark(3);
			assert_eq!(watermark.done_until(), 3);
		});
	}

	#[test]
	fn test_done_until() {
		init_and_close(|watermark| {
			watermark.shared.done_until.store(1, Ordering::SeqCst);
			assert_eq!(watermark.done_until(), 1);
		});
	}

	#[test]
	fn test_high_concurrency() {
		let system = ActorSystem::new(Pools::default(), Clock::Real);
		let watermark = Arc::new(WaterMark::new("concurrent".into(), &system.spawner()));

		const NUM_TASKS: usize = 50;
		const OPS_PER_TASK: usize = 100;

		let mut handles = vec![];

		// Spawn tasks that perform concurrent begin/done operations
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

		sleep(Duration::from_millis(100));

		// Verify the watermark progressed
		let final_done = watermark.done_until();
		assert!(final_done.0 > 0, "Watermark should have progressed");

		system.shutdown();
		sleep(Duration::from_millis(150)); // Wait for actor to stop
	}

	#[test]
	fn test_concurrent_wait_for_mark() {
		let system = ActorSystem::new(Pools::default(), Clock::Real);
		let watermark = Arc::new(WaterMark::new("wait_concurrent".into(), &system.spawner()));
		let success_count = Arc::new(AtomicUsize::new(0));

		// Start some versions
		for i in 1..=10 {
			watermark.register_in_flight(CommitVersion(i));
		}

		let mut handles = vec![];

		// Spawn tasks that wait for marks
		for version in 1..=10 {
			let wm = watermark.clone();
			let counter = success_count.clone();
			let handle = thread::spawn(move || {
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
			watermark.mark_finished(CommitVersion(i));
		}

		for handle in handles {
			handle.join().unwrap();
		}

		// All waits should have succeeded
		assert_eq!(success_count.load(Ordering::Relaxed), 10);

		system.shutdown();
		sleep(Duration::from_millis(150)); // Wait for actor to stop
	}

	#[test]
	fn test_old_version_rejection() {
		init_and_close(|watermark| {
			// Advance done_until significantly
			for i in 1..=100 {
				watermark.register_in_flight(CommitVersion(i));
				watermark.mark_finished(CommitVersion(i));
			}

			let reached = watermark.wait_for_mark_timeout(CommitVersion(100), Duration::from_secs(5));
			assert!(reached, "Should have processed all 100 versions");
			let done_until = watermark.done_until();

			// Try to wait for a very old version (should return immediately)
			let very_old = done_until.0.saturating_sub(OLD_VERSION_THRESHOLD + 10);
			let clock = Clock::Real;
			let start = clock.instant();
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
			watermark.register_in_flight(CommitVersion(1));

			// Wait with short timeout
			let clock = Clock::Real;
			let start = clock.instant();
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
			watermark.register_in_flight(CommitVersion(3));
			watermark.register_in_flight(CommitVersion(1));
			watermark.register_in_flight(CommitVersion(2));

			// Complete in order
			watermark.mark_finished(CommitVersion(1));
			watermark.mark_finished(CommitVersion(2));
			watermark.mark_finished(CommitVersion(3));

			let reached = watermark.wait_for_mark_timeout(CommitVersion(3), Duration::from_secs(5));
			assert!(reached, "Timed out waiting for watermark to advance to 3");
			let done = watermark.done_until();
			assert_eq!(done.0, 3, "Watermark should advance to 3, got {}", done.0);
		});
	}

	#[test]
	fn test_orphaned_done_before_begin() {
		// Test that done() arriving before begin() is handled correctly
		init_and_close(|watermark| {
			// done() arrives before begin() - this is an "orphaned" done
			watermark.mark_finished(CommitVersion(1));

			// Wait a bit for processing
			sleep(Duration::from_millis(20));

			// Watermark should NOT advance yet (begin hasn't arrived)
			assert_eq!(watermark.done_until().0, 0);

			// Now begin() arrives
			watermark.register_in_flight(CommitVersion(1));

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
			watermark.register_in_flight(CommitVersion(2));
			watermark.mark_finished(CommitVersion(3)); // orphaned
			watermark.register_in_flight(CommitVersion(1));
			watermark.mark_finished(CommitVersion(1));
			watermark.register_in_flight(CommitVersion(3));
			watermark.mark_finished(CommitVersion(2));

			let reached = watermark.wait_for_mark_timeout(CommitVersion(3), Duration::from_secs(5));
			assert!(reached, "Timed out waiting for watermark to advance to 3");
			let done = watermark.done_until();
			assert_eq!(done.0, 3, "Watermark should advance to 3, got {}", done.0);
		});
	}

	#[test]
	fn test_notify_on_mark_event_driven() {
		let system = ActorSystem::new(Pools::default(), Clock::Real);
		let watermark = Arc::new(WaterMark::new("notify_on_mark".into(), &system.spawner()));
		let fired = Arc::new(AtomicUsize::new(0));

		watermark.register_in_flight(CommitVersion(1));

		let f = fired.clone();
		watermark.notify_on_mark(
			CommitVersion(1),
			Box::new(move || {
				f.fetch_add(1, Ordering::SeqCst);
			}),
		);

		sleep(Duration::from_millis(20));
		assert_eq!(fired.load(Ordering::SeqCst), 0, "callback must not fire before the mark is reached");

		watermark.mark_finished(CommitVersion(1));
		sleep(Duration::from_millis(50));
		assert_eq!(fired.load(Ordering::SeqCst), 1, "callback fires once when the mark advances");

		let f2 = fired.clone();
		watermark.notify_on_mark(
			CommitVersion(1),
			Box::new(move || {
				f2.fetch_add(1, Ordering::SeqCst);
			}),
		);
		sleep(Duration::from_millis(20));
		assert_eq!(
			fired.load(Ordering::SeqCst),
			2,
			"callback fires immediately if the mark is already reached"
		);

		system.shutdown();
		sleep(Duration::from_millis(150));
	}

	fn init_and_close<F>(f: F)
	where
		F: FnOnce(Arc<WaterMark>),
	{
		let system = ActorSystem::new(Pools::default(), Clock::Real);
		let watermark = Arc::new(WaterMark::new("watermark".into(), &system.spawner()));

		f(watermark);

		sleep(Duration::from_millis(10));
		system.shutdown();
		sleep(Duration::from_millis(150)); // Wait for actor to stop
	}
}
