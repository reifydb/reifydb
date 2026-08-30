// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt, fmt::Debug, result::Result};

use reifydb_core::common::CommitVersion;
use reifydb_runtime::{
	actor::{
		mailbox::{ActorRef, SendError},
		system::ActorSpawner,
	},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
		mutex::Mutex,
		waiter::WaiterHandle,
	},
};
use reifydb_value::{reifydb_assertions, value::duration::Duration};
use tracing::instrument;

#[cfg(not(reifydb_single_threaded))]
use super::advancer::WatermarkAdvancer;
use super::{
	MAX_INLINE_ADVANCE,
	advancer::AdvanceKick,
	state::{AdvanceBudget, AdvanceOutcome, WatermarkShared, WatermarkState},
};

enum AdvancerHandle {
	Inline,
	#[cfg_attr(reifydb_single_threaded, allow(dead_code))]
	Actor(ActorRef<AdvanceKick>),
}

pub struct WaterMark {
	state: Arc<Mutex<WatermarkState>>,
	shared: Arc<WatermarkShared>,
	advancer: AdvancerHandle,
}

impl Debug for WaterMark {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("WaterMark")
			.field("done_until", &self.shared.done_until.load(Ordering::Relaxed))
			.finish()
	}
}

impl WaterMark {
	#[instrument(name = "transaction::watermark::new", level = "debug", fields(task_name = %task_name))]
	pub fn new(task_name: String) -> Self {
		Self {
			state: Arc::new(Mutex::new(WatermarkState::new())),
			shared: Arc::new(WatermarkShared {
				done_until: AtomicU64::new(0),
			}),
			advancer: AdvancerHandle::Inline,
		}
	}

	#[cfg(not(reifydb_single_threaded))]
	pub fn with_advancer(task_name: String, spawner: &ActorSpawner) -> Self {
		let advancer_name = format!("{task_name}-advancer");
		let watermark = Self::new(task_name);
		let actor = WatermarkAdvancer::new(watermark.state.clone(), watermark.shared.clone());
		let actor_ref = spawner.spawn_coordination(&advancer_name, actor).actor_ref().clone();
		Self {
			advancer: AdvancerHandle::Actor(actor_ref),
			..watermark
		}
	}

	#[cfg(reifydb_single_threaded)]
	pub fn with_advancer(task_name: String, _spawner: &ActorSpawner) -> Self {
		Self::new(task_name)
	}

	fn budget(&self) -> AdvanceBudget {
		match &self.advancer {
			AdvancerHandle::Inline => AdvanceBudget::Unlimited,
			AdvancerHandle::Actor(_) => AdvanceBudget::Capped(MAX_INLINE_ADVANCE),
		}
	}

	fn kick(&self) {
		let AdvancerHandle::Actor(actor_ref) = &self.advancer else {
			return;
		};
		match actor_ref.send(AdvanceKick) {
			Ok(()) | Err(SendError::Full(AdvanceKick)) => {}
			Err(SendError::Closed(AdvanceKick)) => self.drain(),
		}
	}

	fn complete_operation(&self, outcome: AdvanceOutcome, to_notify: Vec<Arc<WaiterHandle>>) {
		if outcome == AdvanceOutcome::MoreWork {
			self.kick();
		}
		for waiter in to_notify {
			waiter.notify();
		}
	}

	pub(crate) fn drain(&self) {
		let mut to_notify = Vec::new();
		{
			let mut state = self.state.lock();
			state.drain_remaining(&self.shared.done_until, &mut to_notify);
		}
		for waiter in to_notify {
			waiter.notify();
		}
	}

	#[instrument(name = "transaction::watermark::register_in_flight", level = "trace", skip(self), fields(version = version.0))]
	pub fn register_in_flight(&self, version: CommitVersion) {
		let mut to_notify = Vec::new();
		let outcome = {
			let mut state = self.state.lock();
			state.process_begin(version.0, &self.shared.done_until, &mut to_notify, self.budget())
		};
		self.complete_operation(outcome, to_notify);
	}

	pub fn register_in_flight_with<E>(
		&self,
		version_fn: impl FnOnce() -> Result<CommitVersion, E>,
	) -> Result<CommitVersion, E> {
		let mut to_notify = Vec::new();
		let (version, outcome) = {
			let mut state = self.state.lock();
			let version = version_fn()?;
			let outcome =
				state.process_begin(version.0, &self.shared.done_until, &mut to_notify, self.budget());
			(version, outcome)
		};
		self.complete_operation(outcome, to_notify);
		Ok(version)
	}

	#[instrument(name = "transaction::watermark::mark_finished", level = "trace", skip(self), fields(index = version.0))]
	pub fn mark_finished(&self, version: CommitVersion) {
		let mut to_notify = Vec::new();
		let outcome = {
			let mut state = self.state.lock();
			state.process_done(version.0, &self.shared.done_until, &mut to_notify, self.budget())
		};
		self.complete_operation(outcome, to_notify);
	}

	pub fn done_until(&self) -> CommitVersion {
		CommitVersion(self.shared.done_until.load(Ordering::SeqCst))
	}

	pub fn advance_to(&self, version: CommitVersion) {
		reifydb_assertions! {
			if let Some(live) = self.state.lock().min_live_in_flight() {
				assert!(
					version.0 < live,
					"advancing the frontier to {} by fiat would leap over the live in-flight \
					 version {}; consumers would treat that commit as applied before its writes \
					 exist, tearing snapshots and permanently skipping its CDC events",
					version.0, live
				);
			}
		}
		self.shared.done_until.fetch_max(version.0, Ordering::SeqCst);
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
		if self.shared.done_until.load(Ordering::SeqCst) >= index.0 {
			return true;
		}
		let waiter = Arc::new(WaiterHandle::new());
		if !self.register_mark_waiter(index, waiter.clone()) {
			return false;
		}
		waiter.wait_timeout(timeout)
	}

	pub fn notify_on_mark(&self, index: CommitVersion, callback: Box<dyn FnOnce() + Send>) {
		if self.shared.done_until.load(Ordering::SeqCst) >= index.0 {
			callback();
			return;
		}
		let waiter = Arc::new(WaiterHandle::with_callback(callback));
		let _ = self.register_mark_waiter(index, waiter);
	}
}

#[cfg(test)]
pub mod tests {
	#[cfg(not(reifydb_single_threaded))]
	use std::sync::atomic::AtomicBool;
	use std::{sync::atomic::AtomicUsize, thread, thread::sleep};

	#[cfg(reifydb_single_threaded)]
	use reifydb_runtime::context::clock::MockClock;
	use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};

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
		watermark.wait_for_mark_timeout(CommitVersion(very_old), Duration::from_seconds(30).unwrap());
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
	fn fast_path_wait_returns_true_with_zero_timeout_when_mark_reached() {
		// An already-reached mark must be answered from the done_until fast path without
		// registering (or blocking on) a waiter; with a zero timeout, any accidental trip
		// through the blocking path would return false and misreport an applied commit as
		// unapplied.
		let watermark = WaterMark::new("fast_path".into());
		watermark.register_in_flight(CommitVersion(1));
		watermark.mark_finished(CommitVersion(1));

		let reached =
			watermark.wait_for_mark_timeout(CommitVersion(1), Duration::from_milliseconds(0).unwrap());
		assert!(reached, "a reached mark must be reported reached even with a zero timeout");
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

	#[cfg(not(reifydb_single_threaded))]
	fn watermark_with_live_advancer(name: &str) -> (ActorSystem, WaterMark) {
		// The returned ActorSystem must stay alive for the test's duration; dropping it
		// kills the advancer and silently degrades every kick into the inline fallback,
		// which would make the actor-path assertions vacuously pass.
		let system = ActorSystem::testing(Clock::Real);
		let spawner = system.spawner();
		let watermark = WaterMark::with_advancer(name.into(), &spawner);
		(system, watermark)
	}

	#[cfg(not(reifydb_single_threaded))]
	#[test]
	fn with_advancer_reaches_frontier_after_coalesced_kicks() {
		// Out-of-order bursts far beyond MAX_INLINE_ADVANCE force deferrals whose kicks
		// coalesce in the capacity-1 mailbox. If Full-as-coalesced could drop the LAST
		// kick, the frontier would stop short of the top and this wait would time out.
		let (_system, watermark) = watermark_with_live_advancer("coalesced");
		let watermark = Arc::new(watermark);
		let threads = 8u64;
		let per_thread = 2_000u64;
		let total = threads * per_thread;

		let mut handles = vec![];
		for thread_id in 0..threads {
			let wm = watermark.clone();
			handles.push(thread::spawn(move || {
				let first = thread_id * per_thread + 1;
				let last = (thread_id + 1) * per_thread;
				for version in first..=last {
					wm.register_in_flight(CommitVersion(version));
					wm.mark_finished(CommitVersion(version));
				}
			}));
		}
		for handle in handles {
			handle.join().unwrap();
		}

		assert!(
			watermark.wait_for_mark_timeout(CommitVersion(total), Duration::from_seconds(10).unwrap()),
			"advancer never reached the frontier: done_until={:?} expected {total}",
			watermark.done_until()
		);
	}

	#[cfg(not(reifydb_single_threaded))]
	#[test]
	fn advancer_notifies_waiter_beyond_inline_budget() {
		// The finisher only advances MAX_INLINE_ADVANCE versions inline, so the waiter at
		// the far end of a 300-version backlog can only ever be released by the advancer.
		// A lost kick or a lost wakeup across drain chunks leaves it stuck until timeout.
		let (_system, watermark) = watermark_with_live_advancer("beyond-budget");

		for version in 1..=300 {
			watermark.register_in_flight(CommitVersion(version));
		}
		for version in 2..=300 {
			watermark.mark_finished(CommitVersion(version));
		}
		assert_eq!(watermark.done_until().0, 0, "the frontier is blocked on version 1");

		watermark.mark_finished(CommitVersion(1));

		assert!(
			watermark.wait_for_mark_timeout(CommitVersion(300), Duration::from_seconds(5).unwrap()),
			"the advancer must finish what the bounded inline advance left behind"
		);
	}

	#[cfg(not(reifydb_single_threaded))]
	#[test]
	fn waiter_registered_mid_drain_is_notified() {
		// Waiters registering concurrently with an in-progress advancer drain must be
		// released no matter which side of a chunk boundary they land on; registration
		// and collection are serialized by the same state lock.
		let (_system, watermark) = watermark_with_live_advancer("mid-drain");
		let watermark = Arc::new(watermark);

		for version in 1..=1_000 {
			watermark.register_in_flight(CommitVersion(version));
		}
		for version in 2..=1_000 {
			watermark.mark_finished(CommitVersion(version));
		}

		let mut waiters = vec![];
		for target in [65u64, 300, 700, 999, 1_000] {
			let wm = watermark.clone();
			waiters.push(thread::spawn(move || {
				wm.wait_for_mark_timeout(CommitVersion(target), Duration::from_seconds(5).unwrap())
			}));
		}

		watermark.mark_finished(CommitVersion(1));

		for waiter in waiters {
			assert!(waiter.join().unwrap(), "a waiter racing the drain must still be released");
		}
	}

	#[cfg(not(reifydb_single_threaded))]
	#[test]
	fn callback_reentering_watermark_via_advancer_completes() {
		// notify_on_mark callbacks fire on the advancer thread once the mark is past the
		// inline budget; a callback that re-enters the watermark must not deadlock against
		// the advancer's own state lock (it is released before notification).
		let (_system, watermark) = watermark_with_live_advancer("reentrant-advancer");
		let watermark = Arc::new(watermark);

		for version in 1..=300 {
			watermark.register_in_flight(CommitVersion(version));
		}
		for version in 2..=300 {
			watermark.mark_finished(CommitVersion(version));
		}

		let wm = watermark.clone();
		let reentered = Arc::new(AtomicUsize::new(0));
		let flag = reentered.clone();
		watermark.notify_on_mark(
			CommitVersion(300),
			Box::new(move || {
				let _ = wm.done_until();
				wm.register_in_flight(CommitVersion(301));
				flag.fetch_add(1, Ordering::SeqCst);
				wm.mark_finished(CommitVersion(301));
			}),
		);

		watermark.mark_finished(CommitVersion(1));

		assert!(
			watermark.wait_for_mark_timeout(CommitVersion(301), Duration::from_seconds(5).unwrap()),
			"the re-entrant callback must complete and its version must advance the frontier"
		);
		assert_eq!(reentered.load(Ordering::SeqCst), 1, "callback ran exactly once");
	}

	#[cfg(reifydb_assertions)]
	#[test]
	#[should_panic(expected = "leap over the live in-flight")]
	fn fiat_advance_over_a_live_in_flight_version_panics_under_assertions() {
		// advance_to is a bare fetch_max on done_until that bypasses the ring entirely
		// (bootstrap uses it); if it ever leaps over a version that is
		// registered but not yet finished, consumers treat that commit as applied before
		// its writes exist - torn snapshots and permanently skipped CDC events. The
		// tripwire must turn that silent downstream corruption into a loud local panic.
		let watermark = WaterMark::new("advance-tripwire".into());
		watermark.register_in_flight(CommitVersion(5));
		watermark.advance_to(CommitVersion(10));
	}

	#[test]
	fn new_without_advancer_advances_fully_inline() {
		// WaterMark::new is the no-advancer configuration (unit fixtures, single-threaded
		// builds); a burst far beyond MAX_INLINE_ADVANCE must be fully drained by the
		// finishing call itself, synchronously, because there is nobody else to do it.
		let watermark = WaterMark::new("inline-only".into());

		for version in 1..=10_000 {
			watermark.register_in_flight(CommitVersion(version));
		}
		for version in (2..=10_000).rev() {
			watermark.mark_finished(CommitVersion(version));
		}
		watermark.mark_finished(CommitVersion(1));

		assert_eq!(
			watermark.done_until().0,
			10_000,
			"the inline path must reach the frontier synchronously, before mark_finished returns"
		);
	}

	#[cfg(not(reifydb_single_threaded))]
	#[test]
	fn kick_after_system_shutdown_falls_back_inline() {
		// After the actor system dies, kicks return Closed and mark_finished must degrade
		// to the full inline drain; anything less leaves done_until frozen below versions
		// that are fully finished, which freezes the GC cutoff for the process lifetime.
		let (system, watermark) = watermark_with_live_advancer("post-shutdown");
		system.shutdown();
		let _ = system.join();

		for version in 1..=200 {
			watermark.register_in_flight(CommitVersion(version));
		}
		for version in 2..=200 {
			watermark.mark_finished(CommitVersion(version));
		}
		watermark.mark_finished(CommitVersion(1));

		assert_eq!(
			watermark.done_until().0,
			200,
			"a Closed kick must trigger the synchronous inline drain, not silently stall"
		);
	}

	#[cfg(not(reifydb_single_threaded))]
	#[test]
	fn drain_restores_frontier_regardless_of_advancer_progress() {
		// Oracle::stop calls drain() after the runtime is gone; whatever the advancer did
		// or did not get to, one drain must restore the frontier and release every waiter
		// left in the map, or shutdown strands threads until their timeouts.
		let (_system, watermark) = watermark_with_live_advancer("stop-drain");

		for version in 1..=1_000 {
			watermark.register_in_flight(CommitVersion(version));
		}
		for version in 2..=1_000 {
			watermark.mark_finished(CommitVersion(version));
		}

		let waiter = Arc::new(WaiterHandle::new());
		watermark.register_mark_waiter(CommitVersion(1_000), waiter.clone());

		watermark.mark_finished(CommitVersion(1));
		watermark.drain();

		assert_eq!(watermark.done_until().0, 1_000, "drain must reach the true frontier before returning");
		assert!(
			waiter.wait_timeout(Duration::from_seconds(5).unwrap()),
			"a waiter pending at drain time must be released"
		);
	}

	#[cfg(not(reifydb_single_threaded))]
	#[test]
	fn done_until_monotonic_under_concurrent_inline_and_advancer() {
		// The frontier is advanced from two places at once (bounded inline pops and the
		// advancer's chunks); a sampler must never observe it move backwards, because
		// consumers treat done_until as a GC cutoff that only grows.
		let (_system, watermark) = watermark_with_live_advancer("monotonic");
		let watermark = Arc::new(watermark);
		let counter = Arc::new(AtomicU64::new(0));
		let stop = Arc::new(AtomicBool::new(false));
		let total = 200_000u64;

		let sampler = {
			let wm = watermark.clone();
			let stop = stop.clone();
			thread::spawn(move || {
				let mut previous = 0u64;
				while !stop.load(Ordering::Relaxed) {
					let current = wm.done_until().0;
					assert!(
						current >= previous,
						"done_until regressed from {previous} to {current}"
					);
					previous = current;
				}
			})
		};

		let mut handles = vec![];
		for _ in 0..4 {
			let wm = watermark.clone();
			let counter = counter.clone();
			handles.push(thread::spawn(move || {
				loop {
					let version = counter.fetch_add(1, Ordering::Relaxed) + 1;
					if version > total {
						break;
					}
					wm.register_in_flight(CommitVersion(version));
					wm.mark_finished(CommitVersion(version));
				}
			}));
		}
		for handle in handles {
			handle.join().unwrap();
		}
		assert!(
			watermark.wait_for_mark_timeout(CommitVersion(total), Duration::from_seconds(10).unwrap()),
			"the frontier must reach the last version"
		);
		stop.store(true, Ordering::Relaxed);
		sampler.join().unwrap();
	}

	#[cfg(reifydb_single_threaded)]
	#[test]
	fn with_advancer_is_inline_under_single_threaded() {
		// Single-threaded builds (wasm, DST) must never spawn the advancer: actor sends
		// run handlers inline on the caller's stack there, and the RefCell-backed mutex
		// would panic on the re-entrant state lock.
		let system = ActorSystem::testing(Clock::Mock(MockClock::from_millis(0)));
		let watermark = WaterMark::with_advancer("single-threaded".into(), &system.spawner());
		assert!(
			matches!(watermark.advancer, AdvancerHandle::Inline),
			"with_advancer must degrade to the inline configuration under reifydb_single_threaded"
		);
	}
}
