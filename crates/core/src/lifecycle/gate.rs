// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_runtime::context::clock::Clock;
use reifydb_value::value::duration::Duration;

use crate::lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask};

#[derive(Clone)]
pub struct RetentionStartupGate {
	inner: Arc<Inner>,
}

struct Inner {
	clock: Clock,
	armed_at_nanos: u64,
	grace: Duration,
	skipped_slices: AtomicU64,
}

impl RetentionStartupGate {
	pub fn arm(clock: Clock, grace: Duration) -> Self {
		let armed_at_nanos = clock.now().to_nanos();
		Self {
			inner: Arc::new(Inner {
				clock,
				armed_at_nanos,
				grace,
				skipped_slices: AtomicU64::new(0),
			}),
		}
	}

	pub fn open(clock: Clock) -> Self {
		Self::arm(clock, Duration::zero())
	}

	pub fn is_open(&self) -> bool {
		if self.inner.grace.is_zero() {
			return true;
		}
		let now = self.inner.clock.now();
		match now.checked_sub(self.inner.grace) {
			Some(released) => released.to_nanos() >= self.inner.armed_at_nanos,
			None => false,
		}
	}

	pub fn record_skip(&self) {
		self.inner.skipped_slices.fetch_add(1, Ordering::Relaxed);
	}

	pub fn skipped_slices(&self) -> u64 {
		self.inner.skipped_slices.load(Ordering::Relaxed)
	}

	pub fn grace(&self) -> Duration {
		self.inner.grace
	}
}

pub struct Gated<T: LifecycleTask> {
	inner: T,
	gate: RetentionStartupGate,
}

impl<T: LifecycleTask> Gated<T> {
	pub fn new(inner: T, gate: RetentionStartupGate) -> Self {
		Self {
			inner,
			gate,
		}
	}
}

impl<T: LifecycleTask> LifecycleTask for Gated<T> {
	fn name(&self) -> &'static str {
		self.inner.name()
	}

	fn interval(&self) -> Duration {
		self.inner.interval()
	}

	fn classes(&self) -> &'static [RetentionClass] {
		self.inner.classes()
	}

	fn run_slice(&mut self) -> Progress {
		if !self.gate.is_open() {
			self.gate.record_skip();
			return Progress::Exhausted;
		}
		self.inner.run_slice()
	}
}

#[cfg(test)]
mod tests {
	use std::sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	};

	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_value::value::duration::Duration;

	use super::{Gated, RetentionStartupGate};
	use crate::lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask};

	fn mock() -> (Clock, MockClock) {
		let mock = MockClock::from_millis(0);
		(Clock::Mock(mock.clone()), mock)
	}

	#[test]
	fn a_gate_armed_with_a_grace_period_starts_closed() {
		// The whole point of the gate: the first tick after a restart must not delete. A gate that starts
		// open would let a process that was down longer than its TTLs mass-evict on tick one.
		let (clock, _mock) = mock();
		let gate = RetentionStartupGate::arm(clock, Duration::from_seconds(300).unwrap());

		assert!(!gate.is_open(), "a freshly armed gate must hold reclamation back for its grace period");
	}

	#[test]
	fn a_zero_grace_gate_is_open_immediately() {
		// Tests and single-shot tools need reclamation without waiting out a grace period; zero grace is the
		// documented way to ask for that, so it must not accidentally still gate.
		let (clock, _mock) = mock();
		let gate = RetentionStartupGate::open(clock);

		assert!(gate.is_open(), "zero grace must mean no gating at all");
	}

	#[test]
	fn the_gate_opens_once_the_grace_period_has_elapsed() {
		let (clock, mock) = mock();
		let gate = RetentionStartupGate::arm(clock, Duration::from_seconds(60).unwrap());
		assert!(!gate.is_open(), "precondition: still inside the grace window");

		mock.advance_secs(59);
		assert!(!gate.is_open(), "one second short of the grace period must still gate");

		mock.advance_secs(1);
		assert!(gate.is_open(), "once the grace period elapses the gate must release reclamation");
	}

	#[test]
	fn the_gate_stays_open_once_released() {
		// The gate is a startup guard, not a rate limiter; re-closing it later would stall reclamation
		// permanently on any clock that is not strictly monotonic across reads.
		let (clock, mock) = mock();
		let gate = RetentionStartupGate::arm(clock, Duration::from_seconds(60).unwrap());
		mock.advance_secs(60);
		assert!(gate.is_open(), "precondition: released");

		mock.advance_secs(3600);

		assert!(gate.is_open(), "a released gate must remain open for the life of the process");
	}

	struct CountingTask {
		slices: Arc<AtomicU64>,
	}

	impl LifecycleTask for CountingTask {
		fn name(&self) -> &'static str {
			"counting"
		}

		fn interval(&self) -> Duration {
			Duration::from_seconds(1).unwrap()
		}

		fn classes(&self) -> &'static [RetentionClass] {
			&[RetentionClass::RowTtl]
		}

		fn run_slice(&mut self) -> Progress {
			self.slices.fetch_add(1, Ordering::SeqCst);
			Progress::Exhausted
		}
	}

	#[test]
	fn a_gated_task_does_no_work_while_the_gate_is_closed() {
		// A durable epoch un-blinds every TTL consumer at boot, so the first slice after a long downtime
		// would try to reclaim the whole backlog at once. The gate must stop the work itself, not
		// merely record that it happened.
		let (clock, _mock) = mock();
		let gate = RetentionStartupGate::arm(clock, Duration::from_seconds(300).unwrap());
		let slices = Arc::new(AtomicU64::new(0));
		let mut task = Gated::new(
			CountingTask {
				slices: slices.clone(),
			},
			gate.clone(),
		);

		assert_eq!(task.run_slice(), Progress::Exhausted, "a gated slice must not ask the lane for a catch-up");

		assert_eq!(slices.load(Ordering::SeqCst), 0, "the wrapped task must not run at all while gated");
		assert_eq!(gate.skipped_slices(), 1, "the skip must be counted so a gated class is not read as idle");
	}

	#[test]
	fn a_gated_task_runs_normally_once_the_gate_opens() {
		// The other half: a gate that never releases is indistinguishable from reclamation being disabled,
		// which is the failure this whole subsystem exists to make impossible.
		let (clock, mock) = mock();
		let gate = RetentionStartupGate::arm(clock, Duration::from_seconds(60).unwrap());
		let slices = Arc::new(AtomicU64::new(0));
		let mut task = Gated::new(
			CountingTask {
				slices: slices.clone(),
			},
			gate,
		);
		task.run_slice();
		assert_eq!(slices.load(Ordering::SeqCst), 0, "precondition: gated");

		mock.advance_secs(60);
		task.run_slice();

		assert_eq!(slices.load(Ordering::SeqCst), 1, "once released the wrapped task must run");
	}

	#[test]
	fn gating_preserves_the_wrapped_class_identity() {
		// The name and cadence are how the lane schedules a class and how the report and metrics key it. A
		// wrapper that renamed or re-timed its inner task would make the gated class untraceable.
		let (clock, _mock) = mock();
		let gate = RetentionStartupGate::arm(clock, Duration::from_seconds(300).unwrap());
		let task = Gated::new(
			CountingTask {
				slices: Arc::new(AtomicU64::new(0)),
			},
			gate,
		);

		assert_eq!(task.name(), "counting", "gating must not rename the class");
		assert_eq!(task.interval(), Duration::from_seconds(1).unwrap(), "gating must not change the cadence");
	}

	#[test]
	fn the_gate_counts_the_slices_it_turned_away() {
		// A gated executor looks identical to a broken one from the outside - both report zero work. The skip
		// counter is what distinguishes "deliberately held back" from "silently not running", which is the
		// exact ambiguity this subsystem exists to remove.
		let (clock, _mock) = mock();
		let gate = RetentionStartupGate::arm(clock, Duration::from_seconds(300).unwrap());

		for _ in 0..3 {
			if !gate.is_open() {
				gate.record_skip();
			}
		}

		assert_eq!(gate.skipped_slices(), 3, "every skipped slice must be counted, not silently dropped");
	}
}
