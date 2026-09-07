// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Scheduling contract of the lifecycle lane.
//!
//! Every reclamation class owns its own actor: one tick performs exactly one bounded slice, and `RunToExhaustion`
//! always notifies its waiter. The catch-up reschedule is a timer the harness cannot observe, so it is pinned
//! indirectly through `Continue` plus a single `run_slice`.

use std::sync::Arc;

use reifydb_core::lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask};
use reifydb_runtime::{
	actor::{testing::TestHarness, traits::Directive},
	sync::{mutex::Mutex, waiter::WaiterHandle},
};
use reifydb_sub_lifecycle::actor::{LifecycleActor, LifecycleMessage};
use reifydb_value::value::duration::Duration;

/// Records every slice in order, so a test can assert WHICH class ran, not merely how many times.
type Journal = Arc<Mutex<Vec<&'static str>>>;

struct ScriptedTask {
	name: &'static str,
	/// Slices still holding work; each run decrements. While non-zero the task reports Yielded.
	remaining: usize,
	journal: Journal,
}

impl ScriptedTask {
	fn new(name: &'static str, remaining: usize, journal: Journal) -> Self {
		Self {
			name,
			remaining,
			journal,
		}
	}
}

impl LifecycleTask for ScriptedTask {
	fn name(&self) -> &'static str {
		self.name
	}

	fn interval(&self) -> Duration {
		Duration::from_seconds(60).unwrap()
	}

	// These tests pin the lane's scheduling contract, not reclamation.
	fn classes(&self) -> &'static [RetentionClass] {
		&[]
	}

	fn run_slice(&mut self) -> Progress {
		self.journal.lock().push(self.name);
		self.remaining = self.remaining.saturating_sub(1);
		if self.remaining == 0 {
			Progress::Exhausted
		} else {
			Progress::Yielded
		}
	}
}

fn journal() -> Journal {
	Arc::new(Mutex::new(Vec::new()))
}

#[test]
fn a_tick_runs_exactly_one_slice_of_exactly_its_own_class_and_never_a_neighbour() {
	// Each class owns an actor, so a tick delivered to one must not drive another. Sharing a lane is what let a
	// slow class delay every other one, and running a neighbour would double-drive its cursor.
	let journal = journal();
	let mut first = TestHarness::new(LifecycleActor::new(Box::new(ScriptedTask::new(
		"first",
		1,
		journal.clone(),
	))));
	let mut second = TestHarness::new(LifecycleActor::new(Box::new(ScriptedTask::new(
		"second",
		1,
		journal.clone(),
	))));

	second.send(LifecycleMessage::Tick);
	let directives = second.process_all();
	let _ = &mut first;

	assert_eq!(
		*journal.lock(),
		vec!["second"],
		"a tick must run only the class whose actor received it - driving a neighbour double-drives its cursor"
	);
	assert_eq!(directives, vec![Directive::Continue], "a completed slice must keep the lane alive");
}

#[test]
fn a_tick_yields_the_lane_after_one_slice_even_when_the_class_still_has_work() {
	// The budget contract. If this actor ever drains inline on Yielded, a class with a large backlog occupies
	// the lane for the whole drain and every other class - including persistent flush - waits behind it.
	let journal = journal();
	let mut harness = TestHarness::new(LifecycleActor::new(Box::new(ScriptedTask::new(
		"backlogged",
		5,
		journal.clone(),
	))));

	harness.send(LifecycleMessage::Tick);
	let directives = harness.process_all();

	assert_eq!(
		journal.lock().len(),
		1,
		"one tick must perform exactly one slice; draining {} slices inline would monopolise the lane",
		journal.lock().len()
	);
	assert_eq!(directives, vec![Directive::Continue], "yielding with work left must not stop the lane");
}

#[test]
fn run_to_exhaustion_drains_the_backlog_and_notifies_the_waiter() {
	let journal = journal();
	let mut harness = TestHarness::new(LifecycleActor::new(Box::new(ScriptedTask::new(
		"backlogged",
		4,
		journal.clone(),
	))));
	let waiter = Arc::new(WaiterHandle::new());

	harness.send(LifecycleMessage::RunToExhaustion {
		waiter: waiter.clone(),
	});
	harness.process_all();

	assert_eq!(
		journal.lock().len(),
		4,
		"RunToExhaustion must keep slicing until the class reports Exhausted, not stop at the first budget"
	);
	assert!(
		waiter.wait_timeout(Duration::from_milliseconds(1).unwrap()),
		"the waiter must be notified once draining completes - a missed notify hangs the caller forever"
	);
}

#[test]
fn run_to_exhaustion_notifies_even_when_the_class_has_nothing_to_drain() {
	// A caller blocked on a waiter cannot distinguish "still draining" from "there was nothing to drain". If a
	// class that reports Exhausted immediately skipped the notify, shutdown would block on it forever.
	let journal = journal();
	let mut harness =
		TestHarness::new(LifecycleActor::new(Box::new(ScriptedTask::new("idle", 1, journal.clone()))));
	let waiter = Arc::new(WaiterHandle::new());

	harness.send(LifecycleMessage::RunToExhaustion {
		waiter: waiter.clone(),
	});
	harness.process_all();

	assert!(
		waiter.wait_timeout(Duration::from_milliseconds(1).unwrap()),
		"a drain with no backlog must still notify, or the caller deadlocks"
	);
}

#[test]
fn shutdown_stops_the_lane() {
	let journal = journal();
	let mut harness =
		TestHarness::new(LifecycleActor::new(Box::new(ScriptedTask::new("only", 1, journal.clone()))));

	harness.send(LifecycleMessage::Shutdown);
	let directives = harness.process_all();

	assert_eq!(directives, vec![Directive::Stop], "Shutdown must stop the lane");
}

#[test]
fn a_cancelled_context_stops_the_lane_before_running_any_further_work() {
	// Cancellation is how the runtime tears the lane down. Running a slice after cancellation means touching
	// stores that are mid-teardown.
	let journal = journal();
	let mut harness =
		TestHarness::new(LifecycleActor::new(Box::new(ScriptedTask::new("only", 3, journal.clone()))));

	harness.cancel();
	harness.send(LifecycleMessage::Tick);
	let directives = harness.process_all();

	assert!(journal.lock().is_empty(), "no class may run once the context is cancelled");
	assert_eq!(directives, vec![Directive::Stop], "a cancelled lane must stop");
}

#[test]
fn set_interval_retunes_the_timer_without_stopping_the_lane() {
	// Retuning is how a class picks up a changed interval at runtime. Cancelling the old timer must leave the
	// actor alive and armed, or the class silently stops ticking.
	let journal = journal();
	let mut harness =
		TestHarness::new(LifecycleActor::new(Box::new(ScriptedTask::new("only", 1, journal.clone()))));

	harness.send(LifecycleMessage::SetInterval {
		interval: Duration::from_seconds(1).unwrap(),
	});
	let directives = harness.process_all();

	assert!(journal.lock().is_empty(), "retuning must not run a slice of its own");
	assert_eq!(directives, vec![Directive::Continue], "retuning must be inert to liveness, not fatal");
}
