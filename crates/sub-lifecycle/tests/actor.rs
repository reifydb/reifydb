// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Scheduling contract of the lifecycle lane.
//!
//! Every reclamation class runs through this one actor, which is also the flush lane: one tick performs exactly one
//! bounded slice, and `RunToExhaustion` always notifies its waiter. The catch-up reschedule is a timer the harness
//! cannot observe, so it is pinned indirectly through `Continue` plus a single `run_slice`.

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
fn a_tick_runs_exactly_one_slice_of_exactly_the_addressed_class() {
	let journal = journal();
	let mut harness = TestHarness::new(LifecycleActor::new(vec![
		Box::new(ScriptedTask::new("first", 1, journal.clone())),
		Box::new(ScriptedTask::new("second", 1, journal.clone())),
	]));

	harness.send(LifecycleMessage::Tick(1));
	let directives = harness.process_all();

	assert_eq!(
		*journal.lock(),
		vec!["second"],
		"Tick(1) must run only the class at index 1 - running neighbours would double-drive their cursors"
	);
	assert_eq!(directives, vec![Directive::Continue], "a completed slice must keep the lane alive");
}

#[test]
fn a_tick_yields_the_lane_after_one_slice_even_when_the_class_still_has_work() {
	// The budget contract. If this actor ever drains inline on Yielded, a class with a large backlog occupies
	// the lane for the whole drain and every other class - including persistent flush - waits behind it.
	let journal = journal();
	let mut harness = TestHarness::new(LifecycleActor::new(vec![Box::new(ScriptedTask::new(
		"backlogged",
		5,
		journal.clone(),
	))]));

	harness.send(LifecycleMessage::Tick(0));
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
	let mut harness = TestHarness::new(LifecycleActor::new(vec![Box::new(ScriptedTask::new(
		"backlogged",
		4,
		journal.clone(),
	))]));
	let waiter = Arc::new(WaiterHandle::new());

	harness.send(LifecycleMessage::RunToExhaustion {
		index: 0,
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
fn run_to_exhaustion_notifies_even_when_the_class_index_does_not_exist() {
	// A caller blocked on a waiter cannot distinguish "still draining" from "there was nothing to drain". If a
	// bad index skipped the notify, shutdown would block on a class that was never registered.
	let mut harness = TestHarness::new(LifecycleActor::new(Vec::new()));
	let waiter = Arc::new(WaiterHandle::new());

	harness.send(LifecycleMessage::RunToExhaustion {
		index: 7,
		waiter: waiter.clone(),
	});
	harness.process_all();

	assert!(
		waiter.wait_timeout(Duration::from_milliseconds(1).unwrap()),
		"an out-of-range RunToExhaustion must still notify, or the caller deadlocks"
	);
}

#[test]
fn a_tick_for_an_unknown_class_is_ignored_without_stopping_the_lane() {
	// Timers outlive the tasks they address during shutdown races. A stale tick must be inert, not fatal: losing
	// the lane would silently stop every other class.
	let journal = journal();
	let mut harness =
		TestHarness::new(LifecycleActor::new(vec![Box::new(ScriptedTask::new("only", 1, journal.clone()))]));

	harness.send(LifecycleMessage::Tick(99));
	let directives = harness.process_all();

	assert!(journal.lock().is_empty(), "an unknown index must run no class");
	assert_eq!(directives, vec![Directive::Continue], "a stale tick must not take the lane down");
}

#[test]
fn shutdown_stops_the_lane() {
	let journal = journal();
	let mut harness =
		TestHarness::new(LifecycleActor::new(vec![Box::new(ScriptedTask::new("only", 1, journal.clone()))]));

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
		TestHarness::new(LifecycleActor::new(vec![Box::new(ScriptedTask::new("only", 3, journal.clone()))]));

	harness.cancel();
	harness.send(LifecycleMessage::Tick(0));
	let directives = harness.process_all();

	assert!(journal.lock().is_empty(), "no class may run once the context is cancelled");
	assert_eq!(directives, vec![Directive::Stop], "a cancelled lane must stop");
}

#[test]
fn set_interval_for_an_unknown_class_is_ignored_without_stopping_the_lane() {
	let mut harness = TestHarness::new(LifecycleActor::new(Vec::new()));

	harness.send(LifecycleMessage::SetInterval {
		index: 4,
		interval: Duration::from_seconds(1).unwrap(),
	});
	let directives = harness.process_all();

	assert_eq!(directives, vec![Directive::Continue], "retuning an unknown class must be inert, not fatal");
}
