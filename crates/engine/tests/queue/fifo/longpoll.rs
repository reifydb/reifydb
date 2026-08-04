// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	thread,
	time::{Duration as StdDuration, Instant},
};

use reifydb_core::execution::ExecutionResult;
use reifydb_engine::session::Session;
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::duration::Duration;

const QUEUE: &str = "CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }";

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(QUEUE);
	t
}

fn session(t: &TestEngine) -> Session {
	Session::trusted(t.inner().clone(), TestEngine::identity())
}

fn millis(value: i64) -> Duration {
	Duration::from_milliseconds(value).unwrap()
}

fn rows(result: &ExecutionResult) -> usize {
	result.frames.iter().map(|frame| frame.row_count()).sum()
}

fn claim_wait(session: &Session, wait_for: Duration) -> ExecutionResult {
	session.claim_wait("test::jobs", "w1", 10, Duration::from_seconds(30).unwrap(), wait_for)
}

#[test]
fn test_a_parked_claim_is_released_by_a_concurrent_insert() {
	// This is the whole point of the step: a worker asks for work with a long budget and gets it
	// the moment somebody enqueues, not when the budget runs out. The 5s budget against a 2s
	// assertion is the discriminator - if the post-commit nudge or the registry breaks, the claim
	// can only come back at 5s (or empty), and both outcomes fail here.
	let t = engine();
	let worker = session(&t);

	let started = Instant::now();
	let parked = thread::scope(|scope| {
		let handle = scope.spawn(|| claim_wait(&worker, Duration::from_seconds(5).unwrap()));
		thread::sleep(StdDuration::from_millis(100));
		t.command("INSERT test::jobs [{ id: 1 }]");
		handle.join().unwrap()
	});
	let elapsed = started.elapsed();

	assert!(parked.error.is_none(), "a woken claim must not fault: {:?}", parked.error);
	assert_eq!(rows(&parked), 1, "the parked worker must receive the item that woke it");
	assert!(
		elapsed < StdDuration::from_secs(2),
		"the claim must return on the wake, not on the {}s budget; took {elapsed:?}",
		5
	);
}

#[test]
fn test_a_claim_that_waits_out_its_budget_returns_zero_rows_as_a_success() {
	// Clients re-poll on the timeout, so it must be an ordinary empty result, never an error.
	// Returning early would also be wrong: a worker that gets an instant empty answer spins.
	let t = engine();
	let worker = session(&t);

	let started = Instant::now();
	let result = claim_wait(&worker, millis(300));
	let elapsed = started.elapsed();

	assert!(result.error.is_none(), "a timed-out claim must be a clean success: {:?}", result.error);
	assert_eq!(rows(&result), 0);
	assert!(elapsed >= StdDuration::from_millis(300), "the claim must wait out its budget; took {elapsed:?}");
}

#[test]
fn test_a_zero_budget_claim_never_parks() {
	// wait_for = 0 is the non-blocking contract every existing caller relies on. Parking here
	// would turn a poll into a stall.
	let t = engine();
	let worker = session(&t);

	let started = Instant::now();
	let result = claim_wait(&worker, Duration::zero());

	assert!(result.error.is_none());
	assert_eq!(rows(&result), 0);
	assert!(started.elapsed() < StdDuration::from_millis(200), "a zero budget must not park at all");
}

#[test]
fn test_a_claim_finds_work_that_is_already_waiting_without_parking() {
	// The loop scans before it waits. If it parked first, an item enqueued before the call would
	// sit until some later insert nudged it, which is the lost-wakeup bug in its most visible form.
	let t = engine();
	t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }]");
	let worker = session(&t);

	let started = Instant::now();
	let result = claim_wait(&worker, Duration::from_seconds(30).unwrap());

	assert_eq!(rows(&result), 2, "both waiting items must come back on the first scan");
	assert!(started.elapsed() < StdDuration::from_millis(500), "existing work must not wait for a nudge");
}

#[test]
fn test_an_unknown_queue_faults_instead_of_parking() {
	// Parking on a nonexistent queue would hide a typo behind a full wait budget. The procedure's
	// own not-found error must surface immediately.
	let t = engine();
	let worker = session(&t);

	let started = Instant::now();
	let result = worker.claim_wait(
		"test::missing",
		"w1",
		1,
		Duration::from_seconds(30).unwrap(),
		Duration::from_seconds(30).unwrap(),
	);

	assert!(result.error.is_some(), "an unknown queue must fault");
	assert!(started.elapsed() < StdDuration::from_millis(500), "an error must not be delayed by the budget");
}

#[test]
fn test_one_insert_wakes_exactly_one_of_two_parked_workers() {
	// Wake-N FIFO end to end. Asserting WHICH worker wins is what makes this a real test: under
	// FIFO the older park is woken and takes the item while the younger one sleeps on, so the
	// outcome is deterministic. Under LIFO the younger worker would be woken and win the race,
	// and under wake-all the winner would be a coin flip - both break this assertion.
	let t = engine();
	let first = session(&t);
	let second = session(&t);

	thread::scope(|scope| {
		let a = scope.spawn(|| claim_wait(&first, Duration::from_seconds(3).unwrap()));
		thread::sleep(StdDuration::from_millis(100));
		let b = scope.spawn(|| claim_wait(&second, millis(600)));
		thread::sleep(StdDuration::from_millis(100));

		t.command("INSERT test::jobs [{ id: 1 }]");

		assert_eq!(rows(&a.join().unwrap()), 1, "the worker that parked first must be the one woken");
		assert_eq!(rows(&b.join().unwrap()), 0, "a single item must not stampede the second worker");
	});
}
