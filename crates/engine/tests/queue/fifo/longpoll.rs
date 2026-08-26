// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::atomic::{AtomicBool, Ordering},
	thread,
	time::{Duration as StdDuration, Instant},
};

use reifydb_core::{execution::ExecutionResult, interface::catalog::id::QueueId};
use reifydb_engine::{queue::lookup::find_queue_id, session::Session};
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

fn queue_id(t: &TestEngine) -> QueueId {
	find_queue_id(t.inner(), TestEngine::identity(), "test::jobs").expect("the queue must exist")
}

fn await_parked(t: &TestEngine, count: usize) {
	// Advancing before the worker is on the registry expires the budget before it ever parks.
	let registry = t.inner().queue_wake();
	let queue = queue_id(t);
	let deadline = Instant::now() + StdDuration::from_secs(5);

	while registry.parked(queue) < count {
		assert!(Instant::now() < deadline, "{count} worker(s) never parked");
		thread::yield_now();
	}
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
	// A timeout must be a clean empty result, and the 299ms probe fails a claim that returns early.
	let t = engine();
	let worker = session(&t);
	let clock = t.mock_clock();
	let returned = AtomicBool::new(false);

	let result = thread::scope(|scope| {
		let handle = scope.spawn(|| {
			let result = claim_wait(&worker, millis(300));
			returned.store(true, Ordering::SeqCst);
			result
		});

		await_parked(&t, 1);
		clock.advance_millis(299);
		thread::sleep(StdDuration::from_millis(50));
		assert!(
			!returned.load(Ordering::SeqCst),
			"the claim must not return one millisecond short of its budget"
		);

		clock.advance_millis(1);
		handle.join().unwrap()
	});

	assert!(result.error.is_none(), "a timed-out claim must be a clean success: {:?}", result.error);
	assert_eq!(rows(&result), 0);
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
	// Under FIFO the older park must win the item, so LIFO order or wake-all break these assertions.
	let t = engine();
	let first = session(&t);
	let second = session(&t);
	let clock = t.mock_clock();

	thread::scope(|scope| {
		let a = scope.spawn(|| claim_wait(&first, Duration::from_seconds(3).unwrap()));
		await_parked(&t, 1);
		let b = scope.spawn(|| claim_wait(&second, millis(600)));
		await_parked(&t, 2);

		t.command("INSERT test::jobs [{ id: 1 }]");

		assert_eq!(rows(&a.join().unwrap()), 1, "the worker that parked first must be the one woken");
		clock.advance_millis(600);
		assert_eq!(rows(&b.join().unwrap()), 0, "a single item must not stampede the second worker");
	});
}
