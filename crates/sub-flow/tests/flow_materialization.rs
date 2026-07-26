// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The flow caught-up watermark must be a materialization barrier for chains of ANY depth, not just
// one hop. A flow's cursor passing version V does not mean it has seen the effects of V: in a
// multi-hop chain those effects only exist as a LATER commit, produced by the upstream flow. A
// downstream flow that finds nothing for its own sources at V skips straight past it and publishes
// its cursor as V before that later commit even exists, so a watermark defined as "the min cursor
// across live flows" reports caught-up while the tail of the chain is still one commit behind.
//
// These tests drive a chain repeatedly and assert the tail is complete the instant the barrier
// returns. They must be run with a short flow tick: the downstream flow has to be draining actively
// for its cursor to run ahead, which is exactly what makes this race show up under parallel load
// and vanish when a single test has the machine to itself.

use std::time::{Duration as StdDuration, Instant};

use reifydb::{ConfigKey, Value, WithSubsystem, embedded};
use reifydb_test_harness::db::TestDb;

const BARRIER_TIMEOUT: StdDuration = StdDuration::from_secs(10);
const ROUNDS: usize = 40;

// The false zero this pins is a narrow window between a flow's commit and its CDC becoming
// consumable, so one insert rarely lands in it; the round count is what makes the guard reliable.
const OUTSTANDING_ROUNDS: usize = 300;

// Spins rather than delegating to `await_all_flows`: the defect is a watermark that crosses too
// EARLY, so the tail must be observed at the first instant it reports caught-up. Polling on a sleep
// interval hands the chain enough time to finish and hides the bug entirely.
fn await_caught_up(db: &TestDb) {
	let target = db.watermarks().tx().current().expect("current commit version");
	let deadline = Instant::now() + BARRIER_TIMEOUT;
	while db.watermarks().cdc().flow_consumer() < target {
		assert!(
			Instant::now() < deadline,
			"the caught-up watermark never reached committed version {} - the materialization gate is \
			 stalled, not just imprecise",
			target.0
		);
		std::thread::yield_now();
	}
}

fn setup() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_config(ConfigKey::FlowTick, Value::duration_milliseconds(2))
			.with_flow(|f| f)
			.build()
			.expect("build memory db with flow"),
	)
}

// Lets the periodic tick drain every flow up to the current version, so each hop is primed to
// skip-advance the moment the next insert lands. Without this the chain is quiet at the start of
// the run and the first rounds cannot expose the race.
fn prime_flows(db: &TestDb) {
	assert!(db.await_all_flows(BARRIER_TIMEOUT), "flows never caught up on an idle chain");
	std::thread::sleep(StdDuration::from_millis(100));
}

fn create_table(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4 }");
}

fn create_hop(db: &TestDb, name: &str, source: &str) {
	db.admin(&format!("CREATE DEFERRED VIEW app::{name} {{ id: int4 }} AS {{ FROM app::{source} MAP {{ id }} }}"));
}

fn drive(db: &TestDb, tail: &str, depth: usize) {
	prime_flows(db);

	for round in 0..ROUNDS {
		db.command(&format!("INSERT app::t [{{ id: {round} }}]"));

		await_caught_up(db);

		let want = round + 1;
		let got = db.row_count(&format!("FROM app::{tail}"));
		assert_eq!(
			got, want,
			"round {round}: the barrier returned but the {depth}-hop tail has {got} of {want} rows. \
			 Caught-up claimed a version whose effects had not reached the end of the chain, so every \
			 test that awaits flows and then queries a chained view reads stale rows"
		);
	}
}

// `system::flow::watermarks` carries two different signals per row and they must not be confused.
// `lag` is per-object and derived from writes the flow supervisor has OBSERVED on the CDC stream, so
// it legitimately reads zero in the window between a commit and its observation. `outstanding` is
// per-flow and measured against everything consumable, so zero there - on every row - is the real
// quiescence signal. Computing `outstanding` from the observed tracker instead would reproduce the
// false zero: this asserts it is measured against the consumable frontier.
#[test]
fn zero_outstanding_means_the_whole_chain_is_materialized() {
	let db = setup();
	create_table(&db);
	create_hop(&db, "v1", "t");
	create_hop(&db, "v2", "v1");

	prime_flows(&db);

	for round in 0..OUTSTANDING_ROUNDS {
		db.command(&format!("INSERT app::t [{{ id: {round} }}]"));

		let deadline = Instant::now() + BARRIER_TIMEOUT;
		loop {
			let flow = db.watermarks().flow().expect("flow watermarks");
			let rows = flow.all();
			if !rows.is_empty() && rows.iter().all(|r| r.outstanding == 0) {
				break;
			}
			assert!(Instant::now() < deadline, "round {round}: outstanding never reached zero");
			std::thread::yield_now();
		}

		let want = round + 1;
		let got = db.row_count("FROM app::v2");
		assert_eq!(
			got, want,
			"round {round}: every row reported outstanding 0 but the tail has {got} of {want} rows, so \
			 the column is measured against observed writes rather than everything consumable"
		);
	}
}

#[test]
fn caught_up_is_a_barrier_for_a_two_hop_chain() {
	let db = setup();
	create_table(&db);
	create_hop(&db, "v1", "t");
	create_hop(&db, "v2", "v1");

	drive(&db, "v2", 2);
}

// Depth independence: the gate is a single predicate over the whole flow set, not a one-hop
// lookahead, so a third hop must not reintroduce the lag. This fails by exactly one commit per
// extra hop if the watermark ever regresses to a plain min-of-cursors.
#[test]
fn caught_up_is_a_barrier_for_a_three_hop_chain() {
	let db = setup();
	create_table(&db);
	create_hop(&db, "v1", "t");
	create_hop(&db, "v2", "v1");
	create_hop(&db, "v3", "v2");

	drive(&db, "v3", 3);
}
