// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The flow caught-up watermark must be a materialization barrier for chains of any depth. A cursor
// passing version V does not mean the effects of V are visible: in a multi-hop chain those effects
// exist only as a later commit, so a plain min-of-cursors reports caught-up one commit early.

use std::time::{Duration as StdDuration, Instant};

use reifydb::{ConfigKey, Value, WithSubsystem, embedded};
use reifydb_test_harness::db::TestDb;

const BARRIER_TIMEOUT: StdDuration = StdDuration::from_secs(10);
const ROUNDS: usize = 40;

// The false zero this pins is a narrow window between a flow's commit and its CDC becoming
// consumable, so one insert rarely lands in it; the round count is what makes the guard reliable.
const OUTSTANDING_ROUNDS: usize = 300;

fn await_caught_up(db: &TestDb) {
	// Spins rather than delegating to `await_all_flows`: the defect is a watermark that crosses too
	// early, so a sleep interval hands the chain enough time to finish and hides it.
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

fn prime_flows(db: &TestDb) {
	// Primes every hop to skip-advance the moment the next insert lands; a chain that is still
	// quiet cannot expose the race in the first rounds.
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

#[test]
fn zero_outstanding_means_the_whole_chain_is_materialized() {
	// `lag` is per-object and derived from writes the supervisor has observed, so it legitimately
	// reads zero between a commit and its observation. `outstanding` is per-flow against everything
	// consumable, so zero on every row is the real quiescence signal.
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

#[test]
fn caught_up_is_a_barrier_for_a_three_hop_chain() {
	// The gate is one predicate over the whole flow set, not a one-hop lookahead, so a third hop
	// must not reintroduce lag; a plain min-of-cursors falls one commit behind per extra hop.
	let db = setup();
	create_table(&db);
	create_hop(&db, "v1", "t");
	create_hop(&db, "v2", "v1");
	create_hop(&db, "v3", "v2");

	drive(&db, "v3", 3);
}
