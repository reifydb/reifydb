// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The supervisor pushes each decoded CDC batch to every live flow; a flow the batch does not touch
// consumes it as a pure cursor advance. An idle flow that is never advanced pins the caught-up
// watermark and, through its durable checkpoint, the floor that gates CDC log compaction.

use std::thread;

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_core::interface::catalog::config::ConfigKey;
use reifydb_runtime::context::clock::Clock;
use reifydb_value::value::{Value, duration::Duration};

fn setup() -> TestDb {
	// FLOW_TICK = 1h so the per-flow tick cannot fire during the test, leaving a routed push as the
	// only thing that can advance a flow. `with_config` seeds it before any flow actor spawns.
	TestDb::from(
		embedded::memory()
			.with_config(ConfigKey::FlowTick, Value::duration_seconds(3600))
			.with_flow(|f| f)
			.build()
			.expect("build memory db with flow"),
	)
}

#[test]
fn unrelated_write_advances_idle_flow_without_tick() {
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::a { id: int4 }");
	db.admin("CREATE TABLE app::b { id: int4 }");
	// Two independent deferred views over disjoint source tables.
	db.admin("CREATE DEFERRED VIEW app::va { id: int4 } AS { FROM app::a MAP { id } }");
	db.admin("CREATE DEFERRED VIEW app::vb { id: int4 } AS { FROM app::b MAP { id } }");

	// Establish vb's flow: a write to its own source table pushes it and it materializes. Waiting
	// for that also fully drains the view-creation batches, so the later write to `a` is a clean,
	// isolated data batch.
	db.command("INSERT app::b [{ id: 100 }]");
	let vb_rows = db.await_row_count("FROM app::vb", 1, Duration::from_seconds_const(5));
	assert_eq!(vb_rows, 1, "vb must materialize a write to its own source table b; got {vb_rows}");

	// Now write only into table a. Only va sources a; vb is idle for this batch.
	db.command("INSERT app::a [{ id: 1 }, { id: 2 }]");
	let target = db.watermarks().tx().current().expect("current version");

	// The affected view materializes from its push.
	let va = db.await_row_count("FROM app::va", 2, Duration::from_seconds_const(5));
	assert_eq!(va, 2, "the affected view must materialize from its push; got {va}");

	// The write to `a` does not touch vb's source and vb cannot tick for an hour, so only the
	// pushed batch can advance it. The caught-up watermark is the min across live flows, so it
	// reaching `target` is the only observable proof that the idle flow advanced.
	let deadline = Clock::Real.instant() + Duration::from_seconds_const(5);
	loop {
		let caught_up = db.watermarks().cdc().flow_consumer();
		if caught_up >= target {
			break;
		}
		assert!(
			Clock::Real.instant() < deadline,
			"an unrelated write must advance the idle view over table b via the pushed batch: \
			 flow_consumer={} never reached the committed target={} under a 1h tick, so vb was \
			 skipped by the supervisor and is pinning the caught-up watermark and CDC compaction",
			caught_up.0,
			target.0
		);
		thread::sleep(Duration::from_milliseconds_const(20).to_std());
	}
}

#[test]
fn sequential_writes_materialize_exactly_via_push() {
	// Each insert commits on its own, so each is a separate push with an advancing covers_from, and
	// waiting for each row keeps the flow exactly caught up on the aligned path. Under a 1h tick
	// nothing else can advance it, so a dropped or duplicated boundary version surfaces here.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4 }");
	db.admin("CREATE DEFERRED VIEW app::v { id: int4 } AS { FROM app::t MAP { id } }");

	for id in 1..=6i32 {
		db.command(&format!("INSERT app::t [{{ id: {id} }}]"));
		let want = id as usize;
		let got = db.await_row_count("FROM app::v", want, Duration::from_seconds_const(3));
		assert_eq!(
			got, want,
			"row {id} must materialize through its own push before the next insert (only the push can \
			 advance the flow under a 1h tick); got {got} rows"
		);
	}
}

#[test]
fn every_later_unrelated_write_advances_the_idle_flow_again() {
	// The full wake must fire after every round, or an idle flow advances once and pins cdc compaction.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::a { id: int4 }");
	db.admin("CREATE TABLE app::b { id: int4 }");
	db.admin("CREATE DEFERRED VIEW app::va { id: int4 } AS { FROM app::a MAP { id } }");
	db.admin("CREATE DEFERRED VIEW app::vb { id: int4 } AS { FROM app::b MAP { id } }");

	db.command("INSERT app::b [{ id: 100 }]");
	let vb_rows = db.await_row_count("FROM app::vb", 1, Duration::from_seconds_const(5));
	assert_eq!(vb_rows, 1, "vb must materialize a write to its own source table b; got {vb_rows}");

	for id in 1..=3i32 {
		thread::sleep(Duration::from_milliseconds_const(300).to_std());
		db.command(&format!("INSERT app::a [{{ id: {id} }}]"));
		let target = db.watermarks().tx().current().expect("current version");
		let va_rows = db.await_row_count("FROM app::va", id as usize, Duration::from_seconds_const(5));
		assert_eq!(
			va_rows, id as usize,
			"va must materialize write {id} to its own source table a; got {va_rows}"
		);

		let deadline = Clock::Real.instant() + Duration::from_seconds_const(5);
		loop {
			let caught_up = db.watermarks().cdc().flow_consumer();
			if caught_up >= target {
				break;
			}
			assert!(
				Clock::Real.instant() < deadline,
				"unrelated write {id} must advance the idle view over table b again: flow_consumer={} never \
				 reached target={}, so vb stopped advancing after an earlier write and pins cdc compaction",
				caught_up.0,
				target.0
			);
			thread::sleep(Duration::from_milliseconds_const(20).to_std());
		}
	}
}
