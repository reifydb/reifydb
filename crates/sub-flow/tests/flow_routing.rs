// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// A CDC batch must only wake the flows whose sources it actually touches. The supervisor decodes
// each batch once and routes a `Wake` to a flow only when the batch's changed shapes intersect that
// flow's source shapes (or the batch carries a flow-origin change). This replaces the old fan-out
// that woke EVERY flow on EVERY batch, which made each flow re-read and re-scan the whole CDC tail -
// O(flows) redundant decodes per batch under continuous ingestion.
//
// The wake is only observable indirectly: an unaffected flow that is NOT woken does not advance its
// processed position, so it pins the global caught-up watermark (a min across all live flows). We
// exploit that here. FLOW_TICK is set to one hour so the per-flow tick (which would otherwise drain
// every flow once a second regardless of routing) cannot fire within the test - leaving routed wakes
// as the only thing that can advance a flow. Under the old wake-all behavior the idle view WOULD be
// woken by the unrelated write, skip the batch, and advance to the committed version, making the
// caught-up watermark reach it and this test fail.

use std::{
	thread,
	time::{Duration as StdDuration, Instant},
};

use reifydb::{Database, Params, WithSubsystem, embedded};
use reifydb_core::interface::catalog::config::ConfigKey;
use reifydb_value::value::Value;

fn setup() -> Database {
	// FLOW_TICK = 1h: the per-flow tick will not fire during the test, so only a routed wake can
	// advance a flow. `with_config` seeds the value before the flow subsystem starts, so every flow
	// actor spawns already reading the long tick.
	embedded::memory()
		.with_config(ConfigKey::FlowTick, Value::duration_seconds(3600))
		.with_flow(|f| f)
		.build()
		.expect("build memory db with flow")
}

fn admin(db: &Database, rql: &str) {
	db.admin_as_root(rql, Params::None).unwrap_or_else(|e| panic!("admin failed: {e:?}\nrql: {rql}"));
}

fn row_count(db: &Database, rql: &str) -> usize {
	let frames = db.query_as_root(rql, Params::None).unwrap_or_else(|e| panic!("query failed: {e:?}\nrql: {rql}"));
	frames.iter().map(|f| f.row_count()).sum()
}

fn await_row_count(db: &Database, rql: &str, want: usize, timeout: StdDuration) -> usize {
	let deadline = Instant::now() + timeout;
	loop {
		let got = row_count(db, rql);
		if got >= want || Instant::now() >= deadline {
			return got;
		}
		thread::sleep(StdDuration::from_millis(20));
	}
}

#[test]
fn unrelated_write_does_not_wake_idle_flow() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE app");
	admin(&db, "CREATE TABLE app::a { id: int4 }");
	admin(&db, "CREATE TABLE app::b { id: int4 }");
	// Two independent deferred views over disjoint source tables.
	admin(&db, "CREATE DEFERRED VIEW app::va { id: int4 } AS { FROM app::a MAP { id } }");
	admin(&db, "CREATE DEFERRED VIEW app::vb { id: int4 } AS { FROM app::b MAP { id } }");

	// Establish vb's flow: a write to its own source table wakes it and it materializes. Waiting for
	// that also fully drains the view-creation batches, so the later write to `a` is a clean, isolated
	// data batch - no newly created flow, hence no `UpdateSources` broadcast that would drain every
	// flow (which would defeat the point of this test).
	db.command_as_root("INSERT app::b [{ id: 100 }]", Params::None).expect("insert b");
	let vb_rows = await_row_count(&db, "FROM app::vb", 1, StdDuration::from_secs(5));
	assert_eq!(vb_rows, 1, "vb must materialize a write to its own source table b; got {vb_rows}");

	// Now write only into table a. Only va sources a.
	db.command_as_root("INSERT app::a [{ id: 1 }, { id: 2 }]", Params::None).expect("insert a");
	let target = db.watermarks().tx().current().expect("current version");

	// The affected view materializes from its routed wake.
	let va = await_row_count(&db, "FROM app::va", 2, StdDuration::from_secs(5));
	assert_eq!(va, 2, "the affected view must materialize from its routed wake; got {va}");

	// Give any (incorrect) wake of vb time to land and advance it before we assert the negative.
	thread::sleep(StdDuration::from_millis(300));

	// The write to `a` does not touch vb's source (table b), so with routed wakes vb is never woken by
	// that batch and cannot tick for an hour. Its position stays at the earlier b-write version, below
	// `target`, pinning the global caught-up watermark (min across live flows) below `target`. Under the
	// removed wake-all fan-out vb would have been woken, skipped the irrelevant batch, and advanced to
	// `target`.
	let caught_up = db.watermarks().cdc().flow_consumer();
	assert!(
		caught_up < target,
		"an unrelated write to table a must not advance the view over table b: \
		 flow_consumer={} reached the committed target={}, which means vb was woken by a batch that \
		 does not touch its sources - the O(flows) wake fan-out this change removes",
		caught_up.0,
		target.0
	);
}

#[test]
fn sequential_writes_materialize_exactly_via_push() {
	// The supervisor pushes each already-decoded CDC batch to the affected flow (FlowActorMessage::
	// Ingest) instead of making it re-read cdc_store, and the actor applies only the versions past its
	// cursor (covers_from <= cursor < up_to). Each insert below is committed on its own, so each is a
	// separate batch/push with an advancing covers_from; we wait for each row to materialize before
	// the next insert so the flow stays exactly caught up and every batch takes the aligned push path.
	// FLOW_TICK is one hour, so the per-flow tick cannot mask a bug: if the push dropped or duplicated
	// the boundary version, the running count would be wrong.
	let db = setup();
	admin(&db, "CREATE NAMESPACE app");
	admin(&db, "CREATE TABLE app::t { id: int4 }");
	admin(&db, "CREATE DEFERRED VIEW app::v { id: int4 } AS { FROM app::t MAP { id } }");

	for id in 1..=6i32 {
		db.command_as_root(&format!("INSERT app::t [{{ id: {id} }}]"), Params::None).expect("insert");
		let want = id as usize;
		let got = await_row_count(&db, "FROM app::v", want, StdDuration::from_secs(3));
		assert_eq!(
			got, want,
			"row {id} must materialize through its own push before the next insert (only the push can \
			 advance the flow under a 1h tick); got {got} rows"
		);
	}
}
