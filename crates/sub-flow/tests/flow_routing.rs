// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The supervisor decodes each CDC batch once and pushes the shared, already-decoded batch
// (`FlowActorMessage::Ingest`, an Arc) to EVERY live flow. A flow whose sources the batch does not
// touch consumes it as a pure cursor advance (`step_pushed` -> skip): no store read, no compute,
// and a durable checkpoint every `checkpoint_lag` versions. This matters twice over: an idle flow
// that is never advanced pins the global caught-up watermark (a min across all live flows), and
// its durable checkpoint is part of the minimum that gates CDC log compaction - a parked flow
// would make the CDC log grow without bound. The per-flow store re-scan this used to imply is
// gone: the push carries the decoded batch, so advancing all flows costs one mailbox send each.
//
// The advance is only observable indirectly, through the caught-up watermark. FLOW_TICK is set to
// one hour so the per-flow tick (which would otherwise drain every flow once a second regardless
// of routing) cannot fire within the test - leaving the push as the only thing that can advance a
// flow. If the supervisor went back to skipping unaffected flows, the idle view would stay parked
// at its last relevant version, the watermark would never reach the committed target, and the
// first test below would time out.

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
fn unrelated_write_advances_idle_flow_without_tick() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE app");
	admin(&db, "CREATE TABLE app::a { id: int4 }");
	admin(&db, "CREATE TABLE app::b { id: int4 }");
	// Two independent deferred views over disjoint source tables.
	admin(&db, "CREATE DEFERRED VIEW app::va { id: int4 } AS { FROM app::a MAP { id } }");
	admin(&db, "CREATE DEFERRED VIEW app::vb { id: int4 } AS { FROM app::b MAP { id } }");

	// Establish vb's flow: a write to its own source table pushes it and it materializes. Waiting
	// for that also fully drains the view-creation batches, so the later write to `a` is a clean,
	// isolated data batch.
	db.command_as_root("INSERT app::b [{ id: 100 }]", Params::None).expect("insert b");
	let vb_rows = await_row_count(&db, "FROM app::vb", 1, StdDuration::from_secs(5));
	assert_eq!(vb_rows, 1, "vb must materialize a write to its own source table b; got {vb_rows}");

	// Now write only into table a. Only va sources a; vb is idle for this batch.
	db.command_as_root("INSERT app::a [{ id: 1 }, { id: 2 }]", Params::None).expect("insert a");
	let target = db.watermarks().tx().current().expect("current version");

	// The affected view materializes from its push.
	let va = await_row_count(&db, "FROM app::va", 2, StdDuration::from_secs(5));
	assert_eq!(va, 2, "the affected view must materialize from its push; got {va}");

	// The write to `a` does not touch vb's source (table b), and vb cannot tick for an hour, so
	// only the pushed batch can advance it. The supervisor pushes every batch to every flow; vb
	// consumes the irrelevant batch as a cursor skip (no store read) and advances to `target`.
	// The caught-up watermark is the min across all live flows, so it reaching `target` proves
	// the idle flow advanced. If idle flows were skipped again, vb would pin the watermark below
	// `target` (stalling waiters and, via its parked durable checkpoint, CDC log compaction).
	let deadline = Instant::now() + StdDuration::from_secs(5);
	loop {
		let caught_up = db.watermarks().cdc().flow_consumer();
		if caught_up >= target {
			break;
		}
		assert!(
			Instant::now() < deadline,
			"an unrelated write must advance the idle view over table b via the pushed batch: \
			 flow_consumer={} never reached the committed target={} under a 1h tick, so vb was \
			 skipped by the supervisor and is pinning the caught-up watermark and CDC compaction",
			caught_up.0,
			target.0
		);
		thread::sleep(StdDuration::from_millis(20));
	}
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
