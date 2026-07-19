// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Regression guard for the flow "caught up" watermark.
//
// The `reifydb` facade exposes `watermarks().cdc().flow_consumer()`, which reads a
// `FlowCaughtUpWatermark` registered in IoC by the flow subsystem. It reports the version up to
// which every live deferred flow has materialized (min of the poll frontier and the slowest
// flow's processed position). Integration test helpers (`await_flows_completion`,
// `await_query_count`) block on it via `wait_for_flow_consumer` to know when flow processing has
// caught up to a committed version.
//
// If the subsystem forgets to register/advance this watermark, `flow_consumer()` is pinned at 0
// forever and every await-based flow test hangs; if it advanced on mere discovery (before the
// per-flow commit) `await_flows` would race the query and read a not-yet-materialized view. This
// test asserts the watermark reaches the committed version only after a deferred view has
// actually materialized its rows - it fails as a fast timeout (not a hang) because it uses a real
// clock and a wall-clock deadline.

use std::time::{Duration as StdDuration, Instant};

use reifydb::{WithSubsystem, embedded};
use reifydb_test_harness::db::TestDb;

fn setup() -> TestDb {
	// `.with_flow(...)` is what installs the flow subsystem (and thus registers the caught-up
	// watermark); without it there is no deferred flow processing to test.
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

#[test]
fn flow_consumer_watermark_advances_to_committed_version() {
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4 }");
	db.admin("CREATE DEFERRED VIEW app::v { id: int4 } AS { FROM app::t MAP { id } }");

	db.command("INSERT app::t [{ id: 1 }, { id: 2 }, { id: 3 }]");

	// The version we expect the flow pipeline to catch up to: the highest committed version
	// right after the insert.
	let target = db.watermarks().tx().current().expect("current version");

	// Poll the flow consumer watermark on a wall-clock deadline. With the wiring in place it
	// reaches `target` within milliseconds; without it, it stays at 0 and this times out.
	let deadline = Instant::now() + StdDuration::from_secs(10);
	loop {
		let flow_consumer = db.watermarks().cdc().flow_consumer();
		if flow_consumer >= target {
			break;
		}
		if Instant::now() >= deadline {
			panic!(
				"flow consumer watermark did not reach the committed version within 10s: \
				 flow_consumer={} target={} (the flow subsystem is not advancing \
				 FlowConsumerWatermark - every await-based flow test will hang)",
				flow_consumer.0, target.0
			);
		}
		std::thread::sleep(StdDuration::from_millis(5));
	}

	// `flow_consumer() >= target` must be a true materialization barrier, not mere discovery:
	// querying the view immediately (no grace poll - mirroring the test scripts' `(await_flows)`
	// then `query`) must already observe every row. If the watermark advanced before the flow
	// committed, this reads a not-yet-materialized (empty) view and fails - which is exactly the
	// race that made the external sort tests flaky.
	let rows: usize = db.row_count("FROM app::v");
	assert_eq!(
		rows, 3,
		"the deferred view must be fully materialized the instant flow_consumer reaches the committed \
		 version (caught-up must imply materialized); got {rows}"
	);
}
