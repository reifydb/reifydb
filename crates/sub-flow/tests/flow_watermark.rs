// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// `await_all_flows` blocks on `flow_consumer()`, so a watermark pinned at 0 hangs every test that
// uses it, and one that advances on discovery rather than on commit races the following query
// against an unmaterialized view. The wall-clock deadline turns the first into a timeout.

use std::time::{Duration as StdDuration, Instant};

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};

fn setup() -> TestDb {
	// `.with_flow(...)` installs the subsystem that registers the caught-up watermark.
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

#[test]
fn flow_consumer_watermark_advances_to_committed_version() {
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4 }");
	db.admin("CREATE DEFERRED VIEW app::v { id: int4 } AS { FROM app::t MAP { id } }");

	db.command("INSERT app::t [{ id: 1 }, { id: 2 }, { id: 3 }]");

	// The highest committed version right after the insert, which the pipeline must catch up to.
	let target = db.watermarks().tx().current().expect("current version");

	// A wall-clock deadline, so a watermark that never advances fails as a timeout, not a hang.
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

	// Queried with no seal poll, so `flow_consumer() >= target` has to be a true materialization
	// barrier: a watermark that advanced on discovery reads an empty view here.
	let rows: usize = db.row_count("FROM app::v");
	assert_eq!(
		rows, 3,
		"the deferred view must be fully materialized the instant flow_consumer reaches the committed \
		 version (caught-up must imply materialized); got {rows}"
	);
}
