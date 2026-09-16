// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::{Duration, Instant};

use reifydb::{WithSubsystem, embedded, testing::db::TestDb, value::value::duration::Duration as ValueDuration};

#[test]
fn waiting_on_a_poisoned_flow_fails_fast_naming_the_cause() {
	// A poisoned flow never moves the watermark, so the wait must stop with its cause instead of burning the
	// timeout.
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().unwrap());
	db.admin("create namespace test");
	db.admin("create table test::src { id: int4 }");
	db.admin("create deferred view test::broken { v: int4 } as { from test::src map { v: missing } }");
	db.command("insert test::src [{ id: 1 }]");
	let target = db.watermarks().tx().current().unwrap();
	let started = Instant::now();
	let result = db.watermarks().cdc().wait_for_flow_consumer(target, ValueDuration::from_seconds(30).unwrap());
	let elapsed = started.elapsed();
	let err = result.expect_err("a poisoned flow must make the wait an error, never a plain timeout");
	assert!(elapsed < Duration::from_secs(10), "the wait must fail soon after the poison, took {elapsed:?}");
	let message = format!("{err}");
	assert!(message.contains("QUERY_001"), "the error must carry the flow failure cause, got: {message}");
}
