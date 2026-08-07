// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration as StdDuration;

use reifydb::{WithSubsystem, embedded};
use reifydb_test_harness::db::TestDb;

const TIMEOUT: StdDuration = StdDuration::from_secs(5);
const EPOCH: &str = "1970-01-01T00:00:00Z";

fn setup(window: &str) -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4, v: float8, ts: datetime } with { time: event(ts) }");
	db.admin(&format!(r#"CREATE DEFERRED VIEW app::r {{ g: int4, total: float8 }} AS {{
			FROM app::t
				| {window}
				by {{ g }}
		}}"#));
	db
}

fn insert(db: &TestDb, g: i32, v: f64, ts: &str) {
	db.command(&format!("INSERT app::t [{{ g: {g}, v: {v}, ts: \"{ts}\" }}]"));
}

fn await_total(db: &TestDb, total: f64, shape: &str) {
	let rql = format!("FROM app::r | filter {{ total == {total} }}");
	let got = db.await_row_count(&rql, 1, TIMEOUT);
	assert_eq!(
		got,
		1,
		"{shape}: a row coordinated at the Unix epoch must reach the view; view now: {:?}",
		db.query_as_root("FROM app::r", ())
	);
}

#[test]
fn a_tumbling_window_publishes_a_row_coordinated_at_the_epoch() {
	let db = setup(r#"window tumbling { total: math::sum(v) } with { interval: "1s", grace: "0s" }"#);
	insert(&db, 1, 10.0, EPOCH);
	insert(&db, 1, 7.0, "1970-01-01T00:00:05Z");
	await_total(&db, 10.0, "tumbling");
}

#[test]
fn a_sliding_window_publishes_a_row_coordinated_at_the_epoch() {
	let db = setup(r#"window sliding { total: math::sum(v) } with { interval: "2s", slide: "1s", grace: "0s" }"#);
	insert(&db, 1, 10.0, EPOCH);
	insert(&db, 1, 7.0, "1970-01-01T00:00:10Z");
	await_total(&db, 10.0, "sliding");
}

#[test]
fn a_session_window_publishes_a_row_coordinated_at_the_epoch() {
	let db = setup(r#"window session { total: math::sum(v) } with { gap: "2s", grace: "0s" }"#);
	insert(&db, 1, 10.0, EPOCH);
	insert(&db, 1, 7.0, "1970-01-01T00:00:10Z");
	await_total(&db, 10.0, "session");
}

#[test]
fn a_session_window_one_hour_past_the_epoch_rotates_across_its_gap() {
	let db = setup(r#"window session { total: math::sum(v) } with { gap: "2s", grace: "0s" }"#);
	insert(&db, 1, 10.0, "1970-01-01T01:00:00Z");
	insert(&db, 1, 7.0, "1970-01-01T01:00:10Z");
	await_total(&db, 10.0, "session one hour past the epoch");
}

#[test]
fn a_rolling_window_retains_a_row_coordinated_at_the_epoch() {
	let db = setup(r#"window rolling { total: math::sum(v) } with { interval: "1h", grace: "5m" }"#);
	insert(&db, 1, 10.0, EPOCH);
	await_total(&db, 10.0, "rolling");
}
