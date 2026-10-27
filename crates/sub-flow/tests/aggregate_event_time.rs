// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_test_harness::assert::column_values;
use reifydb_value::value::{Value, duration::Duration};

const TIMEOUT: Duration = Duration::from_seconds_const(5);

fn setup() -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE DEFERRED VIEW app::agg { g: int4, n: int8, top: int4 } AS { FROM app::t | aggregate { n: math::count(), top: math::max(v) } by { g } }");
	db.admin(
		"CREATE DEFERRED VIEW app::w { s: datetime, n: int8 } AS { FROM app::agg | window tumbling { s: window::start(), n: math::count() } with { duration: 60s, lateness: 1h } }",
	);
	db
}

fn insert(db: &TestDb, id: i32, g: i32, v: i32, ts: &str) {
	db.command(&format!(r#"INSERT app::t [{{ id: {id}, g: {g}, v: {v}, ts: "{ts}" }}]"#));
}

fn buckets(db: &TestDb) -> Vec<(String, Value)> {
	let frames = db.query("FROM app::w");
	let mut out: Vec<(String, Value)> = column_values(&frames[0], "s")
		.iter()
		.map(|v| v.to_string())
		.zip(column_values(&frames[0], "n"))
		.collect();
	out.sort();
	out
}

#[test]
fn a_grouped_row_carries_the_event_time_of_its_rows_into_the_next_window() {
	// Stamping the epoch instead would put every grouped row into one 1970 bucket, whatever its rows' times.
	let db = setup();
	insert(&db, 1, 1, 5, "2026-01-01T00:01:10Z");
	db.await_exact_row_count("FROM app::w", 1, TIMEOUT);

	assert_eq!(buckets(&db), vec![("2026-01-01T00:01:00.000000000Z".to_string(), Value::Int8(1))]);
}

#[test]
fn a_later_row_moves_the_grouped_row_to_its_own_newer_time() {
	// Keeping the first row's time would leave the group in the bucket it no longer belongs to.
	let db = setup();
	insert(&db, 1, 1, 5, "2026-01-01T00:01:10Z");
	db.await_exact_row_count("FROM app::w", 1, TIMEOUT);
	insert(&db, 2, 1, 9, "2026-01-01T00:03:20Z");
	db.await_row_count("FROM app::w | filter { s == @2026-01-01T00:03:00Z and n == 1 }", 1, TIMEOUT);

	let now = buckets(&db);
	assert!(
		now.contains(&("2026-01-01T00:03:00.000000000Z".to_string(), Value::Int8(1))),
		"the group now sits in the 00:03 bucket: {now:?}"
	);
	assert!(
		!now.iter().any(|(s, n)| s.starts_with("2026-01-01T00:01") && *n == Value::Int8(1)),
		"the group left the 00:01 bucket: {now:?}"
	);
}

#[test]
fn two_groups_keep_their_own_event_times() {
	// One group's newer row must not drag another group's row forward in time.
	let db = setup();
	insert(&db, 1, 1, 5, "2026-01-01T00:01:10Z");
	insert(&db, 2, 2, 7, "2026-01-01T00:02:10Z");
	db.await_exact_row_count("FROM app::w", 2, TIMEOUT);

	assert_eq!(
		buckets(&db),
		vec![
			("2026-01-01T00:01:00.000000000Z".to_string(), Value::Int8(1)),
			("2026-01-01T00:02:00.000000000Z".to_string(), Value::Int8(1)),
		]
	);
}
