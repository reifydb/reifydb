// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Every update caller must hand the resolver the row's previous `#time`, not the arrival clock it
// already holds for `updated_at`. Reaching for the wrong local is invisible to the resolver's own
// tests and re-dates every updated row to now, so it is pinned here against a clock that moves.

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	params::Params,
	value::{datetime::DateTime, frame::frame::Frame, identity::IdentityId, system_columns},
};

const BLOCK_TIME: &str = "@2020-01-01T00:00:00Z";

fn block_time() -> DateTime {
	DateTime::from_ymd_hms(2020, 1, 1, 0, 0, 0).unwrap()
}

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t
}

fn query_internal(t: &TestEngine, rql: &str) -> Vec<Frame> {
	let result = t.begin_query(IdentityId::system()).unwrap().rql(rql, Params::None);
	if let Some(e) = result.error {
		panic!("internal query failed: {e:?}\nrql: {rql}")
	}
	result.frames
}

fn only_time(t: &TestEngine, rql: &str) -> DateTime {
	let frames = query_internal(t, rql);
	let time = system_columns::time(&frames[0].batch).unwrap();
	assert_eq!(time.len(), 1, "expected exactly one row from `{rql}`");
	time[0]
}

fn only_updated_at(t: &TestEngine, rql: &str) -> DateTime {
	let frames = query_internal(t, rql);
	let updated_at = system_columns::updated_at(&frames[0].batch).unwrap();
	assert_eq!(updated_at.len(), 1, "expected exactly one row from `{rql}`");
	updated_at[0]
}

#[test]
fn a_processing_time_update_keeps_the_arrival_time_while_updated_at_moves() {
	// The clock advances a year between insert and update so the two stamps cannot coincide;
	// that is what makes a caller passing the arrival clock visible.
	let t = engine();
	t.admin("CREATE TABLE test::audit { id: int4, note: utf8 } WITH { time: processing }");
	t.command(r#"INSERT test::audit [{ id: 1, note: "before" }]"#);

	let inserted_time = only_time(&t, "FROM test::audit");
	let inserted_updated_at = only_updated_at(&t, "FROM test::audit");

	t.mock_clock().advance_secs(365 * 24 * 60 * 60);
	t.command(r#"UPDATE test::audit { note: "after" } FILTER { id == 1 }"#);

	assert_eq!(only_time(&t, "FROM test::audit"), inserted_time, "#time must not move on a processing-time update");
	assert!(
		only_updated_at(&t, "FROM test::audit") > inserted_updated_at,
		"updated_at must move, or the clock never advanced and this test proves nothing"
	);
}

#[test]
fn an_event_time_update_of_an_unrelated_column_leaves_time_alone() {
	// A re-stamp on every update breaks only here: where the populator is edited, the populator
	// and the new #time agree by construction and hide it.
	let t = engine();
	t.admin("CREATE TABLE test::trades { id: int4, qty: int4, at: datetime } WITH { time: event(at) }");
	t.command(&format!(r#"INSERT test::trades [{{ id: 1, qty: 10, at: {BLOCK_TIME} }}]"#));

	t.mock_clock().advance_secs(365 * 24 * 60 * 60);
	t.command("UPDATE test::trades { qty: 99 } FILTER { id == 1 }");

	assert_eq!(
		only_time(&t, "FROM test::trades"),
		block_time(),
		"an edit that does not touch the populator must not move #time"
	);
}

#[test]
fn a_ringbuffer_update_follows_the_same_lifecycle_as_a_table() {
	// The ringbuffer must keep #time on a processing-time update, otherwise it re-dates rows to the clock.
	let t = engine();
	t.admin("CREATE RINGBUFFER test::recent { id: int4, note: utf8 } WITH { capacity: 8, time: processing }");
	t.command(r#"INSERT test::recent [{ id: 1, note: "before" }]"#);
	let inserted_time = only_time(&t, "FROM test::recent");

	t.mock_clock().advance_secs(365 * 24 * 60 * 60);
	t.command(r#"UPDATE test::recent { note: "after" } FILTER { id == 1 }"#);
	assert_eq!(only_time(&t, "FROM test::recent"), inserted_time, "processing-time ringbuffer");
}

#[test]
fn a_series_update_follows_the_same_lifecycle_as_a_table() {
	// An update must keep #time from the populator, never from the series key or the clock.
	let t = engine();
	t.admin("CREATE SERIES test::metrics { ts: int8, note: utf8 } WITH { key: ts, time: processing }");
	t.command(r#"INSERT test::metrics [{ ts: 1000, note: "before" }]"#);
	let inserted_time = only_time(&t, "FROM test::metrics");

	t.mock_clock().advance_secs(365 * 24 * 60 * 60);
	t.command(r#"UPDATE test::metrics { note: "after" } FILTER { ts == 1000 }"#);
	assert_eq!(only_time(&t, "FROM test::metrics"), inserted_time, "processing-time series");

	let t = engine();
	t.admin("CREATE SERIES test::readings { ts: int8, at: datetime, v: int4 } WITH { key: ts, time: event(at) }");
	t.command(&format!(r#"INSERT test::readings [{{ ts: 1000, at: {BLOCK_TIME}, v: 1 }}]"#));

	t.mock_clock().advance_secs(365 * 24 * 60 * 60);
	t.command("UPDATE test::readings { v: 2 } FILTER { ts == 1000 }");
	assert_eq!(only_time(&t, "FROM test::readings"), block_time(), "event-time series");
}
