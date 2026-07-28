// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// What an UPDATE does to a row's `#time`, end to end, on both domains and on every object kind that
// supports updates.
//
// The unit tests in `vm::instruction::dml::time` pin the resolver's two arms. What they cannot see is
// the wiring: every update caller has to hand the resolver the row's PREVIOUS `#time`, not the arrival
// clock it is already holding to move `updated_at` with. Reaching for the wrong one of those two
// locals is invisible at the resolver and re-dates every updated row to now, so it is pinned here
// instead - against a clock that has demonstrably moved between the insert and the update.

use reifydb_engine::test_harness::TestEngine;
use reifydb_value::value::datetime::DateTime;

const BLOCK_TIME: &str = "@2020-01-01T00:00:00Z";
const CORRECTED_TIME: &str = "@2019-06-01T00:00:00Z";

fn block_time() -> DateTime {
	DateTime::from_ymd_hms(2020, 1, 1, 0, 0, 0).unwrap()
}

fn corrected_time() -> DateTime {
	DateTime::from_ymd_hms(2019, 6, 1, 0, 0, 0).unwrap()
}

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t
}

fn only_time(t: &TestEngine, rql: &str) -> DateTime {
	let frames = t.query(rql);
	let time = frames[0].time();
	assert_eq!(time.len(), 1, "expected exactly one row from `{rql}`");
	time[0]
}

fn only_updated_at(t: &TestEngine, rql: &str) -> DateTime {
	let frames = t.query(rql);
	let updated_at = frames[0].updated_at();
	assert_eq!(updated_at.len(), 1, "expected exactly one row from `{rql}`");
	updated_at[0]
}

#[test]
// Intent: THE wiring test. On a processing-time table an update must carry the original #time forward
// while updated_at moves to now. The clock is advanced by a year between the insert and the update so
// the two stamps cannot coincide, which is what makes "the caller passed the arrival clock" visible:
// #time would land on the update's now instead of the insert's.
// Mutation: pass now_nanos rather than the old row's time_nanos at the update call site and #time
// follows updated_at here, while every resolver unit test still passes.
fn a_processing_time_update_keeps_the_arrival_time_while_updated_at_moves() {
	let t = engine();
	t.admin("CREATE TABLE test::audit { id: int4, note: utf8 }");
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
// Intent: the event-time path end to end. #time follows the populator the author edited, and lands on
// the corrected instant even though the correction points BACKWARDS - which neither the arrival clock
// nor the previous #time can produce, so nothing but a genuine re-read of the row can satisfy this.
// Mutation: give the event arm the processing arm's passthrough and #time stays at the original
// instant; stamp the arrival clock instead and it lands on the advanced now. Both fail here.
fn an_event_time_update_re_reads_the_populator_rather_than_the_clock() {
	let t = engine();
	t.admin("CREATE TABLE test::trades { id: int4, at: datetime } WITH { time: event, ts: at }");
	t.command(&format!(r#"INSERT test::trades [{{ id: 1, at: {BLOCK_TIME} }}]"#));

	assert_eq!(only_time(&t, "FROM test::trades"), block_time());

	t.mock_clock().advance_secs(365 * 24 * 60 * 60);
	t.command(&format!(r#"UPDATE test::trades {{ at: {CORRECTED_TIME} }} FILTER {{ id == 1 }}"#));

	assert_eq!(
		only_time(&t, "FROM test::trades"),
		corrected_time(),
		"#time must follow the corrected populator, backwards and all"
	);
}

#[test]
// Intent: an event-time update that edits some OTHER column leaves #time where it was. This is the
// case a careless "re-stamp on every update" would break without any test that edits the populator
// ever noticing, because there the populator and the new #time agree by construction.
// Mutation: stamp the arrival clock on an event-time update and this lands on the advanced now.
fn an_event_time_update_of_an_unrelated_column_leaves_time_alone() {
	let t = engine();
	t.admin("CREATE TABLE test::trades { id: int4, qty: int4, at: datetime } WITH { time: event, ts: at }");
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
// Intent: a ringbuffer update routes through the same resolver, and is wired to it separately, so the
// domain's update semantics can be honoured for tables and dropped for ringbuffers. Both domains are
// checked because the two arms are reached by different code and fail independently.
// Mutation: hand the ringbuffer caller the arrival clock and the processing half fails while every
// table test above still passes.
fn a_ringbuffer_update_follows_the_same_lifecycle_as_a_table() {
	let t = engine();
	t.admin("CREATE RINGBUFFER test::recent { id: int4, note: utf8 } WITH { capacity: 8 }");
	t.command(r#"INSERT test::recent [{ id: 1, note: "before" }]"#);
	let inserted_time = only_time(&t, "FROM test::recent");

	t.mock_clock().advance_secs(365 * 24 * 60 * 60);
	t.command(r#"UPDATE test::recent { note: "after" } FILTER { id == 1 }"#);
	assert_eq!(only_time(&t, "FROM test::recent"), inserted_time, "processing-time ringbuffer");

	let t = engine();
	t.admin("CREATE RINGBUFFER test::events { id: int4, at: datetime } WITH { capacity: 8, time: event, ts: at }");
	t.command(&format!(r#"INSERT test::events [{{ id: 1, at: {BLOCK_TIME} }}]"#));

	t.mock_clock().advance_secs(365 * 24 * 60 * 60);
	t.command(&format!(r#"UPDATE test::events {{ at: {CORRECTED_TIME} }} FILTER {{ id == 1 }}"#));
	assert_eq!(only_time(&t, "FROM test::events"), corrected_time(), "event-time ringbuffer");
}

#[test]
// Intent: a series update is the third and last wiring of the same resolver. A series already carries a
// temporal key of its own, which is precisely why its #time is worth pinning separately - the key and
// the #time populator are different declarations, and an update must move #time with the populator
// while leaving the processing-time case anchored to arrival.
// Mutation: hand the series caller the arrival clock and the processing half fails; give its event arm
// the passthrough and the event half fails.
fn a_series_update_follows_the_same_lifecycle_as_a_table() {
	let t = engine();
	t.admin("CREATE SERIES test::metrics { ts: int8, note: utf8 } WITH { key: ts }");
	t.command(r#"INSERT test::metrics [{ ts: 1000, note: "before" }]"#);
	let inserted_time = only_time(&t, "FROM test::metrics");

	t.mock_clock().advance_secs(365 * 24 * 60 * 60);
	t.command(r#"UPDATE test::metrics { note: "after" } FILTER { ts == 1000 }"#);
	assert_eq!(only_time(&t, "FROM test::metrics"), inserted_time, "processing-time series");

	let t = engine();
	t.admin("CREATE SERIES test::readings { ts: int8, at: datetime } WITH { key: ts, time: event, ts: at }");
	t.command(&format!(r#"INSERT test::readings [{{ ts: 1000, at: {BLOCK_TIME} }}]"#));

	t.mock_clock().advance_secs(365 * 24 * 60 * 60);
	t.command(&format!(r#"UPDATE test::readings {{ at: {CORRECTED_TIME} }} FILTER {{ ts == 1000 }}"#));
	assert_eq!(only_time(&t, "FROM test::readings"), corrected_time(), "event-time series");
}
