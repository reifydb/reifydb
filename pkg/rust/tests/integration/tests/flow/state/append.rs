// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use reifydb::{ConfigKey, Value, WithSubsystem, embedded, testing::db::TestDb};
use reifydb_test_harness::assert::column_values;

use crate::flow::state::{await_state_keys, state_keys};

const TIMEOUT: Duration = Duration::from_secs(15);

const APPEND_NODE_TYPE: u8 = 9;

const SURFACE: &str = "from system::metrics::flow::state::current";

fn setup() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_flow(|f| f)
			.with_config(ConfigKey::MetricsFlushInterval, Value::duration_milliseconds(10))
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build memory db with flow and a fast metrics cadence"),
	)
}

fn append_view(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::a { id: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE TABLE app::b { id: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin(r#"CREATE DEFERRED VIEW app::u { id: int4, v: int4 } AS {
			FROM app::a
				| append { from app::b }
		}"#);
}

fn fill_both_inputs(db: &TestDb) {
	db.command(r#"INSERT app::a [{ id: 1, v: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.command(r#"INSERT app::b [{ id: 2, v: 7, ts: "2026-01-01T00:00:00.500Z" }]"#);
	db.await_row_count("FROM app::u", 2, TIMEOUT);
}

fn advance_far_past_every_input(db: &TestDb) {
	db.admin("call storage::advance(app::a, cast('2026-06-01T00:00:00Z', datetime))");
	db.admin("call storage::advance(app::b, cast('2026-06-01T00:00:00Z', datetime))");
	db.await_all_flows(TIMEOUT);
}

fn append_operator(db: &TestDb) -> u64 {
	let rql = format!("FROM system::operators FILTER {{ node_type == {APPEND_NODE_TYPE} }} MAP {{ id }}");
	let frames = db.query(&rql);
	let values = column_values(frames.first().expect("system::operators returned no frame"), "id");
	match values.as_slice() {
		[Value::Uint8(id)] => *id,
		other => panic!("expected exactly one append operator, found {other:?}"),
	}
}

fn state_of(operator: u64) -> String {
	format!("from system::metrics::flow::state::current filter {{ operator == {operator} }}")
}

#[test]
fn an_append_holds_no_state_for_any_row_it_has_ever_seen() {
	// The output row is computed from the lane and the source row, so any key at all here is state that must not
	// exist.
	let db = setup();
	append_view(&db);
	fill_both_inputs(&db);
	let operator = append_operator(&db);

	assert_eq!(
		await_state_keys(&db, &state_of(operator), 0, TIMEOUT),
		0,
		"a stateless append must own no keyspace whatsoever; surface now: {:?}",
		db.query(SURFACE)
	);
}

#[test]
fn state_stays_empty_however_many_rows_pass_through() {
	// A per-row key that only appears under volume is the leak the seal machinery existed to bound.
	let db = setup();
	append_view(&db);
	let operator = append_operator(&db);

	for id in 0..50 {
		db.command(&format!(r#"INSERT app::a [{{ id: {id}, v: {id}, ts: "2026-01-01T00:00:00.000Z" }}]"#));
	}
	db.await_row_count("FROM app::u", 50, TIMEOUT);

	assert_eq!(
		state_keys(&db, &state_of(operator)),
		0,
		"append must not accumulate a single key across 50 rows; surface now: {:?}",
		db.query(SURFACE)
	);
}

#[test]
fn a_retraction_withdraws_the_published_row_however_late_it_arrives() {
	// This is the defect the stateless rewrite fixes: a late delete once found no mapping and was dropped in
	// silence.
	let db = setup();
	append_view(&db);
	fill_both_inputs(&db);
	advance_far_past_every_input(&db);

	db.command("DELETE app::a FILTER { id == 1 }");
	db.await_all_flows(TIMEOUT);

	assert_eq!(db.row_count("FROM app::a FILTER { id == 1 }"), 0, "precondition: the source row is gone");
	assert_eq!(
		db.row_count("FROM app::u FILTER { v == 5 }"),
		0,
		"a retraction must resolve no matter how long it waited, or the view strands a stale row forever"
	);
	assert_eq!(db.row_count("FROM app::u"), 1, "and the untouched branch must keep its row");
}

#[test]
fn a_mutation_lands_however_late_it_arrives() {
	// An update is a retraction plus an insert on the same output row, so lateness must not half-apply it either.
	let db = setup();
	append_view(&db);
	fill_both_inputs(&db);
	advance_far_past_every_input(&db);

	db.command(r#"UPDATE app::a { v: 99, ts: "2026-06-02T00:00:00Z" } FILTER { id == 1 }"#);
	db.await_all_flows(TIMEOUT);

	assert_eq!(db.row_count("FROM app::u FILTER { v == 99 }"), 1, "a late update must reach the view");
	assert_eq!(db.row_count("FROM app::u FILTER { v == 5 }"), 0, "and must replace the value it overwrote");
	assert_eq!(db.row_count("FROM app::u"), 2, "an update must never add a row");
}

#[test]
fn two_branches_sharing_a_source_row_number_publish_two_distinct_rows() {
	// Both inputs mint row 1, so without a lane bit they alias onto one output row and one branch vanishes.
	let db = setup();
	append_view(&db);

	db.command(r#"INSERT app::a [{ id: 1, v: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.command(r#"INSERT app::b [{ id: 1, v: 7, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.await_row_count("FROM app::u", 2, TIMEOUT);

	assert_eq!(db.row_count("FROM app::u FILTER { v == 5 }"), 1, "the left branch keeps its own output row");
	assert_eq!(db.row_count("FROM app::u FILTER { v == 7 }"), 1, "and so does the right");
}

#[test]
fn a_source_row_keeps_one_output_row_across_repeated_updates() {
	// The stamp is a total function of lane and source row, so re-deriving it must never mint a second view row.
	let db = setup();
	append_view(&db);
	db.command(r#"INSERT app::a [{ id: 1, v: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.await_row_count("FROM app::u", 1, TIMEOUT);

	for (minute, v) in [(1, 10), (2, 20), (3, 30)] {
		db.command(&format!(
			r#"UPDATE app::a {{ v: {v}, ts: "2026-01-01T00:0{minute}:00Z" }} FILTER {{ id == 1 }}"#
		));
		db.await_all_flows(TIMEOUT);
	}

	assert_eq!(db.row_count("FROM app::u"), 1, "three updates to one source row must leave exactly one view row");
	assert_eq!(db.row_count("FROM app::u FILTER { v == 30 }"), 1, "holding the newest value");
}
