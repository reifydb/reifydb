// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use reifydb::{ConfigKey, Value, WithSubsystem, embedded, testing::db::TestDb};
use reifydb_test_harness::assert::column_values;

const TIMEOUT: Duration = Duration::from_secs(15);

const APPEND_NODE_TYPE: u8 = 9;

const ANCHORS: &str = "from system::metrics::flow::state::current
	filter { keyspace == 'CUSTOM' }";

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

fn sealing_append(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::a { id: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE TABLE app::b { id: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin(r#"CREATE DEFERRED VIEW app::u { id: int4, v: int4 } AS {
			FROM app::a
				| append { from app::b } with { seal: { duration: '1s' } }
		}"#);
}

fn fill_both_inputs(db: &TestDb) {
	db.command(r#"INSERT app::a [{ id: 1, v: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.command(r#"INSERT app::b [{ id: 2, v: 7, ts: "2026-01-01T00:00:00.500Z" }]"#);
	db.await_row_count("FROM app::u", 2, TIMEOUT);
	db.await_row_count(ANCHORS, 2, TIMEOUT);
}

fn advance_past_the_seal(db: &TestDb) {
	db.admin("call storage::advance(app::a, cast('2026-01-01T00:01:00Z', datetime))");
	db.admin("call storage::advance(app::b, cast('2026-01-01T00:01:00Z', datetime))");
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

fn per_row_state_of(operator: u64) -> String {
	// Group zero is the operator's own root scope, so only the other groups hold per-source-row state.
	format!("{} filter {{ group != 0 }}", state_of(operator))
}

fn assert_only_the_row_number_counter_survives(db: &TestDb, operator: u64) {
	// The high-water counter must outlive every seal, or a later insert reissues a row number the view published.
	let frames = db.query(&state_of(operator));
	let frame = frames.first().expect("a sealed operator must still report its row-number counter");

	assert_eq!(
		column_values(frame, "keyspace"),
		vec![Value::Utf8("NODE_COUNTER".to_string())],
		"nothing but the row-number counter may outlive a seal; surface now: {:?}",
		db.query(&state_of(operator))
	);
	assert_eq!(
		column_values(frame, "keys"),
		vec![Value::Uint8(1)],
		"the counter is one key per operator, never one per row; surface now: {:?}",
		db.query(&state_of(operator))
	);
}

fn unsealed_append(db: &TestDb) {
	db.admin("CREATE NAMESPACE keep");
	db.admin("CREATE TABLE keep::a { id: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE TABLE keep::b { id: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin(r#"CREATE DEFERRED VIEW keep::u { id: int4, v: int4 } AS {
			FROM keep::a
				| append { from keep::b }
		}"#);
}

#[test]
fn a_live_row_reports_the_state_that_maps_it_to_its_output_row() {
	// Without a surface that sees live state, no later assertion about reaping can mean anything.
	let db = setup();
	sealing_append(&db);
	fill_both_inputs(&db);

	let live = db.row_count(ANCHORS);

	assert_eq!(
		live,
		2,
		"each source row must report the anchor arming its seal; surface now: {:?}",
		db.query(SURFACE)
	);
}

#[test]
fn a_sealed_row_leaves_the_append_operator_holding_nothing_at_all() {
	// Freeing the anchor but keeping the group, the mapping or the dictionary entry leaks one of each per row.
	let db = setup();
	sealing_append(&db);
	fill_both_inputs(&db);
	let operator = append_operator(&db);
	let per_row = per_row_state_of(operator);
	assert!(
		db.row_count(&per_row) > 0,
		"precondition: live rows must report state; surface now: {:?}",
		db.query(SURFACE)
	);

	advance_past_the_seal(&db);

	let remaining = db.await_exact_row_count(&per_row, 0, TIMEOUT);
	assert_eq!(
		remaining,
		0,
		"a sealed append must hold nothing in any per-row keyspace; surface now: {:?}",
		db.query(&state_of(operator))
	);
	assert_only_the_row_number_counter_survives(&db, operator);
}

#[test]
fn a_sealed_row_has_its_mapping_reaped_and_the_operators_own_keyspaces_emptied() {
	// The mapping is the row's address downstream, so a seal that spares it frees nothing that matters.
	let db = setup();
	sealing_append(&db);
	fill_both_inputs(&db);
	let operator = append_operator(&db);
	let mappings = format!("{} filter {{ keyspace == 'ROW_NUMBER_MAPPING' }}", state_of(operator));
	assert_eq!(db.row_count(&mappings), 2, "precondition: both source rows own a mapping");

	advance_past_the_seal(&db);

	let anchors = db.await_exact_row_count(ANCHORS, 0, TIMEOUT);
	assert_eq!(anchors, 0, "a sealed row's anchor must not outlive the reap; surface now: {:?}", db.query(SURFACE));
	assert_eq!(
		db.await_exact_row_count(&mappings, 0, TIMEOUT),
		0,
		"and neither may the mapping it addressed; surface now: {:?}",
		db.query(&state_of(operator))
	);
}

#[test]
fn the_sealed_rows_published_rows_survive_the_reap() {
	// Reclamation must be silent, so freeing state must never retract what the view published.
	let db = setup();
	sealing_append(&db);
	fill_both_inputs(&db);

	advance_past_the_seal(&db);
	db.await_exact_row_count(ANCHORS, 0, TIMEOUT);

	assert_eq!(
		db.row_count("FROM app::u FILTER { v == 5 }"),
		1,
		"reaping the state behind a sealed row must not remove the row it published"
	);
	assert_eq!(db.row_count("FROM app::u FILTER { v == 7 }"), 1);
}

#[test]
fn a_row_still_inside_its_seal_keeps_its_state_through_a_reap() {
	// A reaper that collected on arrival order rather than due time would take live rows with it.
	let db = setup();
	sealing_append(&db);
	fill_both_inputs(&db);

	db.command(r#"INSERT app::a [{ id: 3, v: 1, ts: "2026-01-01T00:01:00Z" }]"#);
	advance_past_the_seal(&db);

	let survivors = db.await_exact_row_count(ANCHORS, 1, TIMEOUT);
	assert_eq!(
		survivors,
		1,
		"the two sealed rows must be reaped and the newest one spared; surface now: {:?}",
		db.query(SURFACE)
	);
}

#[test]
fn an_updated_row_pushes_its_seal_out_and_outlives_the_tick_that_would_have_sealed_it() {
	// The anchor must follow the row's newest event time, or an actively updated row is reaped mid-life.
	let db = setup();
	sealing_append(&db);
	fill_both_inputs(&db);

	db.command(r#"UPDATE app::a { v: 50, ts: "2026-01-01T00:02:00Z" } FILTER { id == 1 }"#);
	advance_past_the_seal(&db);

	let survivors = db.await_exact_row_count(ANCHORS, 1, TIMEOUT);
	assert_eq!(
		survivors,
		1,
		"the re-armed row must outlive the tick that sealed its untouched neighbour; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(db.row_count("FROM app::u FILTER { v == 50 }"), 1, "and the update must have reached the view");
}

#[test]
fn a_mutation_after_the_seal_leaves_the_published_row_where_the_seal_found_it() {
	// A sealed row's mapping is gone, so the update has nowhere to land and must not half-apply.
	let db = setup();
	sealing_append(&db);
	fill_both_inputs(&db);
	advance_past_the_seal(&db);
	db.await_exact_row_count(ANCHORS, 0, TIMEOUT);

	db.command(r#"UPDATE app::a { v: 99, ts: "2026-01-01T00:03:00Z" } FILTER { id == 1 }"#);
	db.await_all_flows(TIMEOUT);

	assert_eq!(db.row_count("FROM app::u FILTER { v == 99 }"), 0, "a sealed row must not accept a later value");
	assert_eq!(db.row_count("FROM app::u FILTER { v == 5 }"), 1, "and must still hold the value it was sealed on");
	assert_eq!(db.row_count("FROM app::u"), 2, "a dropped mutation must never add a row either");
}

#[test]
fn a_delete_after_the_seal_cannot_withdraw_the_published_row() {
	// The remove path is lookup-only, so a sealed row's deletion resolves nothing and the view keeps it.
	let db = setup();
	sealing_append(&db);
	fill_both_inputs(&db);
	advance_past_the_seal(&db);
	db.await_exact_row_count(ANCHORS, 0, TIMEOUT);

	db.command("DELETE app::a FILTER { id == 1 }");
	db.await_all_flows(TIMEOUT);

	assert_eq!(db.row_count("FROM app::a FILTER { id == 1 }"), 0, "precondition: the source row is gone");
	assert_eq!(
		db.row_count("FROM app::u FILTER { v == 5 }"),
		1,
		"a sealed row cannot be withdrawn: the view outlives the source row it was built from"
	);
	assert_eq!(db.row_count("FROM app::u"), 2, "and the view holds exactly what the seal left it");
}

#[test]
fn a_delete_after_the_seal_frees_no_further_state() {
	// A delete that resolved anything after the seal would be reclaiming state the reaper already took.
	let db = setup();
	sealing_append(&db);
	fill_both_inputs(&db);
	let operator = append_operator(&db);
	let per_row = per_row_state_of(operator);
	advance_past_the_seal(&db);
	db.await_exact_row_count(&per_row, 0, TIMEOUT);

	db.command("DELETE app::a FILTER { id == 1 }");
	db.await_all_flows(TIMEOUT);

	assert_eq!(
		db.row_count(&per_row),
		0,
		"a sealed operator must stay empty through a delete it cannot translate; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_only_the_row_number_counter_survives(&db, operator);
}

#[test]
fn a_row_inserted_after_the_seal_arms_its_own_state_and_publishes_its_own_row() {
	// A fresh source row must never land on a sealed row's output row, which is frozen and unreachable.
	let db = setup();
	sealing_append(&db);
	fill_both_inputs(&db);
	advance_past_the_seal(&db);
	db.await_exact_row_count(ANCHORS, 0, TIMEOUT);

	db.command(r#"INSERT app::a [{ id: 9, v: 11, ts: "2026-01-01T00:02:00Z" }]"#);
	db.await_row_count("FROM app::u", 3, TIMEOUT);

	assert_eq!(db.await_exact_row_count(ANCHORS, 1, TIMEOUT), 1, "the new row arms a seal of its own");
	assert_eq!(db.row_count("FROM app::u FILTER { v == 11 }"), 1, "and publishes its own row");
	assert_eq!(db.row_count("FROM app::u FILTER { v == 5 }"), 1, "without disturbing what the seal froze");
	assert_eq!(db.row_count("FROM app::u FILTER { v == 7 }"), 1);
}

#[test]
fn removing_a_source_row_before_its_seal_takes_its_state_and_its_published_row() {
	// The remove path reclaims inline, so the seal index and anchor must come down with it or they outlive the row.
	let db = setup();
	sealing_append(&db);
	fill_both_inputs(&db);

	db.command("DELETE app::a FILTER { id == 1 }");
	db.await_all_flows(TIMEOUT);

	let survivors = db.await_exact_row_count(ANCHORS, 1, TIMEOUT);
	assert_eq!(
		survivors,
		1,
		"only the surviving source row may keep an anchor; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(db.row_count("FROM app::u FILTER { v == 5 }"), 0, "and the removed row must leave the view");
}

#[test]
fn an_append_without_a_seal_keeps_every_row_addressable_forever() {
	// Arming unconditionally would reap operators that never asked to seal, silently freezing their rows.
	let db = setup();
	unsealed_append(&db);
	db.command(r#"INSERT keep::a [{ id: 1, v: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.command(r#"INSERT keep::b [{ id: 2, v: 7, ts: "2026-01-01T00:00:00.500Z" }]"#);
	db.await_row_count("FROM keep::u", 2, TIMEOUT);

	db.admin("call storage::advance(keep::a, cast('2026-01-01T00:01:00Z', datetime))");
	db.admin("call storage::advance(keep::b, cast('2026-01-01T00:01:00Z', datetime))");
	db.await_all_flows(TIMEOUT);
	db.command(r#"UPDATE keep::a { v: 50, ts: "2026-01-01T00:02:00Z" } FILTER { id == 1 }"#);
	db.await_all_flows(TIMEOUT);

	assert_eq!(db.row_count(ANCHORS), 0, "an unsealed append arms nothing; surface now: {:?}", db.query(SURFACE));
	assert_eq!(db.row_count("FROM keep::u FILTER { v == 50 }"), 1, "and its rows stay updatable indefinitely");
}
