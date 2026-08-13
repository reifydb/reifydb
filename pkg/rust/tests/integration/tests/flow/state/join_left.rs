// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use reifydb::{ConfigKey, Value, WithSubsystem, embedded, testing::db::TestDb};
use reifydb_test_harness::assert::column_values;

const TIMEOUT: Duration = Duration::from_secs(15);

const JOIN_NODE_TYPE: u8 = 7;

const ANCHORED_KEYS: &str = "from system::metrics::flow::state::current
	filter { keyspace == 'CUSTOM' }";

const LEFT_ROWS: &str = "from system::metrics::flow::state::current
	filter { keyspace == 'JOIN_LEFT' }";

const RIGHT_ROWS: &str = "from system::metrics::flow::state::current
	filter { keyspace == 'JOIN_RIGHT' }";

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

fn sealing_left_join(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::lhs { id: int4, k: int4, lv: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE TABLE app::rhs { id: int4, k: int4, rv: int4, ts: datetime } with { time: event(ts) }");
	db.admin(r#"CREATE DEFERRED VIEW app::j { k: int4, lv: int4, rv: Option(int4) } AS {
			FROM app::lhs
				| left join { from app::rhs } as r using (k, r.k)
					with { seal: { left: { duration: '1s' }, right: { duration: '1s' } } }
				| map { k: k, lv: lv, rv: r_rv }
		}"#);
}

fn unsealed_left_join(db: &TestDb) {
	db.admin("CREATE NAMESPACE keep");
	db.admin("CREATE TABLE keep::lhs { id: int4, k: int4, lv: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE TABLE keep::rhs { id: int4, k: int4, rv: int4, ts: datetime } with { time: event(ts) }");
	db.admin(r#"CREATE DEFERRED VIEW keep::j { k: int4, lv: int4, rv: Option(int4) } AS {
			FROM keep::lhs
				| left join { from keep::rhs } as r using (k, r.k)
				| map { k: k, lv: lv, rv: r_rv }
		}"#);
}

fn fill_one_pair(db: &TestDb) {
	// The surface reports one row per join key, never one per anchor, so a live pair shows a single anchored key.
	db.command(r#"INSERT app::lhs [{ id: 1, k: 1, lv: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.command(r#"INSERT app::rhs [{ id: 2, k: 1, rv: 7, ts: "2026-01-01T00:00:00.500Z" }]"#);
	db.await_row_count("FROM app::j FILTER { rv == 7 }", 1, TIMEOUT);
	db.await_row_count(LEFT_ROWS, 1, TIMEOUT);
	db.await_row_count(RIGHT_ROWS, 1, TIMEOUT);
	db.await_row_count(ANCHORED_KEYS, 1, TIMEOUT);
}

fn advance_to(db: &TestDb, at: &str) {
	db.admin(&format!("call storage::advance(app::lhs, cast('{at}', datetime))"));
	db.admin(&format!("call storage::advance(app::rhs, cast('{at}', datetime))"));
	db.await_all_flows(TIMEOUT);
}

fn advance_past_the_seal(db: &TestDb) {
	advance_to(db, "2026-01-01T00:01:00Z");
}

fn join_operator(db: &TestDb) -> u64 {
	let rql = format!("FROM system::operators FILTER {{ node_type == {JOIN_NODE_TYPE} }} MAP {{ id }}");
	let frames = db.query(&rql);
	let values = column_values(frames.first().expect("system::operators returned no frame"), "id");
	match values.as_slice() {
		[Value::Uint8(id)] => *id,
		other => panic!("expected exactly one join operator, found {other:?}"),
	}
}

fn state_of(operator: u64) -> String {
	format!("from system::metrics::flow::state::current filter {{ operator == {operator} }}")
}

fn per_key_state_of(operator: u64) -> String {
	// Group zero is the operator's own root scope, so only the other groups hold per-join-key state.
	format!("{} filter {{ group != 0 }}", state_of(operator))
}

fn assert_only_bounded_bookkeeping_survives(db: &TestDb, operator: u64) {
	// Each survivor is bounded per operator or per side; anything per row or per key here would grow forever.
	let frames = db.query(&state_of(operator));
	let frame = frames.first().expect("a sealed operator must still report its bookkeeping");

	assert_eq!(
		column_values(frame, "keyspace"),
		vec![Value::Utf8("JOIN_SCHEMA".to_string()), Value::Utf8("NODE_COUNTER".to_string())],
		"the schema names each side's row shape, the counter keeps row numbers unique; surface now: {:?}",
		db.query(&state_of(operator))
	);
	assert_eq!(
		column_values(frame, "keys"),
		vec![Value::Uint8(1), Value::Uint8(1)],
		"one schema and one counter per operator, never a key per row; surface now: {:?}",
		db.query(&state_of(operator))
	);
}

fn view_rv(db: &TestDb, filter: &str) -> Vec<Value> {
	let frames = db.query(&format!("FROM app::j FILTER {{ {filter} }} MAP {{ rv }}"));
	match frames.first() {
		Some(frame) => column_values(frame, "rv"),
		None => Vec::new(),
	}
}

#[test]
fn a_live_pair_reports_both_sides_and_the_key_arming_their_seal() {
	// Without a surface that sees live state, no later assertion about reaping can mean anything.
	let db = setup();
	sealing_left_join(&db);
	fill_one_pair(&db);

	assert_eq!(db.row_count(LEFT_ROWS), 1, "the left row must be held; surface now: {:?}", db.query(SURFACE));
	assert_eq!(db.row_count(RIGHT_ROWS), 1, "the right row must be held; surface now: {:?}", db.query(SURFACE));
	assert_eq!(
		db.row_count(ANCHORED_KEYS),
		1,
		"and the join key must carry an anchor arming the seal; surface now: {:?}",
		db.query(SURFACE)
	);
}

#[test]
fn a_sealed_pair_leaves_the_join_operator_holding_nothing_per_key() {
	// Freeing the anchor but keeping the row, the mapping or the group leaks one of each per join key.
	let db = setup();
	sealing_left_join(&db);
	fill_one_pair(&db);
	let operator = join_operator(&db);
	let per_key = per_key_state_of(operator);
	assert!(
		db.row_count(&per_key) > 0,
		"precondition: live rows must report state; surface now: {:?}",
		db.query(SURFACE)
	);

	advance_past_the_seal(&db);

	let remaining = db.await_exact_row_count(&per_key, 0, TIMEOUT);
	assert_eq!(
		remaining,
		0,
		"a fully sealed join must hold nothing in any per-key keyspace; surface now: {:?}",
		db.query(&state_of(operator))
	);
	assert_only_bounded_bookkeeping_survives(&db, operator);
}

#[test]
fn the_sealed_pairs_published_row_survives_the_reap() {
	// Reclamation must be silent, so freeing state must never retract what the view published.
	let db = setup();
	sealing_left_join(&db);
	fill_one_pair(&db);

	advance_past_the_seal(&db);
	db.await_exact_row_count(ANCHORED_KEYS, 0, TIMEOUT);

	assert_eq!(
		db.row_count("FROM app::j FILTER { rv == 7 }"),
		1,
		"reaping the state behind a sealed pair must not remove the row it published"
	);
	assert_eq!(db.row_count("FROM app::j"), 1, "and the view holds exactly what the seal left it");
}

#[test]
fn a_row_still_inside_its_seal_keeps_its_state_through_a_reap() {
	// A reaper that collected on arrival order rather than due time would take live rows with it.
	let db = setup();
	sealing_left_join(&db);
	fill_one_pair(&db);

	db.command(r#"INSERT app::lhs [{ id: 3, k: 9, lv: 1, ts: "2026-01-01T00:01:00Z" }]"#);
	advance_past_the_seal(&db);

	let survivors = db.await_exact_row_count(ANCHORED_KEYS, 1, TIMEOUT);
	assert_eq!(
		survivors,
		1,
		"the two sealed rows must be reaped and the newest one spared; surface now: {:?}",
		db.query(SURFACE)
	);
}

#[test]
fn an_updated_left_row_pushes_its_seal_out_and_outlives_the_tick_that_sealed_its_partner() {
	// The anchor must follow the row's newest event time, or an actively updated row is reaped mid-life.
	let db = setup();
	sealing_left_join(&db);
	fill_one_pair(&db);

	db.command(r#"UPDATE app::lhs { lv: 50, ts: "2026-01-01T00:02:00Z" } FILTER { id == 1 }"#);
	advance_past_the_seal(&db);

	assert_eq!(
		db.await_exact_row_count(RIGHT_ROWS, 0, TIMEOUT),
		0,
		"precondition: the right row's own seal fell due on this tick; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(
		db.row_count(LEFT_ROWS),
		1,
		"the re-armed left row must outlive the tick that sealed the right row; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(
		db.row_count(ANCHORED_KEYS),
		1,
		"and the key must keep the anchor the update pushed out; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(db.row_count("FROM app::j FILTER { lv == 50 }"), 1, "and the update must have reached the view");
}

#[test]
fn a_mutation_after_the_seal_leaves_the_published_row_where_the_seal_found_it() {
	// A sealed row's mapping is gone, so the update has nowhere to land and must not half-apply.
	let db = setup();
	sealing_left_join(&db);
	fill_one_pair(&db);
	advance_past_the_seal(&db);
	db.await_exact_row_count(ANCHORED_KEYS, 0, TIMEOUT);

	db.command(r#"UPDATE app::lhs { lv: 99, ts: "2026-01-01T00:03:00Z" } FILTER { id == 1 }"#);
	db.await_all_flows(TIMEOUT);

	assert_eq!(db.row_count("FROM app::j FILTER { lv == 99 }"), 0, "a sealed row must not accept a later value");
	assert_eq!(db.row_count("FROM app::j FILTER { lv == 5 }"), 1, "and must still hold the value it was sealed on");
	assert_eq!(db.row_count("FROM app::j"), 1, "a dropped mutation must never add a row either");
}

#[test]
fn a_delete_after_the_seal_cannot_withdraw_the_published_row() {
	// The remove path is lookup-only, so a sealed row's deletion resolves nothing and the view keeps it.
	let db = setup();
	sealing_left_join(&db);
	fill_one_pair(&db);
	advance_past_the_seal(&db);
	db.await_exact_row_count(ANCHORED_KEYS, 0, TIMEOUT);

	db.command("DELETE app::lhs FILTER { id == 1 }");
	db.await_all_flows(TIMEOUT);

	assert_eq!(db.row_count("FROM app::lhs FILTER { id == 1 }"), 0, "precondition: the source row is gone");
	assert_eq!(
		db.row_count("FROM app::j FILTER { rv == 7 }"),
		1,
		"a sealed row cannot be withdrawn: the view outlives the source row it was built from"
	);
	assert_eq!(db.row_count("FROM app::j"), 1, "and the view holds exactly what the seal left it");
}

#[test]
fn a_delete_after_the_seal_frees_no_further_state() {
	// A delete that resolved anything after the seal would be reclaiming state the reaper already took.
	let db = setup();
	sealing_left_join(&db);
	fill_one_pair(&db);
	let operator = join_operator(&db);
	let per_key = per_key_state_of(operator);
	advance_past_the_seal(&db);
	db.await_exact_row_count(&per_key, 0, TIMEOUT);

	db.command("DELETE app::lhs FILTER { id == 1 }");
	db.await_all_flows(TIMEOUT);

	assert_eq!(
		db.row_count(&per_key),
		0,
		"a sealed join must stay empty through a delete it cannot translate; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_only_bounded_bookkeeping_survives(&db, operator);
}

#[test]
fn a_row_inserted_after_the_seal_arms_its_own_state_and_publishes_its_own_row() {
	// A fresh source row must never land on a sealed row's output row, which is frozen and unreachable.
	let db = setup();
	sealing_left_join(&db);
	fill_one_pair(&db);
	advance_past_the_seal(&db);
	db.await_exact_row_count(ANCHORED_KEYS, 0, TIMEOUT);

	db.command(r#"INSERT app::lhs [{ id: 9, k: 4, lv: 11, ts: "2026-01-01T00:02:00Z" }]"#);
	db.await_row_count("FROM app::j", 2, TIMEOUT);

	assert_eq!(db.await_exact_row_count(ANCHORED_KEYS, 1, TIMEOUT), 1, "the new row arms a seal of its own");
	assert_eq!(db.row_count("FROM app::j FILTER { lv == 11 }"), 1, "and publishes its own row");
	assert_eq!(db.row_count("FROM app::j FILTER { rv == 7 }"), 1, "without disturbing what the seal froze");
}

#[test]
fn removing_a_source_row_before_its_seal_takes_its_state_and_its_published_row() {
	// The remove path reclaims inline, so the anchor must come down with the row or it outlives it.
	let db = setup();
	sealing_left_join(&db);
	fill_one_pair(&db);

	db.command("DELETE app::lhs FILTER { id == 1 }");
	db.await_all_flows(TIMEOUT);

	assert_eq!(
		db.await_exact_row_count(LEFT_ROWS, 0, TIMEOUT),
		0,
		"the removed left row must take its own state with it; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(
		db.row_count(RIGHT_ROWS),
		1,
		"only the surviving right row may still be held; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(db.row_count("FROM app::j"), 0, "and the removed left row must take the joined row with it");
}

#[test]
fn a_right_side_match_never_extends_the_left_rows_anchor() {
	// A match is not a write to the left row, so a match that re-armed it would keep a joined row alive forever.
	let db = setup();
	sealing_left_join(&db);
	db.command(r#"INSERT app::lhs [{ id: 1, k: 1, lv: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.command(r#"INSERT app::rhs [{ id: 2, k: 1, rv: 7, ts: "2026-01-01T00:00:10.000Z" }]"#);
	db.await_row_count("FROM app::j FILTER { rv == 7 }", 1, TIMEOUT);
	db.await_row_count(LEFT_ROWS, 1, TIMEOUT);
	db.await_row_count(RIGHT_ROWS, 1, TIMEOUT);

	advance_to(&db, "2026-01-01T00:00:05Z");

	assert_eq!(
		db.await_exact_row_count(LEFT_ROWS, 0, TIMEOUT),
		0,
		"the left anchor must still name the left row's own write one second in; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(
		db.row_count(RIGHT_ROWS),
		1,
		"and the right row, armed ten seconds in, must be untouched; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(db.row_count("FROM app::j FILTER { rv == 7 }"), 1, "and the joined row it published stays frozen");
}

#[test]
fn an_unmatched_left_row_publishes_none_on_the_right_and_seals_like_any_other() {
	// A left join's unmatched row is a published row like any other, so it must arm and reap the same way.
	let db = setup();
	sealing_left_join(&db);
	db.command(r#"INSERT app::lhs [{ id: 1, k: 1, lv: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.await_row_count("FROM app::j", 1, TIMEOUT);
	let operator = join_operator(&db);
	assert!(
		matches!(view_rv(&db, "lv == 5").as_slice(), [Value::None { .. }]),
		"precondition: an unmatched left row publishes none on the right, found {:?}",
		view_rv(&db, "lv == 5")
	);

	advance_past_the_seal(&db);

	assert_eq!(
		db.await_exact_row_count(&per_key_state_of(operator), 0, TIMEOUT),
		0,
		"an unmatched left row must reap exactly like a matched one; surface now: {:?}",
		db.query(&state_of(operator))
	);
	assert_eq!(db.row_count("FROM app::j FILTER { lv == 5 }"), 1, "and the row it published must survive the reap");
	assert_only_bounded_bookkeeping_survives(&db, operator);
}

#[test]
fn a_right_row_arriving_after_the_left_seal_cannot_fill_in_the_published_none() {
	// The sealed left row is gone from state, so a late match has nothing to join against and must publish nothing.
	let db = setup();
	sealing_left_join(&db);
	db.command(r#"INSERT app::lhs [{ id: 1, k: 1, lv: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.await_row_count("FROM app::j", 1, TIMEOUT);
	advance_past_the_seal(&db);
	db.await_exact_row_count(ANCHORED_KEYS, 0, TIMEOUT);

	db.command(r#"INSERT app::rhs [{ id: 2, k: 1, rv: 7, ts: "2026-01-01T00:02:00Z" }]"#);
	db.await_all_flows(TIMEOUT);

	assert!(
		matches!(view_rv(&db, "lv == 5").as_slice(), [Value::None { .. }]),
		"the sealed row froze with none on the right and a late match must not fill it in, found {:?}",
		view_rv(&db, "lv == 5")
	);
	assert_eq!(db.row_count("FROM app::j"), 1, "and a late right row alone must publish nothing of its own");
}

#[test]
fn a_left_join_without_a_seal_keeps_every_row_addressable_forever() {
	// Arming unconditionally would reap joins that never asked to seal, silently freezing their rows.
	let db = setup();
	unsealed_left_join(&db);
	db.command(r#"INSERT keep::lhs [{ id: 1, k: 1, lv: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.command(r#"INSERT keep::rhs [{ id: 2, k: 1, rv: 7, ts: "2026-01-01T00:00:00.500Z" }]"#);
	db.await_row_count("FROM keep::j FILTER { rv == 7 }", 1, TIMEOUT);

	db.admin("call storage::advance(keep::lhs, cast('2026-01-01T00:01:00Z', datetime))");
	db.admin("call storage::advance(keep::rhs, cast('2026-01-01T00:01:00Z', datetime))");
	db.await_all_flows(TIMEOUT);
	db.command(r#"UPDATE keep::lhs { lv: 50, ts: "2026-01-01T00:02:00Z" } FILTER { id == 1 }"#);
	db.await_all_flows(TIMEOUT);

	assert_eq!(
		db.row_count(ANCHORED_KEYS),
		0,
		"an unsealed join arms nothing; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(db.row_count("FROM keep::j FILTER { lv == 50 }"), 1, "and its rows stay updatable indefinitely");
}
