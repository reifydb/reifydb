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

const BOTH_SIDES: &str = "with { seal: { left: { duration: '1s' }, right: { duration: '1s' } } }";

const LEFT_ONLY: &str = "with { seal: { left: { duration: '1s' } } }";

const RIGHT_ONLY: &str = "with { seal: { right: { duration: '1s' } } }";

const LEFT_SHORT_RIGHT_LONG: &str = "with { seal: { left: { duration: '1s' }, right: { duration: '30s' } } }";

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

fn inner_join(db: &TestDb, with_clause: &str) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::lhs { id: int4, k: int4, lv: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE TABLE app::rhs { id: int4, k: int4, rv: int4, ts: datetime } with { time: event(ts) }");
	db.admin(&format!(r#"CREATE DEFERRED VIEW app::j {{ k: int4, lv: int4, rv: int4 }} AS {{
			FROM app::lhs
				| inner join {{ from app::rhs }} as r using (k, r.k) {with_clause}
				| map {{ k: k, lv: lv, rv: r_rv }}
		}}"#));
}

fn fill_one_pair(db: &TestDb) {
	// The surface reports one row per join key, never one per anchor, so a live pair shows a single anchored key.
	db.command(r#"INSERT app::lhs [{ id: 1, k: 1, lv: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.command(r#"INSERT app::rhs [{ id: 2, k: 1, rv: 7, ts: "2026-01-01T00:00:00.500Z" }]"#);
	db.await_row_count("FROM app::j FILTER { rv == 7 }", 1, TIMEOUT);
	db.await_row_count(LEFT_ROWS, 1, TIMEOUT);
	db.await_row_count(RIGHT_ROWS, 1, TIMEOUT);
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

#[test]
fn a_live_pair_reports_both_sides_and_the_key_arming_their_seal() {
	// Without a surface that sees live state, no later assertion about reaping can mean anything.
	let db = setup();
	inner_join(&db, BOTH_SIDES);
	fill_one_pair(&db);

	assert_eq!(db.row_count(LEFT_ROWS), 1, "the left row must be held; surface now: {:?}", db.query(SURFACE));
	assert_eq!(db.row_count(RIGHT_ROWS), 1, "the right row must be held; surface now: {:?}", db.query(SURFACE));
	assert_eq!(
		db.await_exact_row_count(ANCHORED_KEYS, 1, TIMEOUT),
		1,
		"and the join key must carry an anchor arming the seal; surface now: {:?}",
		db.query(SURFACE)
	);
}

#[test]
fn a_left_only_seal_evicts_the_left_row_and_leaves_the_right_one_addressable() {
	// The two sides seal on their own configuration, so an unconfigured side must never arm or be freed.
	let db = setup();
	inner_join(&db, LEFT_ONLY);
	fill_one_pair(&db);
	assert_eq!(db.await_exact_row_count(ANCHORED_KEYS, 1, TIMEOUT), 1, "precondition: only the left side arms");

	advance_past_the_seal(&db);

	assert_eq!(
		db.await_exact_row_count(LEFT_ROWS, 0, TIMEOUT),
		0,
		"the sealed left row must be freed; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(
		db.row_count(RIGHT_ROWS),
		1,
		"and the unsealed right row must stay addressable; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(db.row_count("FROM app::j FILTER { rv == 7 }"), 1, "while the published row freezes in the view");
}

#[test]
fn a_right_only_seal_evicts_the_right_row_and_leaves_the_left_one_addressable() {
	// The mirror of the left-only case: a rule that keyed off one side would pass one of these and fail the other.
	let db = setup();
	inner_join(&db, RIGHT_ONLY);
	fill_one_pair(&db);
	assert_eq!(db.await_exact_row_count(ANCHORED_KEYS, 1, TIMEOUT), 1, "precondition: only the right side arms");

	advance_past_the_seal(&db);

	assert_eq!(
		db.await_exact_row_count(RIGHT_ROWS, 0, TIMEOUT),
		0,
		"the sealed right row must be freed; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(
		db.row_count(LEFT_ROWS),
		1,
		"and the unsealed left row must stay addressable; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(db.row_count("FROM app::j FILTER { rv == 7 }"), 1, "while the published row freezes in the view");
}

#[test]
fn the_group_that_carried_the_key_outlives_a_seal_on_one_side_only() {
	// Both sides share one group, so reclaiming identity on the first empty side frees state the other still reads.
	let db = setup();
	inner_join(&db, LEFT_ONLY);
	fill_one_pair(&db);
	let operator = join_operator(&db);

	advance_past_the_seal(&db);
	db.await_exact_row_count(LEFT_ROWS, 0, TIMEOUT);

	assert!(
		db.row_count(&per_key_state_of(operator)) > 0,
		"the key's group must survive while the right side still holds a row; surface now: {:?}",
		db.query(&state_of(operator))
	);
}

#[test]
fn both_sides_sealed_leave_the_join_operator_holding_nothing_per_key() {
	// Group identity is reclaimed only once both sides hold nothing, which is exactly this case.
	let db = setup();
	inner_join(&db, BOTH_SIDES);
	fill_one_pair(&db);
	let operator = join_operator(&db);
	assert!(
		db.row_count(&per_key_state_of(operator)) > 0,
		"precondition: live rows must report state; surface now: {:?}",
		db.query(SURFACE)
	);

	advance_past_the_seal(&db);

	assert_eq!(
		db.await_exact_row_count(&per_key_state_of(operator), 0, TIMEOUT),
		0,
		"a fully sealed join must hold nothing in any per-key keyspace; surface now: {:?}",
		db.query(&state_of(operator))
	);
	assert_only_bounded_bookkeeping_survives(&db, operator);
}

#[test]
fn a_longer_right_seal_evicts_on_its_own_schedule_rather_than_the_lefts() {
	// Two spans under one group must expire independently, or the wider one is cut short by the narrower.
	let db = setup();
	inner_join(&db, LEFT_SHORT_RIGHT_LONG);
	db.command(r#"INSERT app::lhs [{ id: 1, k: 1, lv: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.command(r#"INSERT app::rhs [{ id: 2, k: 1, rv: 7, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.await_row_count("FROM app::j FILTER { rv == 7 }", 1, TIMEOUT);
	db.await_row_count(LEFT_ROWS, 1, TIMEOUT);
	db.await_row_count(RIGHT_ROWS, 1, TIMEOUT);
	let operator = join_operator(&db);

	advance_to(&db, "2026-01-01T00:00:05Z");

	assert_eq!(
		db.await_exact_row_count(LEFT_ROWS, 0, TIMEOUT),
		0,
		"the left row is past its own second and the right row is not; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(db.row_count(RIGHT_ROWS), 1, "so the right row must still be addressable");
	assert_eq!(db.row_count(ANCHORED_KEYS), 1, "and the key keeps the right row's anchor");

	advance_to(&db, "2026-01-01T00:01:00Z");

	assert_eq!(
		db.await_exact_row_count(&per_key_state_of(operator), 0, TIMEOUT),
		0,
		"and once its own thirty seconds pass it must be freed too; surface now: {:?}",
		db.query(&state_of(operator))
	);
	assert_eq!(db.row_count("FROM app::j FILTER { rv == 7 }"), 1, "neither eviction may retract the published row");
}

#[test]
fn a_right_side_match_never_extends_the_left_rows_anchor() {
	// A match is not a write to the left row, so a match that re-armed it would keep a joined row alive forever.
	let db = setup();
	inner_join(&db, BOTH_SIDES);
	db.command(r#"INSERT app::lhs [{ id: 1, k: 1, lv: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.command(r#"INSERT app::rhs [{ id: 2, k: 1, rv: 7, ts: "2026-01-01T00:00:10.000Z" }]"#);
	db.await_row_count("FROM app::j FILTER { rv == 7 }", 1, TIMEOUT);
	db.await_row_count(LEFT_ROWS, 1, TIMEOUT);
	db.await_row_count(RIGHT_ROWS, 1, TIMEOUT);

	advance_to(&db, "2026-01-01T00:00:05Z");

	assert_eq!(
		db.await_exact_row_count(LEFT_ROWS, 0, TIMEOUT),
		0,
		"the left anchor must still name the left row's own write, so it seals here; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(db.row_count(RIGHT_ROWS), 1, "while the later right row keeps the anchor its own write gave it");
	assert_eq!(db.row_count("FROM app::j FILTER { rv == 7 }"), 1, "and the joined row it published stays frozen");
}

#[test]
fn a_mutation_after_the_seal_leaves_the_published_row_where_the_seal_found_it() {
	// A sealed row's mapping is gone, so the update has nowhere to land and must not half-apply.
	let db = setup();
	inner_join(&db, BOTH_SIDES);
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
	inner_join(&db, BOTH_SIDES);
	fill_one_pair(&db);
	advance_past_the_seal(&db);
	db.await_exact_row_count(ANCHORED_KEYS, 0, TIMEOUT);

	db.command("DELETE app::rhs FILTER { id == 2 }");
	db.await_all_flows(TIMEOUT);

	assert_eq!(db.row_count("FROM app::rhs FILTER { id == 2 }"), 0, "precondition: the source row is gone");
	assert_eq!(
		db.row_count("FROM app::j FILTER { rv == 7 }"),
		1,
		"a sealed row cannot be withdrawn: the view outlives the source row it was built from"
	);
	assert_eq!(db.row_count("FROM app::j"), 1, "and the view holds exactly what the seal left it");
}

#[test]
fn a_pair_inserted_after_the_seal_arms_its_own_state_and_publishes_its_own_row() {
	// A fresh source row must never land on a sealed row's output row, which is frozen and unreachable.
	let db = setup();
	inner_join(&db, BOTH_SIDES);
	fill_one_pair(&db);
	advance_past_the_seal(&db);
	db.await_exact_row_count(ANCHORED_KEYS, 0, TIMEOUT);

	db.command(r#"INSERT app::lhs [{ id: 9, k: 4, lv: 11, ts: "2026-01-01T00:02:00Z" }]"#);
	db.command(r#"INSERT app::rhs [{ id: 10, k: 4, rv: 13, ts: "2026-01-01T00:02:00Z" }]"#);
	db.await_row_count("FROM app::j FILTER { rv == 13 }", 1, TIMEOUT);

	assert_eq!(db.await_exact_row_count(ANCHORED_KEYS, 1, TIMEOUT), 1, "the new key arms a seal of its own");
	assert_eq!(db.await_exact_row_count(LEFT_ROWS, 1, TIMEOUT), 1, "holding the fresh left row");
	assert_eq!(db.await_exact_row_count(RIGHT_ROWS, 1, TIMEOUT), 1, "and the fresh right row");
	assert_eq!(db.row_count("FROM app::j FILTER { lv == 11 }"), 1, "and publishes its own row");
	assert_eq!(db.row_count("FROM app::j FILTER { rv == 7 }"), 1, "without disturbing what the seal froze");
}

#[test]
fn an_inner_join_without_a_seal_keeps_every_row_addressable_forever() {
	// Arming unconditionally would reap joins that never asked to seal, silently freezing their rows.
	let db = setup();
	inner_join(&db, "");
	fill_one_pair(&db);

	advance_past_the_seal(&db);
	db.command(r#"UPDATE app::lhs { lv: 50, ts: "2026-01-01T00:02:00Z" } FILTER { id == 1 }"#);
	db.await_all_flows(TIMEOUT);

	assert_eq!(
		db.row_count(ANCHORED_KEYS),
		0,
		"an unsealed join arms nothing; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(db.row_count("FROM app::j FILTER { lv == 50 }"), 1, "and its rows stay updatable indefinitely");
}
