// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use reifydb::{ConfigKey, Value, WithSubsystem, embedded, testing::db::TestDb};
use reifydb_test_harness::assert::column_values;

use crate::flow::state::{await_state_keys, state_keys};

const TIMEOUT: Duration = Duration::from_secs(15);

const JOIN_NODE_TYPE: u8 = 7;

const ANCHOR: &str = "from system::metrics::flow::state::current
	filter { keyspace == 'JOIN_ROW_EXPIRY' }";

const LEFT: &str = "from system::metrics::flow::state::current
	filter { keyspace == 'JOIN_LEFT' }";

const RIGHT: &str = "from system::metrics::flow::state::current
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

fn latest_left_join(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::lhs { id: int4, k: int4, lv: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE TABLE app::rhs { id: int4, k: int4, rv: int4, ts: datetime } with { time: event(ts) }");
	db.admin(r#"CREATE DEFERRED VIEW app::j { k: int4, lv: int4, rv: Option(int4) } AS {
			FROM app::lhs
				| left join { from app::rhs } as r using (k, r.k)
					with { retention: { left: 1s }, latest: true }
				| map { k: k, lv: lv, rv: r_rv }
		}"#);
}

fn insert_left(db: &TestDb) {
	db.command(r#"INSERT app::lhs [{ id: 1, k: 1, lv: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
}

fn insert_right_a(db: &TestDb) {
	db.command(r#"INSERT app::rhs [{ id: 2, k: 1, rv: 7, ts: "2026-01-01T00:00:00.100Z" }]"#);
}

fn insert_right_b(db: &TestDb) {
	db.command(r#"INSERT app::rhs [{ id: 3, k: 1, rv: 8, ts: "2026-01-01T00:00:00.200Z" }]"#);
}

fn advance_past_the_seal(db: &TestDb) {
	db.admin("call storage::advance(app::lhs, cast('2026-01-01T00:01:00Z', datetime))");
	db.admin("call storage::advance(app::rhs, cast('2026-01-01T00:01:00Z', datetime))");
	db.await_all_flows(TIMEOUT);
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
	format!("{SURFACE} filter {{ operator == {operator} }}")
}

fn per_key_state_of(operator: u64) -> String {
	// The schema and the counters are the operator's own bookkeeping, so every other keyspace is per join key.
	format!("{} filter {{ keyspace != 'JOIN_SCHEMA' and keyspace != 'NODE_COUNTER' }}", state_of(operator))
}

fn view_rv(db: &TestDb) -> Vec<Value> {
	let frames = db.query("FROM app::j MAP { rv }");
	match frames.first() {
		Some(frame) => column_values(frame, "rv"),
		None => Vec::new(),
	}
}

#[test]
fn two_right_rows_under_one_key_collapse_into_a_single_slot() {
	// A latest join keeps one right row per key, so a second arrival must replace rather than accumulate.
	let db = setup();
	latest_left_join(&db);
	insert_left(&db);
	insert_right_a(&db);
	insert_right_b(&db);
	db.await_row_count("FROM app::j FILTER { rv == 8 }", 1, TIMEOUT);

	assert_eq!(
		await_state_keys(&db, RIGHT, 1, TIMEOUT),
		1,
		"the right side must hold one slot, not one key per arrival; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(db.row_count("FROM app::j"), 1, "and one left row must publish exactly one joined row");
	assert!(
		matches!(view_rv(&db).as_slice(), [Value::Int4(8)]),
		"the slot must carry the newest right row, found {:?}",
		view_rv(&db)
	);
}

#[test]
fn a_later_right_row_rewrites_the_published_row_instead_of_adding_one() {
	// The slot's replacement must reach the view as an update, or every right arrival leaves a stale row behind.
	let db = setup();
	latest_left_join(&db);
	insert_left(&db);
	insert_right_a(&db);
	db.await_row_count("FROM app::j FILTER { rv == 7 }", 1, TIMEOUT);

	insert_right_b(&db);
	db.await_row_count("FROM app::j FILTER { rv == 8 }", 1, TIMEOUT);

	assert_eq!(db.row_count("FROM app::j FILTER { rv == 7 }"), 0, "the superseded value must not survive the slot");
	assert_eq!(db.row_count("FROM app::j"), 1, "and the replacement must not fan the left row out");
}

#[test]
fn an_unmatched_left_row_publishes_none_and_is_upgraded_in_place() {
	// A left join publishes before any right row exists, so the first slot must update that row rather than add
	// one.
	let db = setup();
	latest_left_join(&db);
	insert_left(&db);
	db.await_row_count("FROM app::j", 1, TIMEOUT);
	assert!(
		matches!(view_rv(&db).as_slice(), [Value::None { .. }]),
		"precondition: an unmatched left row publishes none on the right, found {:?}",
		view_rv(&db)
	);

	insert_right_a(&db);
	db.await_row_count("FROM app::j FILTER { rv == 7 }", 1, TIMEOUT);

	assert_eq!(
		db.row_count("FROM app::j"),
		1,
		"filling the slot must upgrade the published row, never add a second"
	);
}

#[test]
fn emptying_the_slot_returns_the_left_row_to_none() {
	// Without a slot there is nothing to join against, and a left row must fall back rather than vanish.
	let db = setup();
	latest_left_join(&db);
	insert_left(&db);
	insert_right_a(&db);
	db.await_row_count("FROM app::j FILTER { rv == 7 }", 1, TIMEOUT);
	await_state_keys(&db, RIGHT, 1, TIMEOUT);

	db.command("DELETE app::rhs FILTER { id == 2 }");
	db.await_all_flows(TIMEOUT);

	assert_eq!(db.row_count("FROM app::j"), 1, "a left join must keep its row when the slot empties");
	assert!(
		matches!(view_rv(&db).as_slice(), [Value::None { .. }]),
		"and that row must fall back to none, found {:?}",
		view_rv(&db)
	);
	assert_eq!(
		await_state_keys(&db, RIGHT, 0, TIMEOUT),
		0,
		"while the emptied slot frees its state; surface now: {:?}",
		db.query(SURFACE)
	);
}

#[test]
fn the_right_rows_arriving_first_reach_the_same_single_row() {
	// Without a snapshot the slot is read at join time, so ordering must not change what the left row sees.
	let db = setup();
	latest_left_join(&db);
	insert_right_a(&db);
	insert_right_b(&db);
	db.await_all_flows(TIMEOUT);
	insert_left(&db);
	db.await_row_count("FROM app::j", 1, TIMEOUT);

	assert!(
		matches!(view_rv(&db).as_slice(), [Value::Int4(8)]),
		"a left row arriving last must still read the newest slot, found {:?}",
		view_rv(&db)
	);
	assert_eq!(db.row_count("FROM app::j"), 1, "and publish exactly one row");
}

#[test]
fn a_sealed_left_row_frees_its_own_state_and_spares_the_unsealed_slot() {
	// Latest rejects a right seal, so the reaper must take the left side alone and leave the slot addressable.
	let db = setup();
	latest_left_join(&db);
	insert_left(&db);
	insert_right_a(&db);
	db.await_row_count("FROM app::j FILTER { rv == 7 }", 1, TIMEOUT);
	let operator = join_operator(&db);
	assert_eq!(await_state_keys(&db, LEFT, 1, TIMEOUT), 1, "precondition: the left row must be held");

	advance_past_the_seal(&db);

	assert_eq!(
		await_state_keys(&db, LEFT, 0, TIMEOUT),
		0,
		"the sealed left row must be freed; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(state_keys(&db, ANCHOR), 0, "leaving no anchor to fire again");
	assert_eq!(
		await_state_keys(&db, RIGHT, 1, TIMEOUT),
		1,
		"while the slot, which latest forbids sealing, stays addressable; surface now: {:?}",
		db.query(SURFACE)
	);
	assert!(
		state_keys(&db, &per_key_state_of(operator)) > 0,
		"so the key's group must outlive the left side that armed it; surface now: {:?}",
		db.query(&state_of(operator))
	);
	assert_eq!(db.row_count("FROM app::j FILTER { rv == 7 }"), 1, "and the row it published freezes in the view");
}
