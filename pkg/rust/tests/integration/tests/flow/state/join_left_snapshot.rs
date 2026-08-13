// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use reifydb::{
	ConfigKey, Value, WithSubsystem, embedded,
	testing::db::{TestDb, await_value},
};
use reifydb_test_harness::assert::column_values;

const TIMEOUT: Duration = Duration::from_secs(15);

const JOIN_NODE_TYPE: u8 = 7;

const LEFT: &str = "JOIN_LEFT";

const PUBLISHED: &str = "JOIN_PUBLISHED";

const PIN: &str = "JOIN_PIN";

const RIGHT: &str = "JOIN_RIGHT";

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

const PUBLISHED_VERSION_ONLY: &str = "| filter { rv == 7 }";

fn snapshot_left_join(db: &TestDb, tail: &str) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::lhs { id: int4, k: int4, lv: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE TABLE app::rhs { id: int4, k: int4, rv: int4, ts: datetime } with { time: event(ts) }");
	db.admin(&format!(r#"CREATE DEFERRED VIEW app::j {{ k: int4, lv: int4, rv: Option(int4) }} AS {{
			FROM app::lhs
				| left join {{ from app::rhs }} as r using (k, r.k)
					with {{ seal: {{ left: {{ duration: '1s' }} }}, snapshot: true }}
				| map {{ k: k, lv: lv, rv: r_rv }}
				{tail}
		}}"#));
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

fn keys_in(db: &TestDb, keyspace: &str) -> u64 {
	// The surface reports one row per group, so the count that must stay bounded is the keys column, not the rows.
	let rql = format!("{SURFACE} filter {{ keyspace == '{keyspace}' }} map {{ keys }}");
	let frames = db.query(&rql);
	let Some(frame) = frames.first() else {
		return 0;
	};
	column_values(frame, "keys")
		.iter()
		.map(|value| match value {
			Value::Uint8(keys) => *keys,
			other => panic!("the keys column must be a uint8, found {other:?}"),
		})
		.sum()
}

fn await_keys_in(db: &TestDb, keyspace: &str, want: u64) -> u64 {
	await_value(want, TIMEOUT, || keys_in(db, keyspace))
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
	// Group zero is the operator's own root scope, so only the other groups hold per-join-key state.
	format!("{} filter {{ group != 0 }}", state_of(operator))
}

fn view_rv(db: &TestDb) -> Vec<Value> {
	let frames = db.query("FROM app::j MAP { rv }");
	match frames.first() {
		Some(frame) => column_values(frame, "rv"),
		None => Vec::new(),
	}
}

#[test]
fn a_left_row_publishes_against_every_right_row_that_was_already_there() {
	// The snapshot is taken at the left row's write, so it must see the whole right side as it stood then.
	let db = setup();
	snapshot_left_join(&db, "");
	insert_right_a(&db);
	insert_right_b(&db);
	db.await_all_flows(TIMEOUT);
	insert_left(&db);
	db.await_row_count("FROM app::j", 2, TIMEOUT);

	assert_eq!(db.row_count("FROM app::j FILTER { rv == 7 }"), 1, "the older right row must be joined");
	assert_eq!(db.row_count("FROM app::j FILTER { rv == 8 }"), 1, "and so must the newer one");
	assert_eq!(
		await_keys_in(&db, PUBLISHED, 2),
		2,
		"the ledger must record one entry per published pair; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(
		await_keys_in(&db, PIN, 2),
		2,
		"and pin the version each pair was published against; surface now: {:?}",
		db.query(SURFACE)
	);
}

#[test]
fn a_right_row_arriving_after_the_left_row_never_reaches_it() {
	// A snapshot join freezes the left row's view of the right side, so a later arrival must not revise it.
	let db = setup();
	snapshot_left_join(&db, "");
	insert_left(&db);
	db.await_row_count("FROM app::j", 1, TIMEOUT);
	assert!(
		matches!(view_rv(&db).as_slice(), [Value::None { .. }]),
		"precondition: a left row that saw an empty right side publishes none, found {:?}",
		view_rv(&db)
	);

	insert_right_a(&db);
	insert_right_b(&db);
	db.await_all_flows(TIMEOUT);

	assert_eq!(db.row_count("FROM app::j"), 1, "a later right row must not add a row to a frozen left row");
	assert!(
		matches!(view_rv(&db).as_slice(), [Value::None { .. }]),
		"and must not fill in the none it was published with, found {:?}",
		view_rv(&db)
	);
}

#[test]
fn changing_a_right_row_after_publication_leaves_the_published_row_alone() {
	// The published row names a version, so following the right side's later edits would break the snapshot.
	let db = setup();
	snapshot_left_join(&db, "");
	insert_right_a(&db);
	db.await_all_flows(TIMEOUT);
	insert_left(&db);
	db.await_row_count("FROM app::j FILTER { rv == 7 }", 1, TIMEOUT);

	db.command(r#"UPDATE app::rhs { rv: 70, ts: "2026-01-01T00:00:00.300Z" } FILTER { id == 2 }"#);
	db.await_all_flows(TIMEOUT);

	assert_eq!(db.row_count("FROM app::rhs FILTER { rv == 70 }"), 1, "precondition: the source row did change");
	assert_eq!(
		db.row_count("FROM app::j FILTER { rv == 70 }"),
		0,
		"the new value must not reach the published row"
	);
	assert_eq!(db.row_count("FROM app::j FILTER { rv == 7 }"), 1, "which must still carry the version it read");
	assert_eq!(db.row_count("FROM app::j"), 1, "and the edit must not add a row either");
}

#[test]
fn withdrawing_a_left_row_retracts_what_it_published_not_what_the_right_side_holds_now() {
	// The filter drops a remove whose own content fails it, so retracting the wrong version strands the row
	// forever.
	let db = setup();
	snapshot_left_join(&db, PUBLISHED_VERSION_ONLY);
	insert_right_a(&db);
	db.await_all_flows(TIMEOUT);
	insert_left(&db);
	db.await_row_count("FROM app::j FILTER { rv == 7 }", 1, TIMEOUT);

	db.command(r#"UPDATE app::rhs { rv: 70, ts: "2026-01-01T00:00:00.300Z" } FILTER { id == 2 }"#);
	db.await_all_flows(TIMEOUT);
	db.command("DELETE app::lhs FILTER { id == 1 }");
	db.await_all_flows(TIMEOUT);

	assert_eq!(
		db.row_count("FROM app::j"),
		0,
		"the retired version must let the withdrawal match the row that was published, leaving nothing behind"
	);
	assert_eq!(
		await_keys_in(&db, PIN, 0),
		0,
		"and the last reference must take the retired copy with it; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(keys_in(&db, PUBLISHED), 0, "along with the ledger entry naming it");
}

#[test]
fn an_unmatched_left_row_is_recorded_without_pinning_anything() {
	// A none published against an empty right side has no version to hold, so pinning one would leak a copy per
	// row.
	let db = setup();
	snapshot_left_join(&db, "");
	insert_left(&db);
	db.await_row_count("FROM app::j", 1, TIMEOUT);

	assert_eq!(
		await_keys_in(&db, PUBLISHED, 1),
		1,
		"the unmatched row must still be recorded so it can be withdrawn; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(keys_in(&db, PIN), 0, "but nothing was read, so nothing may be pinned");
}

#[test]
fn a_sealed_left_row_takes_its_ledger_entry_and_spares_the_unsealed_right_side() {
	// The ledger is per left row, so a seal that spared it would trade a row leak for a bookkeeping leak.
	let db = setup();
	snapshot_left_join(&db, "");
	insert_right_a(&db);
	db.await_all_flows(TIMEOUT);
	insert_left(&db);
	db.await_row_count("FROM app::j FILTER { rv == 7 }", 1, TIMEOUT);
	let operator = join_operator(&db);
	assert_eq!(await_keys_in(&db, PUBLISHED, 1), 1, "precondition: the live pair must be recorded");

	advance_past_the_seal(&db);

	assert_eq!(
		await_keys_in(&db, PUBLISHED, 0),
		0,
		"the sealed left row must take its ledger entry with it; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(keys_in(&db, PIN), 0, "and release the version it pinned");
	assert_eq!(keys_in(&db, LEFT), 0, "along with the left row itself");
	assert_eq!(
		await_keys_in(&db, RIGHT, 1),
		1,
		"while the right side, which snapshot forbids sealing, stays addressable; surface now: {:?}",
		db.query(SURFACE)
	);
	assert!(
		db.row_count(&per_key_state_of(operator)) > 0,
		"so the key's group must outlive the left side that armed it; surface now: {:?}",
		db.query(&state_of(operator))
	);
	assert_eq!(db.row_count("FROM app::j FILTER { rv == 7 }"), 1, "and the row it published freezes in the view");
}
