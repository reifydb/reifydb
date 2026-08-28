// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use reifydb::{ConfigKey, Value, WithSubsystem, embedded, testing::db::TestDb};
use reifydb_test_harness::assert::column_values;

use crate::flow::state::{await_state_keys, state_keys};

const TIMEOUT: Duration = Duration::from_secs(15);

const WINDOW_NODE_TYPE: u8 = 15;

const ACCUMULATORS: &str = "from system::metrics::flow::state::current
	filter { keyspace == 'ACCUMULATOR' }";

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

fn window_operator(db: &TestDb) -> u64 {
	let rql = format!("FROM system::operators FILTER {{ node_type == {WINDOW_NODE_TYPE} }} MAP {{ id }}");
	let frames = db.query(&rql);
	let values = column_values(frames.first().expect("system::operators returned no frame"), "id");
	match values.as_slice() {
		[Value::Uint8(id)] => *id,
		other => panic!("expected exactly one window operator, found {other:?}"),
	}
}

fn state_of(operator: u64) -> String {
	format!("from system::metrics::flow::state::current filter {{ operator == {operator} }}")
}

fn per_window_state_of(operator: u64) -> String {
	// The counters, the ledger and the meta are the operator's own bookkeeping, so the rest is per window.
	format!(
		"{} filter {{ keyspace != 'NODE_COUNTER' and keyspace != 'SEAL_LEDGER' and keyspace != 'WINDOW_META' }}",
		state_of(operator)
	)
}

fn assert_only_bounded_bookkeeping_survives(db: &TestDb, operator: u64) {
	// Each of these is bounded per operator or per partition; anything per-window here would grow without end.
	let frames = db.query(&state_of(operator));
	let frame = frames.first().expect("a sealed operator must still report its bookkeeping");

	assert_eq!(
		column_values(frame, "keyspace"),
		vec![
			Value::Utf8("NODE_COUNTER".to_string()),
			Value::Utf8("SEAL_LEDGER".to_string()),
			Value::Utf8("WINDOW_META".to_string()),
		],
		"the counter keeps row numbers unique, the ledger keeps sealing monotonic, the meta drops late events; surface now: {:?}",
		db.query(&state_of(operator))
	);
	assert_eq!(
		column_values(frame, "keys"),
		vec![Value::Uint8(1), Value::Uint8(1), Value::Uint8(2)],
		"one row number counter and one meta per group, but never a key per window; surface now: {:?}",
		db.query(&state_of(operator))
	);
}

fn tumbling_window(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin(r#"CREATE DEFERRED VIEW app::w { g: int4, total: int8 } AS {
			FROM app::t
				| window tumbling { total: math::sum(v) }
					with { duration: 1s, lateness: 1s }
					by { g }
		}"#);
}

fn fill_first_window(db: &TestDb) {
	db.command(r#"INSERT app::t [{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.command(r#"INSERT app::t [{ id: 2, g: 2, v: 7, ts: "2026-01-01T00:00:00.500Z" }]"#);
	db.await_row_count("FROM app::w", 2, TIMEOUT);
	await_state_keys(db, ACCUMULATORS, 2, TIMEOUT);
}

fn advance_past_the_reap(db: &TestDb) {
	db.admin("call storage::advance(app::t, cast('2026-01-01T00:01:00Z', datetime))");
	db.await_all_flows(TIMEOUT);
}

#[test]
fn a_live_window_reports_the_accumulator_holding_its_aggregate() {
	// Without a surface that sees live state, no later assertion about reaping can mean anything.
	let db = setup();
	tumbling_window(&db);
	fill_first_window(&db);

	let live = state_keys(&db, ACCUMULATORS);

	assert_eq!(live, 2, "two open groups must each report an accumulator; surface now: {:?}", db.query(SURFACE));
}

#[test]
fn a_sealed_window_has_both_its_data_and_its_identity_reaped() {
	// A sealed window is closed to everything, so sparing its identity would leave a reachable address per window.
	let db = setup();
	tumbling_window(&db);
	fill_first_window(&db);
	let operator = window_operator(&db);
	let per_window = per_window_state_of(operator);
	assert!(
		state_keys(&db, &per_window) > 0,
		"precondition: open windows must report state; surface now: {:?}",
		db.query(SURFACE)
	);

	advance_past_the_reap(&db);

	let reaped = await_state_keys(&db, ACCUMULATORS, 0, TIMEOUT);
	assert_eq!(
		reaped,
		0,
		"a sealed window's accumulator must not outlive the reap; surface now: {:?}",
		db.query(SURFACE)
	);

	let remaining = await_state_keys(&db, &per_window, 0, TIMEOUT);
	assert_eq!(
		remaining,
		0,
		"a sealed window must keep neither its accumulator nor the mapping addressing it; surface now: {:?}",
		db.query(&state_of(operator))
	);
	assert_only_bounded_bookkeeping_survives(&db, operator);
}

#[test]
fn the_sealed_windows_published_rows_survive_the_reap() {
	// Reclamation must be silent, so freeing state must never retract what the view published.
	let db = setup();
	tumbling_window(&db);
	fill_first_window(&db);

	advance_past_the_reap(&db);
	await_state_keys(&db, ACCUMULATORS, 0, TIMEOUT);

	assert_eq!(
		db.row_count("FROM app::w FILTER { total == 5 }"),
		1,
		"reaping the state behind a sealed window must not remove the row it published"
	);
	assert_eq!(db.row_count("FROM app::w FILTER { total == 7 }"), 1);
}

#[test]
fn a_window_that_is_still_open_keeps_its_accumulator_through_a_reap() {
	// A reaper that collected on seal order rather than per group would take live state with it.
	let db = setup();
	tumbling_window(&db);
	fill_first_window(&db);

	db.command(r#"INSERT app::t [{ id: 3, g: 9, v: 1, ts: "2026-01-01T00:01:00Z" }]"#);
	db.await_all_flows(TIMEOUT);

	let survivors = await_state_keys(&db, ACCUMULATORS, 1, TIMEOUT);
	assert_eq!(
		survivors,
		1,
		"the two sealed groups must be reaped and the open one spared; surface now: {:?}",
		db.query(SURFACE)
	);
}
