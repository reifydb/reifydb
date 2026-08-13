// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use reifydb::{ConfigKey, Value, WithSubsystem, embedded, testing::db::TestDb};
use reifydb_test_harness::assert::column_values;

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
	// Group zero is the operator's own root scope, so only the other groups hold per-window state.
	format!("{} filter {{ group != 0 }}", state_of(operator))
}

fn state_of_group(operator: u64, group: u64) -> String {
	format!("{} filter {{ group == {group} }}", state_of(operator))
}

fn groups_holding_accumulators(db: &TestDb, operator: u64) -> Vec<u64> {
	// The group dimension is the only per-window identity the surface exposes, so it is how a survivor is named.
	let rql = format!("{} filter {{ keyspace == 'ACCUMULATOR' }}", state_of(operator));
	let frames = db.query(&rql);
	let Some(frame) = frames.first() else {
		return Vec::new();
	};
	column_values(frame, "group")
		.into_iter()
		.map(|value| match value {
			Value::Uint8(group) => group,
			other => panic!("the group dimension must be an unsigned id, found {other:?}"),
		})
		.collect()
}

fn state_shape(db: &TestDb, rql: &str) -> Vec<(Value, Value, Value)> {
	// Sample time moves every tick, so a comparable shape is what is stored, never when it was observed.
	let frames = db.query(rql);
	let Some(frame) = frames.first() else {
		return Vec::new();
	};
	column_values(frame, "keyspace")
		.into_iter()
		.zip(column_values(frame, "keys"))
		.zip(column_values(frame, "value_bytes"))
		.map(|((keyspace, keys), value_bytes)| (keyspace, keys, value_bytes))
		.collect()
}

fn advance_to(db: &TestDb, at: &str) {
	db.admin(&format!("call storage::advance(app::t, cast('{at}', datetime))"));
	db.await_all_flows(TIMEOUT);
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
		vec![Value::Uint8(2), Value::Uint8(1), Value::Uint8(2)],
		"two counters and one meta per group, but never a key per window; surface now: {:?}",
		db.query(&state_of(operator))
	);
}

fn tumbling_window(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin(r#"CREATE DEFERRED VIEW app::w { g: int4, total: int8 } AS {
			FROM app::t
				| window tumbling { total: math::sum(v) }
					with { interval: "1s", seal: "1s" }
					by { g }
		}"#);
}

fn fill_first_window(db: &TestDb) {
	db.command(r#"INSERT app::t [{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.command(r#"INSERT app::t [{ id: 2, g: 2, v: 7, ts: "2026-01-01T00:00:00.500Z" }]"#);
	db.await_row_count("FROM app::w", 2, TIMEOUT);
	db.await_row_count(ACCUMULATORS, 2, TIMEOUT);
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

	let live = db.row_count(ACCUMULATORS);

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
		db.row_count(&per_window) > 0,
		"precondition: open windows must report state; surface now: {:?}",
		db.query(SURFACE)
	);

	advance_past_the_reap(&db);

	let reaped = db.await_exact_row_count(ACCUMULATORS, 0, TIMEOUT);
	assert_eq!(
		reaped,
		0,
		"a sealed window's accumulator must not outlive the reap; surface now: {:?}",
		db.query(SURFACE)
	);

	let remaining = db.await_exact_row_count(&per_window, 0, TIMEOUT);
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
	db.await_exact_row_count(ACCUMULATORS, 0, TIMEOUT);

	assert_eq!(
		db.row_count("FROM app::w FILTER { total == 5 }"),
		1,
		"reaping the state behind a sealed window must not remove the row it published"
	);
	assert_eq!(db.row_count("FROM app::w FILTER { total == 7 }"), 1);
}

#[test]
fn a_sealed_group_is_reaped_while_a_later_group_keeps_its_state_until_its_own_seal() {
	// A reaper scoped to the operator rather than the group would take the later group with it.
	let db = setup();
	tumbling_window(&db);

	db.command(r#"INSERT app::t [{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.await_row_count("FROM app::w", 1, TIMEOUT);
	db.await_row_count(ACCUMULATORS, 1, TIMEOUT);
	let operator = window_operator(&db);
	let early = groups_holding_accumulators(&db, operator);
	assert_eq!(
		early.len(),
		1,
		"precondition: the first group must hold an accumulator; surface now: {:?}",
		db.query(SURFACE)
	);

	// This insert carries the frontier past the first window's seal, which is what reaps it.
	db.command(r#"INSERT app::t [{ id: 2, g: 2, v: 7, ts: "2026-01-01T00:00:10.000Z" }]"#);
	db.await_row_count("FROM app::w", 2, TIMEOUT);
	db.await_all_flows(TIMEOUT);

	let sealed = state_of_group(operator, early[0]);
	assert_eq!(
		db.await_exact_row_count(&sealed, 0, TIMEOUT),
		0,
		"the sealed group must leave nothing addressable behind; surface now: {:?}",
		db.query(&state_of(operator))
	);

	let late = groups_holding_accumulators(&db, operator);
	assert_eq!(
		late.len(),
		1,
		"the first group sealed and the second is still open, so exactly one accumulator may remain; surface now: {:?}",
		db.query(SURFACE)
	);
	assert!(
		!late.contains(&early[0]),
		"the survivor must be the later group, not the one that sealed; surface now: {:?}",
		db.query(SURFACE)
	);

	let untouched = state_shape(&db, &state_of_group(operator, late[0]));
	assert!(
		!untouched.is_empty(),
		"precondition: the open group must report state; surface now: {:?}",
		db.query(SURFACE)
	);

	advance_to(&db, "2026-01-01T00:00:11.000Z");

	assert_eq!(
		state_shape(&db, &state_of_group(operator, late[0])),
		untouched,
		"a reap pass not yet due for this group must not touch a byte of it; surface now: {:?}",
		db.query(SURFACE)
	);

	advance_to(&db, "2026-01-01T00:01:00.000Z");

	assert_eq!(
		db.await_exact_row_count(ACCUMULATORS, 0, TIMEOUT),
		0,
		"the later group must be reaped once its own seal falls due; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(
		db.await_exact_row_count(&per_window_state_of(operator), 0, TIMEOUT),
		0,
		"neither group may leave per-window state behind; surface now: {:?}",
		db.query(&state_of(operator))
	);
	assert_only_bounded_bookkeeping_survives(&db, operator);

	assert_eq!(db.row_count("FROM app::w FILTER { total == 5 }"), 1, "neither reap may retract a published row");
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

	let survivors = db.await_exact_row_count(ACCUMULATORS, 1, TIMEOUT);
	assert_eq!(
		survivors,
		1,
		"the two sealed groups must be reaped and the open one spared; surface now: {:?}",
		db.query(SURFACE)
	);
}
