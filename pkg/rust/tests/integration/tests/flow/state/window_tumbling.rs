// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use reifydb::{ConfigKey, Value, WithSubsystem, embedded, testing::db::TestDb};

const TIMEOUT: Duration = Duration::from_secs(15);

const ACCUMULATORS: &str = "from system::metrics::flow::state::current
	filter { keyspace == 'ACCUMULATOR' }";

const IDENTITY: &str = "from system::metrics::flow::state::current
	filter { phase == 'identity' }";

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
fn a_sealed_window_has_its_data_state_reaped_and_its_identity_kept() {
	// State that outlives the seal is exactly the leak the reaper exists to prevent.
	let db = setup();
	tumbling_window(&db);
	fill_first_window(&db);

	advance_past_the_reap(&db);

	let reaped = db.await_exact_row_count(ACCUMULATORS, 0, TIMEOUT);
	assert_eq!(
		reaped,
		0,
		"a sealed window's accumulator must not outlive the reap; surface now: {:?}",
		db.query(SURFACE)
	);

	assert!(
		db.row_count(IDENTITY) >= 1,
		"the reaper erases the data phase only; identity state must survive to keep row numbers stable"
	);
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
