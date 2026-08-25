// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use reifydb::{
	ConfigKey, SqliteConfig, Value, WithSubsystem, embedded,
	testing::db::{TempDbPath, TestDb, poll_until},
};
use reifydb_test_harness::assert::{column_values, rows};

const TIMEOUT: Duration = Duration::from_secs(30);

const SURFACE: &str = "from system::metrics::store::operator::persistent::current";

const SOURCE_ROWS: i32 = 20;

fn open(path: &TempDbPath) -> TestDb {
	// The persistent operator tier only exists on an on-disk database, and only a flow-enabled build
	// writes anything into it; the metrics cadence has to outrun the assertions or the surface is empty.
	TestDb::from(
		embedded::sqlite(SqliteConfig::new(path))
			.with_flow(|f| f)
			.with_config(ConfigKey::MetricsFlushInterval, Value::duration_milliseconds(10))
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("an on-disk database with flows and a fast metrics cadence"),
	)
}

fn define_distinct(db: &TestDb) {
	// Distinct must persist one durable key per admitted value; append stores nothing and would leave the table
	// empty.
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::a { id: int4, v: int4 }");
	db.admin("CREATE DEFERRED VIEW app::u { id: int4, v: int4 } AS { FROM app::a | distinct { v } }");
}

fn fill(db: &TestDb) {
	let want = (SOURCE_ROWS * 2) as usize;
	for i in 0..(SOURCE_ROWS * 2) {
		db.command(&format!("INSERT app::a [{{ id: {i}, v: {i} }}]"));
	}
	assert_eq!(db.await_row_count("FROM app::u", want, TIMEOUT), want, "the view never absorbed every source row");
	db.await_all_flows(TIMEOUT);
}

fn measure(db: &TestDb, metric: &str) -> Option<u64> {
	let frames = db.query(SURFACE);
	let frame = frames.first()?;
	match column_values(frame, metric).first() {
		Some(Value::Uint8(count)) => Some(*count),
		Some(other) => panic!("{metric} must be an unsigned count, found {other:?}"),
		None => None,
	}
}

fn sampled(db: &TestDb, metric: &str) -> u64 {
	match poll_until(|| measure(db, metric), TIMEOUT) {
		Some(count) => count,
		None => panic!("the operator persistent surface never published {metric}"),
	}
}

fn await_at_least(db: &TestDb, metric: &str, want: u64) -> u64 {
	match poll_until(|| measure(db, metric).filter(|count| *count >= want), TIMEOUT) {
		Some(count) => count,
		None => panic!("{metric} never reached {want}; surface now: {:?}", rows(&db.query(SURFACE))),
	}
}

#[test]
fn every_operator_state_backed_row_is_still_addressable_after_a_restart() {
	// The corruption case, end to end. A bloom that answers "definitely absent" for a key that exists
	// makes the store report no such row, which is silent data loss, and a freshly armed bloom holds
	// none of the keys a populated operator_state table already carries. The published view rows live
	// in the multi store and survive a restart on their own, so comparing them across the reopen cannot
	// see a poisoned filter by itself; the update afterwards is what forces a point read of persisted
	// operator state, and it can only land on the existing row if that read still finds its mapping.
	let path = TempDbPath::new("operator_filter_restart_rows");
	let want = (SOURCE_ROWS * 2) as usize;

	let before = {
		let mut db = open(&path);
		define_distinct(&db);
		fill(&db);
		let before = rows(&db.query("FROM app::u"));
		db.stop();
		before
	};
	assert_eq!(before.len(), want, "precondition: the view must hold every source row before the stop");

	let mut db = open(&path);
	db.await_all_flows(TIMEOUT);

	let after = rows(&db.query("FROM app::u"));
	assert_eq!(before, after, "a reopened database must serve back exactly the rows it published before the stop");

	db.command("UPDATE app::a { v: 999 } FILTER { id == 5 }");
	db.await_all_flows(TIMEOUT);

	assert_eq!(
		db.await_row_count("FROM app::u FILTER { v == 999 }", 1, TIMEOUT),
		1,
		"the update never reached the view, so a durable operator-state key read back as absent after the reopen"
	);
	assert_eq!(
		db.row_count("FROM app::u"),
		want,
		"the update must land on the row the operator already published, never add a second one"
	);
	db.stop();
}

#[test]
fn a_restart_rebuilds_the_filter_instead_of_leaving_it_disabled() {
	// A database that already holds operator state has to open with the filter disabled, because a
	// fresh bloom holds none of its durable keys. Disabled is safe but inert: may_contain answers true
	// for every key and each point read falls through to sqlite. Nothing about the rows served changes,
	// so no data assertion can catch a filter that stays disabled forever - the rebuild counter is the
	// only surface that can, which is what makes this test the guard on that regression.
	let path = TempDbPath::new("operator_filter_restart_rebuild");

	{
		let mut db = open(&path);
		define_distinct(&db);
		fill(&db);
		db.stop();
	}

	let mut db = open(&path);
	db.await_all_flows(TIMEOUT);

	let rebuilds = await_at_least(&db, "filter_rebuilds", 1);
	assert!(rebuilds >= 1, "the reopened store never rebuilt its filter from the rows already on disk");
	assert_eq!(
		sampled(&db, "filter_enabled"),
		1,
		"a committed rebuild must leave the filter active; surface now: {:?}",
		rows(&db.query(SURFACE))
	);
	db.stop();
}

#[test]
fn a_fresh_database_opens_with_its_filter_already_armed() {
	// An empty operator_state table is the one case where an empty bloom tells the truth, so arming at
	// open costs nothing and removes the window in which the store reads sqlite for keys it has never
	// written. filter_rebuilds is the discriminating assertion here: a gate that never armed would still
	// reach filter_enabled == 1 within milliseconds, because the driver rebuilds any disabled filter it
	// is handed, and only a zero rebuild count proves the filter was armed at open rather than repaired
	// just after it. The rebuild interval is far longer than this test runs, so the zero is not a race.
	let path = TempDbPath::new("operator_filter_fresh");
	let mut db = open(&path);
	define_distinct(&db);
	fill(&db);

	assert_eq!(
		sampled(&db, "filter_enabled"),
		1,
		"an empty operator_state table must arm the filter at open; surface now: {:?}",
		rows(&db.query(SURFACE))
	);
	assert_eq!(
		sampled(&db, "filter_rebuilds"),
		0,
		"the filter was repaired by a rebuild instead of being armed at open; surface now: {:?}",
		rows(&db.query(SURFACE))
	);
	db.stop();
}
