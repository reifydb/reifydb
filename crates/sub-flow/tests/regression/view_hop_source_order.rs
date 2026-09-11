// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use reifydb::{SqliteConfig, Value, WithSubsystem, embedded, testing::db::TestDb};
use reifydb_runtime::{RuntimeConfig, fatal::FatalConfig};

const SETTLE: Duration = Duration::from_secs(10);
const LIVE_ROUNDS: usize = 50;

fn disarmed() -> RuntimeConfig {
	RuntimeConfig::default().fatal(FatalConfig::disarmed())
}

fn memory_db() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_runtime_config(disarmed())
			.with_flow(|f| f)
			.build()
			.expect("build memory db with flow"),
	)
}

fn sqlite_db(config: &SqliteConfig) -> TestDb {
	TestDb::from(
		embedded::sqlite(config.clone())
			.with_runtime_config(disarmed())
			.with_flow(|f| f)
			.build()
			.expect("build sqlite db with flow"),
	)
}

fn settle(db: &TestDb) {
	assert!(db.await_all_flows(SETTLE), "flows never caught up; a stalled gate is a liveness defect, not an ordering one");
}

fn create_tables(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::level { pool: utf8, px: int8 }");
	db.admin("CREATE TABLE app::snap { pool: utf8 }");
}

fn create_curve(db: &TestDb) {
	db.admin(
		"CREATE DEFERRED VIEW app::curve { pool: utf8, usd: int8 } AS { FROM app::level MAP { pool, usd: px * 2 } }",
	);
}

fn create_second_hop(db: &TestDb) {
	db.admin("CREATE DEFERRED VIEW app::curve2 { pool: utf8, usd: int8 } AS { FROM app::curve MAP { pool, usd } }");
}

fn create_cost(db: &TestDb, curve: &str) {
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::cost {{ pool: utf8, usd: int8 }} AS {{ \
		 FROM app::snap \
		 INNER JOIN {{ FROM app::{curve} }} AS c USING (pool, c.pool) WITH {{ snapshot: true, latest: true }} \
		 MAP {{ pool, usd: c_usd }} }}"
	));
}

fn create_left_cost(db: &TestDb) {
	db.admin(
		"CREATE DEFERRED VIEW app::cost { pool: utf8, usd: Option(int8) } AS { \
		 FROM app::snap \
		 LEFT JOIN { FROM app::curve } AS c USING (pool, c.pool) WITH { snapshot: true, latest: true } \
		 MAP { pool, usd: c_usd } }",
	);
}

fn insert_rung(db: &TestDb, pool: &str, px: i64) {
	db.command(&format!("INSERT app::level [{{ pool: '{pool}', px: {px} }}]"));
}

fn insert_header(db: &TestDb, pool: &str) {
	db.command(&format!("INSERT app::snap [{{ pool: '{pool}' }}]"));
}

fn assert_curve_rows(db: &TestDb, want: usize) {
	assert_eq!(
		db.row_count("FROM app::curve"),
		want,
		"the curve view itself is incomplete, so a wrong cost says nothing about ordering across the hop"
	);
}

fn cost_rows(db: &TestDb) -> Vec<(String, Option<i64>)> {
	let mut rows = Vec::new();
	for frame in db.query("FROM app::cost") {
		let pools = frame.columns.iter().find(|c| c.name == "pool").expect("cost must expose a pool column");
		let usds = frame.columns.iter().find(|c| c.name == "usd").expect("cost must expose a usd column");
		for row in 0..pools.data.len() {
			let pool = match pools.data.get_value(row) {
				Value::Utf8(pool) => pool,
				other => panic!("pool must be utf8, got {other:?}"),
			};
			let usd = match usds.data.get_value(row) {
				Value::Int8(usd) => Some(usd),
				Value::None {
					..
				} => None,
				other => panic!("usd must be int8 or none, got {other:?}"),
			};
			rows.push((pool, usd));
		}
	}
	rows.sort();
	rows
}

fn paired(pool: &str, usd: i64) -> (String, Option<i64>) {
	(pool.to_string(), Some(usd))
}

#[test]
fn a_header_after_the_curve_has_its_rows_pairs_with_them() {
	// Without this passing, a red sibling could be failing on the fixture rather than on order.
	let db = memory_db();
	create_tables(&db);
	create_curve(&db);
	create_cost(&db, "curve");

	insert_rung(&db, "a", 10);
	settle(&db);
	assert_curve_rows(&db, 1);
	insert_header(&db, "a");
	settle(&db);

	assert_eq!(cost_rows(&db), vec![paired("a", 20)]);
}

#[test]
fn rungs_and_header_in_one_commit_pair_through_the_view_hop() {
	// A view row stamped with the header's own version must go first, otherwise a same-commit header never pairs.
	let db = memory_db();
	create_tables(&db);
	create_curve(&db);
	create_cost(&db, "curve");

	db.command("INSERT app::level [{ pool: 'a', px: 10 }]; INSERT app::snap [{ pool: 'a' }]");
	settle(&db);
	assert_curve_rows(&db, 1);

	assert_eq!(
		cost_rows(&db),
		vec![paired("a", 20)],
		"the header ran before the curve rows made from its own commit, so the snapshot join paired it with nothing"
	);
}

#[test]
fn rungs_before_header_pair_through_the_view_hop_on_replay() {
	// Views created after the data replay from zero, so curve's output always lands above the header's version.
	let db = memory_db();
	create_tables(&db);
	insert_rung(&db, "a", 10);
	insert_header(&db, "a");

	create_curve(&db);
	create_cost(&db, "curve");
	settle(&db);
	assert_curve_rows(&db, 1);

	assert_eq!(
		cost_rows(&db),
		vec![paired("a", 20)],
		"cost handled the header before the curve rows made from the earlier rung commit"
	);
}

#[test]
fn rungs_before_header_pair_across_fifty_live_rounds() {
	// The live polaris shape; one round where curve commits after the header must already fail the test.
	let db = memory_db();
	create_tables(&db);
	create_curve(&db);
	create_cost(&db, "curve");
	settle(&db);

	for round in 0..LIVE_ROUNDS {
		let pool = format!("p{round:02}");
		insert_rung(&db, &pool, round as i64);
		insert_header(&db, &pool);
	}
	settle(&db);
	assert_curve_rows(&db, LIVE_ROUNDS);

	let want: Vec<_> = (0..LIVE_ROUNDS).map(|round| paired(&format!("p{round:02}"), round as i64 * 2)).collect();
	let got = cost_rows(&db);
	assert_eq!(
		got.len(),
		LIVE_ROUNDS,
		"{} of {LIVE_ROUNDS} headers paired; the rest ran before their curve rows",
		got.len()
	);
	assert_eq!(got, want);
}

#[test]
fn rungs_before_header_pair_through_two_view_hops() {
	// The stamp must survive a view over a view; a second hop that restamps with its input's physical version reorders again.
	let db = memory_db();
	create_tables(&db);
	insert_rung(&db, "a", 10);
	insert_header(&db, "a");

	create_curve(&db);
	create_second_hop(&db);
	create_cost(&db, "curve2");
	settle(&db);
	assert_curve_rows(&db, 1);
	assert_eq!(db.row_count("FROM app::curve2"), 1, "the second hop is incomplete, so cost proves nothing");

	assert_eq!(
		cost_rows(&db),
		vec![paired("a", 20)],
		"after two hops cost handled the header before the rows made from the earlier rung commit"
	);
}

#[test]
fn a_header_pairs_with_the_rung_value_as_of_its_own_commit() {
	// One stamp per batch fails here: curve replays both rung commits in one slice, so the header sees 40 or nothing, never 20.
	let db = memory_db();
	create_tables(&db);
	insert_rung(&db, "a", 10);
	insert_header(&db, "a");
	db.command("UPDATE app::level { px: 20 } FILTER { pool == 'a' }");

	create_curve(&db);
	create_cost(&db, "curve");
	settle(&db);
	assert_curve_rows(&db, 1);

	assert_eq!(
		cost_rows(&db),
		vec![paired("a", 20)],
		"the header must pair with the rung as of its own commit (px 10), not the later update (px 20) nor nothing"
	);
}

#[test]
fn a_header_before_its_rungs_stays_unpaired() {
	// Guards against putting view rows first regardless of stamp, which would pair a header with rungs from its future.
	let db = memory_db();
	create_tables(&db);
	insert_header(&db, "a");
	insert_rung(&db, "a", 10);

	create_curve(&db);
	create_cost(&db, "curve");
	settle(&db);
	assert_curve_rows(&db, 1);

	assert_eq!(
		cost_rows(&db),
		Vec::new(),
		"a snapshot join must never pair a header with rungs committed after it"
	);
}

#[test]
fn a_quiet_upstream_view_does_not_stall_its_consumer() {
	// A gate keyed on upstream output instead of upstream position never opens while curve has nothing to emit.
	let db = memory_db();
	create_tables(&db);
	create_curve(&db);
	create_left_cost(&db);

	insert_header(&db, "a");
	settle(&db);

	assert_eq!(
		cost_rows(&db),
		vec![("a".to_string(), None)],
		"the unmatched header must still reach cost while curve stays quiet"
	);
}

#[test]
fn a_header_after_a_restart_pairs_with_rungs_made_before_it() {
	// Without this passing, the restart sibling could be failing on reopen or replay rather than on order.
	let (config, _guard) = SqliteConfig::test();
	{
		let mut db = sqlite_db(&config);
		create_tables(&db);
		insert_rung(&db, "a", 10);
		create_curve(&db);
		settle(&db);
		assert_curve_rows(&db, 1);
		db.stop();
	}

	let db = sqlite_db(&config);
	insert_header(&db, "a");
	create_cost(&db, "curve");
	settle(&db);

	assert_eq!(cost_rows(&db), vec![paired("a", 20)]);
}

#[test]
fn rungs_before_header_pair_through_the_view_hop_after_a_restart() {
	// The stamp must come back from disk; one held only in memory is gone after the restart and the header runs first again.
	let (config, _guard) = SqliteConfig::test();
	{
		let mut db = sqlite_db(&config);
		create_tables(&db);
		insert_rung(&db, "a", 10);
		insert_header(&db, "a");
		create_curve(&db);
		settle(&db);
		assert_curve_rows(&db, 1);
		db.stop();
	}

	let db = sqlite_db(&config);
	create_cost(&db, "curve");
	settle(&db);

	assert_eq!(
		cost_rows(&db),
		vec![paired("a", 20)],
		"replayed from disk, cost handled the header before the curve rows made from the earlier rung commit"
	);
}
