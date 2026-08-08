// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A seal won through a view hop, across process lifetimes. The frontier registry itself is covered
//! in-crate (round-trip in `deferred::output_frontier`, resolve/hydrate ordering in
//! `reifydb_flow::transaction::frontier`); a hydrated entry always resolves as withheld until its
//! producer republishes, so registry hydration has no running-flow observable and this suite does not
//! claim one. What only a full database can show is that a bucket sealed against a silent producer's
//! frontier is still sealed after a close and reopen.
//!
//! A memory build disables operator snapshots, so this suite is sqlite-only: without a generation the
//! arena boots empty and every window reopens regardless of what any frontier said.

use std::{thread::sleep, time::Duration};

use reifydb::{
	SqliteConfig, Value, WithSubsystem, core::interface::catalog::config::ConfigKey, embedded,
	testing::db::{TempDbPath, TestDb},
};

const TIMEOUT: Duration = Duration::from_secs(10);

fn open(path: &TempDbPath) -> TestDb {
	TestDb::from(
		embedded::sqlite(SqliteConfig::new(path))
			// Without a generation the arena boots empty and every window reopens, so the restart would prove nothing about frontiers.
			.with_config(ConfigKey::OperatorSnapshotInterval, Value::duration_seconds(1))
			.with_flow(|f| f)
			.build()
			.expect("build a sqlite database with the flow subsystem"),
	)
}

/// One commit plus a barrier on the flow consumer watermark, because a frontier crosses one hop per
/// round and a chain needs one round per view hop before its consumer can seal against anything.
fn settle_round(db: &TestDb, id: i32, g: i32) {
	db.command(&format!(r#"INSERT rst::busy [{{ id: {id}, g: {g}, v: 1, ts: "2026-01-01T00:00:10Z" }}]"#));
	assert!(db.await_all_flows(TIMEOUT), "every flow must reach the committed version, or this is not a barrier");
}

fn declare(db: &TestDb) {
	db.admin("CREATE NAMESPACE rst");
	db.admin("CREATE TABLE rst::busy { id: int4, g: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE TABLE rst::quiet { id: int4, g: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE DEFERRED VIEW rst::mid_quiet { id: int4, g: int4, v: int4, ts: datetime } AS { FROM rst::quiet }");
	db.admin("CREATE DEFERRED VIEW rst::mid_busy { id: int4, g: int4, v: int4, ts: datetime } AS { FROM rst::busy }");
	db.admin(r#"CREATE DEFERRED VIEW rst::w { g: int4, total: int8 } AS {
			FROM rst::mid_busy APPEND { FROM rst::mid_quiet }
				| window tumbling { total: math::sum(v) }
					with { interval: "1s", grace: "0s" }
					by { g }
		}"#);
}

/// Bucket 0 is identified by the aggregate it carries, because the bucket instant itself is a private
/// system column and every total in this suite is distinct.
fn bucket_zero_total(db: &TestDb, want: i64) -> usize {
	db.await_exact_row_count(&format!("FROM rst::w FILTER {{ total == {want} }}"), 1, TIMEOUT)
}

#[test]
fn a_seal_won_through_a_view_hop_survives_a_restart() {
	// Bucket 0 can only seal here because rst::mid_quiet's frontier crossed the hop, and that seal must still hold after a reopen, otherwise the restarted consumer folds in rows the first lifetime already refused and the aggregate silently disagrees with itself across the restart.
	let path = TempDbPath::new("frontier_restart");

	{
		let mut db = open(&path);
		declare(&db);

		db.command(r#"INSERT rst::busy [{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00Z" }]"#);
		db.await_row_count("FROM rst::w FILTER { total == 5 }", 1, TIMEOUT);

		db.command(r#"INSERT rst::busy [{ id: 2, g: 1, v: 7, ts: "2026-01-01T00:00:10Z" }]"#);
		db.await_row_count("FROM rst::w FILTER { total == 7 }", 1, TIMEOUT);

		db.admin("call system::source::complete_through(rst::quiet, cast('2026-01-01T00:00:10Z', datetime))");
		settle_round(&db, 5, 2);

		db.command(r#"INSERT rst::busy [{ id: 3, g: 1, v: 100, ts: "2026-01-01T00:00:00.500Z" }]"#);
		assert!(db.await_all_flows(TIMEOUT), "the late row must be consumed before the seal is judged");
		assert_eq!(
			bucket_zero_total(&db, 5),
			1,
			"precondition: bucket 0 must be sealed before the restart, or the test proves nothing about \
			 what survives it; view now: {:?}",
			db.query("FROM rst::w")
		);

		// Operator state reaches disk only on a snapshot generation, so nothing here is durable until one lands.
		sleep(Duration::from_secs(7));
		db.stop();
	}

	let mut db = open(&path);
	assert_eq!(
		bucket_zero_total(&db, 5),
		1,
		"the view rows themselves are durable and must survive the restart untouched; view now: {:?}",
		db.query("FROM rst::w")
	);

	db.command(r#"INSERT rst::busy [{ id: 4, g: 1, v: 1000, ts: "2026-01-01T00:00:00.500Z" }]"#);
	assert!(db.await_all_flows(TIMEOUT), "the post-restart late row must be consumed before the seal is judged");
	assert_eq!(
		bucket_zero_total(&db, 5),
		1,
		"bucket 0 sealed before the restart and must stay sealed after it; a total of 1000 means the \
		 reopened window lost the seal and started the bucket over; view now: {:?}",
		db.query("FROM rst::w")
	);

	db.stop();
}
