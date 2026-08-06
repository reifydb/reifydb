// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Operator state across process lifetimes, through the real supervisor bootstrap and a real
//! `operator.db`. Everything below the actor is covered in-crate against a StandardEngine; what
//! only a full database can show is that the snapshot file survives a close, that the supervisor
//! actually loads it, and that a flow resumed from it keeps computing from where it left off.
//!
//! A memory build silently disables snapshots (builder/database.rs), so this suite is sqlite-only
//! on purpose: a memory version of it would pass without ever writing a generation.

use std::{
	thread::sleep,
	time::{Duration, Instant},
};

use reifydb::{Frame, SqliteConfig, Value, WithSubsystem, core::interface::catalog::config::ConfigKey, embedded};
use reifydb_test_harness::{
	assert::column_values,
	db::{TempDbPath, TestDb},
};

fn open(path: &TempDbPath) -> TestDb {
	TestDb::from(
		embedded::sqlite(SqliteConfig::new(path))
			// Snapshots are off by default at this cadence; one second is what makes a
			// generation land inside a test's lifetime at all.
			.with_config(ConfigKey::OperatorSnapshotInterval, Value::duration_seconds(1))
			.with_flow(|f| f)
			.build()
			.expect("build a sqlite database with the flow subsystem"),
	)
}

fn insert(db: &TestDb, id: u32, group: u32) {
	db.command(&format!(r#"insert app::t [{{ id: {id}, g: {group}, ts: "1970-01-01T00:{id:02}:00Z" }}]"#));
}

fn total_for(frames: &[Frame], group: i32) -> Option<i64> {
	let frame = frames.first()?;
	let groups = column_values(frame, "g");
	let totals = column_values(frame, "total");
	groups.iter().position(|value| *value == Value::Int4(group)).map(|row| match totals[row] {
		Value::Int8(total) => total,
		ref other => panic!("total must be int8, got {other:?}"),
	})
}

fn await_total(db: &TestDb, group: i32, want: i64, timeout: Duration) -> Option<i64> {
	let deadline = Instant::now() + timeout;
	loop {
		let got = total_for(&db.query("from app::v"), group);
		if got == Some(want) || Instant::now() >= deadline {
			return got;
		}
		sleep(Duration::from_millis(20));
	}
}

#[test]
fn operator_state_survives_a_real_restart() {
	// An aggregate's running counters live only in the arena, which is RAM. If a reopened
	// database boots that arena empty, the flow keeps serving the view rows it already wrote
	// while recomputing totals from zero, so the very next row makes the view disagree with the
	// table and nothing ever errors. This pins the whole durability chain end to end: a
	// generation reaches operator.db, the supervisor loads it at bootstrap, and the resumed flow
	// counts on from the state it restored. Falsified by making load_flow a no-op (the reopened
	// flow reports 1 for a group that already had five rows), and by booting the arena without
	// the catch-up replay whenever the crash window holds unsnapshotted rows.
	let path = TempDbPath::new("operator_snapshot_restart");

	{
		let mut db = open(&path);
		db.admin("create namespace app");
		db.admin("create table app::t { id: int4, g: int4, ts: datetime } with { ts: ts }");
		db.admin("create deferred view app::v { g: int4, total: int8 } \
			 as { from app::t aggregate { total: math::count(id) } by { g } }");

		for id in 1..=5 {
			insert(&db, id, 1);
		}
		assert_eq!(
			await_total(&db, 1, 5, Duration::from_secs(10)),
			Some(5),
			"precondition: the aggregate must materialize all five rows before the snapshot"
		);

		// The snapshot interval is one second and the flow tick fires at the same cadence, so a
		// generation lands within this settle.
		sleep(Duration::from_secs(3));

		// Written after the settle, these are the rows most likely to sit in the gap between the
		// newest generation and the checkpoint - the window catch-up exists to close.
		for id in 6..=8 {
			insert(&db, id, 1);
		}
		assert_eq!(
			await_total(&db, 1, 8, Duration::from_secs(10)),
			Some(8),
			"precondition: the gap rows must be consumed, so the checkpoint moves past the snapshot"
		);

		db.stop();
	}

	let mut db = open(&path);
	assert_eq!(
		total_for(&db.query("from app::v"), 1),
		Some(8),
		"the view rows themselves are durable and must survive untouched"
	);

	insert(&db, 9, 1);
	assert_eq!(
		await_total(&db, 1, 9, Duration::from_secs(15)),
		Some(9),
		"the reopened flow must count on from the state it restored: a total of 1 means the arena \
		 booted empty, and anything between 2 and 8 means it resumed from a snapshot whose gap was \
		 never replayed"
	);

	// A second group proves the restored state is addressed correctly rather than a single
	// counter that happens to be right.
	insert(&db, 10, 2);
	assert_eq!(
		await_total(&db, 2, 1, Duration::from_secs(15)),
		Some(1),
		"a group that did not exist before the restart must start at one"
	);
	assert_eq!(
		total_for(&db.query("from app::v"), 1),
		Some(9),
		"the first group's total must not move when an unrelated group is written"
	);

	db.stop();
}
