// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{RuntimeConfig, embedded as db_embedded, testing::db::TestDb};
use reifydb_value::value::Value;

fn seeded_db() -> TestDb {
	let db = TestDb::from(
		db_embedded::memory().with_runtime_config(RuntimeConfig::default().seeded(0)).build().expect("build"),
	);
	db.admin("CREATE NAMESPACE st");
	db.admin("CREATE TABLE st::t { id: int4, at: datetime } with { time: event(at) }");
	db.command(
		r#"INSERT st::t [
			{ id: 5, at: "2026-01-01T00:00:05Z" },
			{ id: 3, at: "2026-01-01T00:00:03Z" },
			{ id: 1, at: "2026-01-01T00:00:01Z" },
			{ id: 4, at: "2026-01-01T00:00:04Z" },
			{ id: 2, at: "2026-01-01T00:00:02Z" }
		]"#,
	);
	db
}

#[test]
fn a_projected_time_column_carries_the_declared_populator() {
	let db = seeded_db();
	let frames = db.query("FROM st::t | MAP { id, #time, at }");
	let frame = frames.first().expect("no frame");

	let time = frame.columns.iter().find(|c| c.name == "time").expect("#time did not project as a column");
	let at = frame.columns.iter().find(|c| c.name == "at").expect("no `at` column");

	assert_eq!(time.data.len(), 5);
	for i in 0..5 {
		assert_eq!(
			time.data.get_value(i),
			at.data.get_value(i),
			"#time[{i}] must equal the declared ts column"
		);
	}
}

#[test]
fn time_is_sortable() {
	let db = seeded_db();
	let frames = db.query("FROM st::t | SORT { #time: DESC } | MAP { id }");
	let frame = frames.first().expect("no frame");
	let id = frame.columns.iter().find(|c| c.name == "id").expect("no `id` column");

	let ids: Vec<Value> = (0..id.data.len()).map(|i| id.data.get_value(i)).collect();
	assert_eq!(
		ids,
		vec![Value::Int4(5), Value::Int4(4), Value::Int4(3), Value::Int4(2), Value::Int4(1)],
		"SORT {{ #time: DESC }} must order by the event stamp"
	);
}

#[test]
fn the_other_system_columns_remain_reachable() {
	let db = seeded_db();
	let frames = db.query("FROM st::t | MAP { #rownum, #created_at, #updated_at, #time }");
	let frame = frames.first().expect("no frame");

	for name in ["rownum", "created_at", "updated_at", "time"] {
		let col = frame.columns.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("{name} missing"));
		assert_eq!(col.data.len(), 5, "{name} must carry one entry per row");
	}
}
