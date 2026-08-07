// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{RuntimeConfig, embedded as db_embedded};
use reifydb_test_harness::db::TestDb;
use reifydb_value::value::Value;

fn db() -> TestDb {
	let db = TestDb::from(
		db_embedded::memory().with_runtime_config(RuntimeConfig::default().seeded(0)).build().expect("build"),
	);
	db.admin("CREATE NAMESPACE st");
	db
}

#[track_caller]
fn single(db: &TestDb, rql: &str, column: &str) -> Value {
	let frames = db.query(rql);
	let frame = frames.first().unwrap_or_else(|| panic!("{rql}: no frame"));
	let col = frame.columns.iter().find(|c| c.name == column).unwrap_or_else(|| panic!("{rql}: no `{column}`"));
	assert_eq!(col.data.len(), 1, "{rql}: expected exactly one row");
	col.data.get_value(0)
}

#[test]
fn an_update_on_a_processing_time_object_carries_time_forward() {
	let db = db();
	db.admin("CREATE TABLE st::t { id: int4, n: int4 }");
	db.command("INSERT st::t [{ id: 1, n: 10 }]");

	let before = single(&db, "FROM st::t | MAP { #time }", "time");
	let created_before = single(&db, "FROM st::t | MAP { #created_at }", "created_at");

	db.command("UPDATE st::t { n: 20 } FILTER { id == 1 }");

	assert_eq!(single(&db, "FROM st::t | MAP { n }", "n"), Value::Int4(20), "the update must have applied");
	assert_eq!(
		single(&db, "FROM st::t | MAP { #time }", "time"),
		before,
		"#time records when the event arrived; rewriting a row in place is not a new arrival"
	);
	assert_eq!(
		single(&db, "FROM st::t | MAP { #created_at }", "created_at"),
		created_before,
		"created_at must still be preserved across the update"
	);
}

#[test]
fn an_update_that_moves_the_populator_moves_time_with_it() {
	let db = db();
	db.admin("CREATE TABLE st::e { id: int4, at: datetime } with { time: event(at) }");
	db.command(r#"INSERT st::e [{ id: 1, at: "2026-01-01T00:00:01Z" }]"#);

	db.command(r#"UPDATE st::e { at: "2026-01-01T00:00:09Z" } FILTER { id == 1 }"#);

	let at = single(&db, "FROM st::e | MAP { at }", "at");
	assert_eq!(
		single(&db, "FROM st::e | MAP { #time }", "time"),
		at,
		"an event-time object re-reads its populator, so correcting the column corrects #time"
	);
}

#[test]
fn an_update_that_leaves_the_populator_alone_leaves_time_alone() {
	let db = db();
	db.admin("CREATE TABLE st::e { id: int4, at: datetime, n: int4 } with { time: event(at) }");
	db.command(r#"INSERT st::e [{ id: 1, at: "2026-01-01T00:00:01Z", n: 10 }]"#);

	let before = single(&db, "FROM st::e | MAP { #time }", "time");
	db.command("UPDATE st::e { n: 20 } FILTER { id == 1 }");

	assert_eq!(single(&db, "FROM st::e | MAP { #time }", "time"), before);
}
