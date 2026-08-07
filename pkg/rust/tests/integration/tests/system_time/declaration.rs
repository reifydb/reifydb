// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{RuntimeConfig, embedded as db_embedded};
use reifydb_test_harness::db::TestDb;

fn db() -> TestDb {
	let db = TestDb::from(
		db_embedded::memory().with_runtime_config(RuntimeConfig::default().seeded(0)).build().expect("build"),
	);
	db.admin("CREATE NAMESPACE st");
	db
}

#[track_caller]
fn admin_err(db: &TestDb, rql: &str) -> String {
	match db.try_admin(rql) {
		Ok(_) => panic!("{rql}: expected rejection, but it succeeded"),
		Err(err) => format!("{:?}", err),
	}
}

#[test]
fn a_populator_naming_no_column_is_rejected_at_definition_time() {
	let db = db();
	let err = admin_err(&db, "CREATE TABLE st::t { id: int4 } with { time: event(nope) }");
	assert!(err.contains("TIME_003"), "expected TIME_003, got: {err}");
	assert!(err.contains("nope"), "diagnostic must name the missing column: {err}");
}

#[test]
fn a_populator_that_is_not_a_datetime_is_rejected_at_definition_time() {
	let db = db();
	let err = admin_err(&db, "CREATE TABLE st::t { id: int4 } with { time: event(id) }");
	assert!(err.contains("TIME_004"), "expected TIME_004, got: {err}");
}

#[test]
fn a_datetime_populator_is_accepted() {
	let db = db();
	db.admin("CREATE TABLE st::t { id: int4, at: datetime } with { time: event(at) }");
	db.command(r#"INSERT st::t [{ id: 1, at: "2026-01-01T00:00:01Z" }]"#);
	let frames = db.query("FROM st::t | MAP { #time, at }");
	let frame = frames.first().expect("no frame");
	let time = frame.columns.iter().find(|c| c.name == "time").expect("no #time");
	let at = frame.columns.iter().find(|c| c.name == "at").expect("no at");
	assert_eq!(time.data.get_value(0), at.data.get_value(0));
}

#[test]
fn every_source_object_kind_validates_its_populator() {
	let db = db();
	for rql in [
		"CREATE TABLE st::a { id: int4 } with { time: event(nope) }",
		"CREATE SERIES st::b { k: int8, id: int4 } with { key: k, time: event(nope) }",
		"CREATE RINGBUFFER st::c { id: int4 } with { capacity: 8, time: event(nope) }",
		"CREATE QUEUE st::d { id: int4 } with { fifo: {}, time: event(nope) }",
	] {
		let err = admin_err(&db, rql);
		assert!(err.contains("TIME_003"), "{rql}: expected TIME_003, got: {err}");
	}
}
