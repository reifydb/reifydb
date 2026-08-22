// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TestDb;

fn plain() -> TestDb {
	let db = TestDb::memory();
	db.admin("create namespace test");
	db.admin("create table test::t { id: int4, status: utf8 }");
	db
}

fn colliding() -> TestDb {
	let db = TestDb::memory();
	db.admin("create namespace test");
	db.admin("create table test::t { id: int4, status: utf8, pre_status: utf8 }");
	db
}

#[test]
fn test_delete_returning_post_image() {
	// A delete reports the row it removed, and the row must really be gone.
	let db = plain();
	db.command(r#"insert test::t [{ id: 1, status: "old" }]"#);

	let frames = db.command(r#"delete test::t filter { id == 1 } returning { id, status }"#);
	let f = &frames[0];

	assert_eq!(f.row_count(), 1);
	assert_eq!(f.get::<String>("status", 0).unwrap().unwrap(), "old");

	let frames = db.query("from test::t");
	assert_eq!(frames[0].row_count(), 0);
}

#[test]
fn test_delete_returning_pre_image_equals_post_image() {
	// A delete writes nothing, so the pre-image and the returned row are the same
	// value. pre_ must still resolve rather than silently yielding none.
	let db = plain();
	db.command(r#"insert test::t [{ id: 1, status: "old" }]"#);

	let frames = db.command(r#"delete test::t filter { id == 1 } returning { pre_status, status }"#);
	let f = &frames[0];

	assert_eq!(f.row_count(), 1);
	assert_eq!(f.get::<String>("pre_status", 0).unwrap().unwrap(), "old");
	assert_eq!(f.get::<String>("status", 0).unwrap().unwrap(), "old");
}

#[test]
fn test_delete_returning_pre_prefixed_column_is_the_stored_column() {
	// The stored column wins even where the pre-image would give the same answer,
	// so the rule cannot be satisfied by accident.
	let db = colliding();
	db.command(r#"insert test::t [{ id: 1, status: "old", pre_status: "a" }]"#);

	let frames = db.command(r#"delete test::t filter { id == 1 } returning { pre_status }"#);
	let f = &frames[0];

	assert_eq!(f.row_count(), 1);
	assert_eq!(f.get::<String>("pre_status", 0).unwrap().unwrap(), "a");
}
