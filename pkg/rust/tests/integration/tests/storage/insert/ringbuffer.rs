// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TestDb;

fn plain() -> TestDb {
	let db = TestDb::memory();
	db.admin("create namespace test");
	db.admin("create ringbuffer test::rb { id: int4, status: utf8 } with { capacity: 10 }");
	db
}

fn colliding() -> TestDb {
	let db = TestDb::memory();
	db.admin("create namespace test");
	db.admin("create ringbuffer test::rb { id: int4, status: utf8, pre_status: utf8 } with { capacity: 10 }");
	db
}

#[test]
fn test_insert_returning_post_image() {
	// The post-image of an insert is the row now stored.
	let db = plain();

	let frames = db.command(r#"insert test::rb [{ id: 1, status: "new" }] returning { id, status }"#);
	let f = &frames[0];

	assert_eq!(f.row_count(), 1);
	assert_eq!(f.get::<String>("status", 0).unwrap().unwrap(), "new");
}

#[test]
fn test_insert_returning_pre_image_is_none() {
	// An insert has no pre-image, so every pre_ column is none rather than an error.
	// Erroring would make the clause's validity depend on the statement kind.
	let db = plain();

	let frames = db.command(r#"insert test::rb [{ id: 1, status: "new" }] returning { id, pre_status }"#);
	let f = &frames[0];

	assert_eq!(f.row_count(), 1);
	assert!(f.get::<String>("pre_status", 0).unwrap().is_none());
}

#[test]
fn test_insert_returning_pre_prefixed_column_is_the_stored_column() {
	// A column really named pre_status must win over the pre-image of status.
	// Silently handing back the pre-image would return wrong data with no error.
	let db = colliding();

	let frames = db
		.command(r#"insert test::rb [{ id: 1, status: "new", pre_status: "b" }] returning { id, pre_status }"#);
	let f = &frames[0];

	assert_eq!(f.row_count(), 1);
	assert_eq!(f.get::<String>("pre_status", 0).unwrap().unwrap(), "b");
}
