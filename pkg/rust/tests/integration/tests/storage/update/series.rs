// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TestDb;

fn plain() -> TestDb {
	let db = TestDb::memory();
	db.admin("create namespace test");
	db.admin("create series test::s { ts: int8, status: utf8 } with { key: ts }");
	db
}

fn colliding() -> TestDb {
	let db = TestDb::memory();
	db.admin("create namespace test");
	db.admin("create series test::s { ts: int8, status: utf8, pre_status: utf8 } with { key: ts }");
	db
}

#[test]
fn test_update_returning_post_image() {
	// The post-image is the value the update wrote, not the value it replaced.
	let db = plain();
	db.command(r#"insert test::s [{ ts: 1000, status: "old" }]"#);

	let frames = db.command(r#"update test::s { status: "new" } filter { ts == 1000 } returning { ts, status }"#);
	let f = &frames[0];

	assert_eq!(f.row_count(), 1);
	assert_eq!(f.get::<String>("status", 0).unwrap().unwrap(), "new");
}

#[test]
fn test_update_returning_pre_image() {
	// The whole point of pre_: the caller learns what the row held before its own
	// write without a second read, so check-then-write stays one statement.
	let db = plain();
	db.command(r#"insert test::s [{ ts: 1000, status: "old" }]"#);

	let frames = db
		.command(r#"update test::s { status: "new" } filter { ts == 1000 } returning { pre_status, status }"#);
	let f = &frames[0];

	assert_eq!(f.row_count(), 1);
	assert_eq!(f.get::<String>("pre_status", 0).unwrap().unwrap(), "old");
	assert_eq!(f.get::<String>("status", 0).unwrap().unwrap(), "new");
}

#[test]
fn test_update_returning_pre_prefixed_column_is_the_stored_column() {
	// Three answers are possible here and only one is right: the stored column's
	// post value "b", not its pre value "a", and not the pre-image of status "old".
	let db = colliding();
	db.command(r#"insert test::s [{ ts: 1000, status: "old", pre_status: "a" }]"#);

	let frames = db.command(
		r#"update test::s { status: "new", pre_status: "b" } filter { ts == 1000 } returning { pre_status }"#,
	);
	let f = &frames[0];

	assert_eq!(f.row_count(), 1);
	assert_eq!(f.get::<String>("pre_status", 0).unwrap().unwrap(), "b");
}
