// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// A sort whose output is consumed by a downstream operator cannot be maintained incrementally, so
// it is refused at CREATE VIEW time; a terminal sort, with nothing consuming it, is allowed.

use reifydb::testing::db::TestDb;
use reifydb::{WithSubsystem, embedded};

fn setup() -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|c| c).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE v");
	db.admin("CREATE TABLE v::base { id: int4, qty: int4 }");
	db
}

fn create_view_error(db: &TestDb, rql: &str) -> reifydb_value::error::Diagnostic {
	db.try_admin(rql).expect_err("expected CREATE VIEW to be rejected").diagnostic()
}

fn create_view_ok(db: &TestDb, rql: &str) {
	db.try_admin(rql).unwrap_or_else(|e| panic!("expected CREATE VIEW to succeed: {e:?}\n{rql}"));
}

#[test]
fn non_terminal_sort_in_deferred_view_rejected() {
	let db = setup();
	let diag = create_view_error(
		&db,
		"CREATE DEFERRED VIEW v::d { id: int4, qty: int4 } AS { FROM v::base SORT { qty } MAP { id, qty } }",
	);
	assert_eq!(diag.code, "FLOW_012", "expected FLOW_012, got {:?}: {}", diag.code, diag.message);
}

#[test]
fn terminal_sort_in_deferred_view_succeeds() {
	let db = setup();
	create_view_ok(&db, "CREATE DEFERRED VIEW v::d { id: int4, qty: int4 } AS { FROM v::base SORT { qty } }");
}
