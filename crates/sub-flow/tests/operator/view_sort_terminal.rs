// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// `sort` is only valid as the terminal (outermost) operator of a view pipeline. A sort whose output
// is consumed by a downstream operator cannot be maintained incrementally, so the flow compiler must
// reject it at CREATE VIEW time with FLOW_012. This applies to both deferred and transactional views.
// A terminal sort (nothing consumes its output) is allowed.

use reifydb::{Database, Params, WithSubsystem, embedded};

fn setup() -> Database {
	let db = embedded::memory().with_flow(|c| c).build().expect("build memory db with flow");
	db.admin_as_root("CREATE NAMESPACE v", Params::None).expect("create namespace");
	db.admin_as_root("CREATE TABLE v::base { id: int4, qty: int4 }", Params::None).expect("create table");
	db
}

fn create_view_error(db: &Database, rql: &str) -> reifydb_value::error::Diagnostic {
	db.admin_as_root(rql, Params::None).expect_err("expected CREATE VIEW to be rejected").diagnostic()
}

fn create_view_ok(db: &Database, rql: &str) {
	db.admin_as_root(rql, Params::None).unwrap_or_else(|e| panic!("expected CREATE VIEW to succeed: {e:?}\n{rql}"));
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
fn non_terminal_sort_in_transactional_view_rejected() {
	let db = setup();
	let diag = create_view_error(
		&db,
		"CREATE TRANSACTIONAL VIEW v::t { id: int4, qty: int4 } AS { FROM v::base SORT { qty } MAP { id, qty } }",
	);
	assert_eq!(diag.code, "FLOW_012", "expected FLOW_012, got {:?}: {}", diag.code, diag.message);
}

#[test]
fn terminal_sort_in_deferred_view_succeeds() {
	let db = setup();
	create_view_ok(&db, "CREATE DEFERRED VIEW v::d { id: int4, qty: int4 } AS { FROM v::base SORT { qty } }");
}

#[test]
fn terminal_sort_in_transactional_view_succeeds() {
	let db = setup();
	create_view_ok(&db, "CREATE TRANSACTIONAL VIEW v::t { id: int4, qty: int4 } AS { FROM v::base SORT { qty } }");
}
