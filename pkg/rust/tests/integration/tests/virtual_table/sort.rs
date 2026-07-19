// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{RuntimeConfig, embedded};
use reifydb_test_harness::{assert::FrameAssert, db::TestDb};

fn new_db() -> TestDb {
	TestDb::from(embedded::memory().with_runtime_config(RuntimeConfig::default().seeded(0)).build().expect("build"))
}

#[test]
fn test_sort_system_namespaces() {
	let db = new_db();

	db.admin("CREATE NAMESPACE zoo");
	db.admin("CREATE NAMESPACE alpha");
	db.admin("CREATE NAMESPACE beta");

	// no direction means descending by default
	db.query("FROM system::namespaces SORT {name}").assert().column_descending("name");
}

#[test]
fn test_sort_system_namespaces_asc() {
	let db = new_db();

	db.admin("CREATE NAMESPACE zoo");
	db.admin("CREATE NAMESPACE alpha");
	db.admin("CREATE NAMESPACE beta");

	db.query("FROM system::namespaces SORT {name:ASC}").assert().column_ascending("name");
}

#[test]
fn test_sort_system_tables() {
	let db = new_db();

	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::zebra { id: int4 }");
	db.admin("CREATE TABLE test::apple { id: int4 }");
	db.admin("CREATE TABLE test::banana { id: int4 }");

	db.query("FROM system::tables SORT {name:ASC}").assert().column_ascending("name");
}

#[test]
fn test_sort_system_tables_with_pipe_syntax() {
	let db = new_db();

	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::zebra { id: int4 }");
	db.admin("CREATE TABLE test::apple { id: int4 }");
	db.admin("CREATE TABLE test::banana { id: int4 }");

	// no direction means descending by default
	db.query("from system::tables | sort {name}").assert().column_descending("name");
}
