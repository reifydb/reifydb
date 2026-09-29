// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::execution::ExecutionResult;
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{error::Diagnostic, params::Params, value::identity::IdentityId};

const RESERVED: &str = "QUERY_004";

fn seeded() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, v: int4 }");
	t.command("INSERT test::t [{ id: 1, v: 10 }, { id: 2, v: 20 }]");
	t
}

fn diagnostic(result: ExecutionResult, rql: &str) -> Diagnostic {
	result.error
		.unwrap_or_else(|| panic!("`{rql}` names a user column starting with #, so it must fail"))
		.diagnostic()
}

#[track_caller]
fn assert_reserved(result: ExecutionResult, rql: &str, name: &str) {
	// Without the exact message a different QUERY_004 (the read-only inline key) would pass here.
	let diagnostic = diagnostic(result, rql);
	assert_eq!(diagnostic.code, RESERVED, "`{rql}` must fail with the reserved-name code, got {diagnostic:?}");
	assert_eq!(
		diagnostic.message,
		format!("column name '{name}' is reserved for system columns"),
		"`{rql}` must name the reserved column, got {diagnostic:?}"
	);
}

#[track_caller]
fn admin_reserved(t: &TestEngine, rql: &str, name: &str) {
	assert_reserved(t.inner().admin_as(IdentityId::system(), rql, Params::None), rql, name);
}

#[track_caller]
fn command_reserved(t: &TestEngine, rql: &str, name: &str) {
	assert_reserved(t.inner().command_as(IdentityId::system(), rql, Params::None), rql, name);
}

#[track_caller]
fn query_reserved(t: &TestEngine, rql: &str, name: &str) {
	assert_reserved(t.inner().query_as(IdentityId::system(), rql, Params::None), rql, name);
}

#[test]
fn create_table_rejects_a_hash_column() {
	// A table column named #x would shadow or fake a system column on every scan of the table.
	let t = seeded();
	admin_reserved(&t, "CREATE TABLE test::a { id: int4, `#x`: int4 }", "#x");
	admin_reserved(&t, "CREATE TABLE test::b { id: int4, #rownum: int4 }", "#rownum");
}

#[test]
fn create_ringbuffer_rejects_a_hash_column() {
	// A ringbuffer column named #x would be read back as a system column.
	let t = seeded();
	admin_reserved(&t, "CREATE RINGBUFFER test::r { id: int4, `#x`: int4 } WITH { capacity: 10 }", "#x");
}

#[test]
fn create_series_rejects_a_hash_column() {
	// A series column named #x would be read back as a system column.
	let t = seeded();
	admin_reserved(&t, "CREATE SERIES test::s { ts: int8, `#x`: int8 } WITH { key: ts }", "#x");
}

#[test]
fn create_deferred_view_rejects_a_hash_column() {
	// A deferred view column named #x would be read back as a system column.
	let t = seeded();
	admin_reserved(&t, "CREATE DEFERRED VIEW test::v { id: int4, `#x`: int4 } AS { FROM test::t }", "#x");
}

#[test]
fn create_transactional_view_rejects_a_hash_column() {
	// A transactional view column named #x would be read back as a system column.
	let t = seeded();
	admin_reserved(&t, "CREATE TRANSACTIONAL VIEW test::v { id: int4, `#x`: int4 } AS { FROM test::t }", "#x");
}

#[test]
fn create_queue_rejects_a_hash_column() {
	// A queue column named #x would be read back as a system column.
	let t = seeded();
	admin_reserved(&t, "CREATE QUEUE test::q { id: int4, `#x`: int4 } WITH { fifo: {} }", "#x");
}

#[test]
fn alter_table_add_column_rejects_a_hash_column() {
	// Adding the column later must not bypass the check create table makes.
	let t = seeded();
	admin_reserved(&t, "ALTER TABLE test::t ADD COLUMN `#x`: int4", "#x");
	admin_reserved(&t, "ALTER TABLE test::t ADD COLUMN #rownum: int4", "#rownum");
}

#[test]
fn alter_table_rename_column_rejects_a_hash_name() {
	// Renaming an existing column must not bypass the check create table makes.
	let t = seeded();
	admin_reserved(&t, "ALTER TABLE test::t RENAME COLUMN v TO `#x`", "#x");
}

#[test]
fn map_rejects_a_hash_alias() {
	// A map alias named #rownum would stand in for the real row number downstream.
	let t = seeded();
	query_reserved(&t, "FROM test::t | map { `#x`: v }", "#x");
	query_reserved(&t, "FROM test::t | map { \"#x\": v }", "#x");
	query_reserved(&t, "FROM test::t | map { `#rownum`: v }", "#rownum");
}

#[test]
fn extend_rejects_a_hash_alias() {
	// An extended column named #x would be read back as a system column.
	let t = seeded();
	query_reserved(&t, "FROM test::t | extend { `#x`: v }", "#x");
	query_reserved(&t, "FROM test::t | extend { `#rownum`: v }", "#rownum");
}

#[test]
fn aggregate_rejects_a_hash_alias() {
	// An aggregate output named #x would be read back as a system column.
	let t = seeded();
	query_reserved(&t, "FROM test::t | aggregate { `#x`: math::sum(v) } by { id }", "#x");
}

#[test]
fn patch_rejects_a_hash_alias() {
	// Patch and update would overwrite or fake a system column under a user name.
	let t = seeded();
	query_reserved(&t, "FROM test::t | patch { `#x`: v }", "#x");
	command_reserved(&t, "UPDATE test::t { `#x`: 1 } FILTER { id == 1 }", "#x");
}

#[test]
fn window_rejects_a_hash_alias() {
	// A window aggregation named #x would be read back as a system column.
	let t = seeded();
	query_reserved(
		&t,
		"FROM test::t | window rolling { `#x`: math::sum(v) } with { duration: 1h } by { id }",
		"#x",
	);
	query_reserved(
		&t,
		"FROM test::t | window rolling { total: math::sum(v) } with { duration: 1h } by { `#x`: id }",
		"#x",
	);
}

#[test]
fn returning_rejects_a_hash_alias() {
	// A returned column named #x would reach the client as a system column.
	let t = seeded();
	command_reserved(&t, "INSERT test::t [{ id: 3, v: 30 }] RETURNING { `#x`: v }", "#x");
	command_reserved(&t, "DELETE test::t FILTER { id == 1 } RETURNING { `#x`: v }", "#x");
}

#[test]
fn append_inline_rejects_a_hash_key() {
	// An appended row keyed #x would put a fake system column into the variable.
	let t = seeded();
	command_reserved(&t, "append $x from [{ `#x`: 1 }]; from $x", "#x");
}

#[test]
fn inline_from_rejects_a_hash_key_as_read_only() {
	// The inline key keeps its read-only diagnostic so the INSERT goldens do not move.
	let t = seeded();
	let rql = "FROM [{ `#x`: 1 }]";
	let diagnostic = diagnostic(t.inner().query_as(IdentityId::system(), rql, Params::None), rql);
	assert_eq!(diagnostic.code, RESERVED, "`{rql}` must fail with QUERY_004, got {diagnostic:?}");
	assert_eq!(diagnostic.message, "system column '#x' is read-only", "got {diagnostic:?}");
}

#[test]
fn reading_system_columns_stays_legal() {
	// A check keyed on labels instead of aliases would reject plain reads of # columns.
	let t = seeded();
	assert_eq!(TestEngine::row_count(&t.query("FROM test::t | map { id, #rownum }")), 2);
	assert_eq!(TestEngine::row_count(&t.query("FROM test::t | map { r: #rownum }")), 2);
	assert_eq!(TestEngine::row_count(&t.query("FROM test::t | filter { #rownum > 0 } | extend { w: v }")), 2);
}
