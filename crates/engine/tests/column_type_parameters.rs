// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::src { a: int4 }");
	t
}

#[test]
fn an_unknown_type_parameter_is_rejected() {
	// Accepting int4(5) as int4 lets a typo in a schema pass silently and look like a working limit.
	let t = engine();

	let err = t.admin_err("CREATE DEFERRED VIEW test::v { a: int4(5) } AS { FROM test::src }");

	assert!(err.contains("AST_012"), "int4(5) must report AST_012, got: {err}");
}

#[test]
fn create_table_reports_the_real_type_error() {
	// Rewriting every type error to AST_008 tells the user the type does not exist when only its parameter is
	// wrong.
	let t = engine();

	let err = t.admin_err("CREATE TABLE test::bad { a: int4(5) }");

	assert!(err.contains("AST_012"), "create table must report the same AST_012 a view reports, got: {err}");
	assert!(!err.contains("AST_008"), "create table must not claim int4 is an unknown type, got: {err}");
}

#[test]
fn an_optional_limited_utf8_keeps_its_limit() {
	// Dropping the inner limit lets Option(utf8(4)) store any length while utf8(4) rejects the same value.
	let t = engine();
	t.admin("CREATE TABLE test::names { plain: utf8(4), opt: Option(utf8(4)) }");

	let plain_err = t.command_err(r#"INSERT test::names [{ plain: "abcdefgh", opt: none }]"#);
	let opt_err = t.command_err(r#"INSERT test::names [{ plain: "ab", opt: "abcdefgh" }]"#);

	assert!(plain_err.contains("exceeds maximum byte length"), "utf8(4) must reject 8 bytes, got: {plain_err}");
	assert!(opt_err.contains("exceeds maximum byte length"), "Option(utf8(4)) must reject 8 bytes, got: {opt_err}");
	t.command(r#"INSERT test::names [{ plain: "ab", opt: none }]"#);
	t.command(r#"INSERT test::names [{ plain: "ab", opt: "abcd" }]"#);
}

#[test]
fn an_optional_type_with_an_unknown_parameter_is_rejected() {
	// Option must not hide a bad parameter that the bare type reports.
	let t = engine();

	let err = t.admin_err("CREATE TABLE test::bad { a: Option(int4(5)) }");

	assert!(err.contains("AST_012"), "Option(int4(5)) must report AST_012, got: {err}");
}

#[test]
fn an_optional_limited_decimal_keeps_its_scale() {
	// Dropping the inner scale lets Option(decimal(10,2)) store digits that decimal(10,2) rejects.
	let t = engine();
	t.admin("CREATE TABLE test::prices { plain: decimal(10,2), opt: Option(decimal(10,2)) }");

	let plain_err = t.command_err("INSERT test::prices [{ plain: 1.234, opt: none }]");
	let opt_err = t.command_err("INSERT test::prices [{ plain: 1.23, opt: 1.234 }]");

	assert!(plain_err.contains("exceeds maximum scale"), "decimal(10,2) must reject 3 decimals, got: {plain_err}");
	assert!(opt_err.contains("exceeds maximum scale"), "Option(decimal(10,2)) must reject 3 decimals, got: {opt_err}");
	t.command("INSERT test::prices [{ plain: 1.23, opt: none }]");
	t.command("INSERT test::prices [{ plain: 1.23, opt: 1.23 }]");
}
