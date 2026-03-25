// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Error condition tests for the bulk_insert module.
//!
//! Tests cover all error types: namespace not found, table not found,
//! ringbuffer not found, column not found, too many values, and coercion failures.

use reifydb_engine::test_prelude::*;

#[test]
fn test_error_namespace_not_found() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	// Try to insert into a table in a non-existent namespace
	let mut builder = t.bulk_insert(identity);
	builder.table("nonexistent::mytable").row(params! { id: 1 }).done();
	let result = builder.execute();

	assert!(result.is_err());
	let err = result.unwrap_err();
	let msg = format!("{}", err);
	assert!(msg.contains("namespace") || msg.contains("not found"), "Expected namespace error, got: {}", msg);
}

#[test]
fn test_error_table_not_found() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");

	// Try to insert into a non-existent table
	let mut builder = t.bulk_insert(identity);
	builder.table("test::nonexistent").row(params! { id: 1 }).done();
	let result = builder.execute();

	assert!(result.is_err());
	let err = result.unwrap_err();
	let msg = format!("{}", err);
	assert!(msg.contains("table") || msg.contains("not found"), "Expected table error, got: {}", msg);
}

#[test]
fn test_error_ringbuffer_not_found() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");

	// Try to insert into a non-existent ringbuffer
	let mut builder = t.bulk_insert(identity);
	builder.ringbuffer("test::nonexistent").row(params! { id: 1 }).done();
	let result = builder.execute();

	assert!(result.is_err());
	let err = result.unwrap_err();
	let msg = format!("{}", err);
	assert!(
		msg.contains("ring") || msg.contains("buffer") || msg.contains("not found"),
		"Expected ringbuffer error, got: {}",
		msg
	);
}

#[test]
fn test_error_column_not_found() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::users { id: int4, name: utf8 }");

	// Try to insert with an unknown column name
	let mut builder = t.bulk_insert(identity);
	builder.table("test::users").row(params! { id: 1, name: "Alice", unknown_column: "value" }).done();
	let result = builder.execute();

	assert!(result.is_err());
	let err = result.unwrap_err();
	let msg = format!("{}", err);
	assert!(msg.contains("column") || msg.contains("not found"), "Expected column error, got: {}", msg);
}

#[test]
fn test_error_too_many_values() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::small { a: int4, b: int4 }");

	// Try to insert with more positional values than columns
	let mut builder = t.bulk_insert(identity);
	builder.table("test::small").row(params![1, 2, 3, 4, 5]).done(); // 5 values for 2 columns
	let result = builder.execute();

	assert!(result.is_err());
	let err = result.unwrap_err();
	let msg = format!("{}", err);
	assert!(
		msg.contains("too many") || msg.contains("values") || msg.contains("column"),
		"Expected too many values error, got: {}",
		msg
	);
}

#[test]
fn test_error_coercion_failure() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::typed { num: int4 }");

	// Try to insert a string that cannot be coerced to int4
	let mut builder = t.bulk_insert(identity);
	builder.table("test::typed").row(params! { num: "not_a_number" }).done();
	let result = builder.execute();

	assert!(result.is_err());
	let err = result.unwrap_err();
	let msg = format!("{}", err);
	// The error should indicate a type/coercion issue
	assert!(
		msg.contains("type") || msg.contains("coerce") || msg.contains("convert") || msg.contains("cast"),
		"Expected coercion error, got: {}",
		msg
	);
}

#[test]
fn test_error_ringbuffer_namespace_not_found() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	// Try to insert into a ringbuffer in a non-existent namespace
	let mut builder = t.bulk_insert(identity);
	builder.ringbuffer("nonexistent::events").row(params! { id: 1 }).done();
	let result = builder.execute();

	assert!(result.is_err());
	let err = result.unwrap_err();
	let msg = format!("{}", err);
	assert!(msg.contains("namespace") || msg.contains("not found"), "Expected namespace error, got: {}", msg);
}
