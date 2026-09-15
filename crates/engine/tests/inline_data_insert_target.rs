// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{Value, frame::frame::Frame},
};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin("CREATE ENUM s::status { Active, Inactive }");
	t.admin("CREATE TABLE s::t { a: int4 }");
	t.admin("CREATE RINGBUFFER s::r { a: int4 } WITH { capacity: 10 }");
	t.admin("CREATE SERIES s::x { ts: datetime, val: float8 } WITH { key: ts }");
	t.admin("CREATE SERIES s::m { ts: datetime, val: int4 } WITH { key: ts, tag: s::status }");
	t
}

fn command_err(t: &TestEngine, rql: &str) -> Diagnostic {
	let r = t.inner().command_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => e.diagnostic(),
		None => panic!("expected an error, got frames {:?}\nrql: {rql}", r.frames),
	}
}

fn column_values(frames: &[Frame], name: &str) -> Vec<Value> {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let column = frames[0]
		.columns
		.iter()
		.find(|c| c.name == name)
		.unwrap_or_else(|| panic!("column {name} missing from {:?}", frames[0].columns));
	(0..column.data.len()).map(|row| column.data.get_value(row)).collect()
}

fn assert_column_not_found(err: &Diagnostic, name: &str) {
	assert_eq!(err.code, "QUERY_001", "got: {err:?}");
	assert_eq!(err.fragment.text(), name, "got: {err:?}");
}

#[test]
fn series_insert_with_an_integer_in_a_column_not_in_the_series_is_column_not_found() {
	// A value for a column the series does not have must fail loud instead of being dropped.
	let t = engine();

	let err = command_err(&t, "INSERT s::x [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1.0, extra: 5 }]");

	assert_column_not_found(&err, "extra");
	assert!(t.query("FROM s::x").iter().all(|f| f.columns.iter().all(|c| c.data.len() == 0)));
}

#[test]
fn series_insert_with_a_duration_in_a_column_not_in_the_series_is_column_not_found() {
	// A non integer value in an unknown column must be the same error, never a panic in the column buffer.
	let t = engine();

	let err = command_err(
		&t,
		"INSERT s::x [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1.0, extra: duration::hours(1) }]",
	);

	assert_column_not_found(&err, "extra");
}

#[test]
fn series_insert_with_text_in_a_column_not_in_the_series_is_column_not_found() {
	// Text in an unknown column must be the same error, never a panic in the column buffer.
	let t = engine();

	let err =
		command_err(&t, "INSERT s::x [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1.0, extra: 'abc' }]");

	assert_column_not_found(&err, "extra");
}

#[test]
fn tagged_series_insert_with_a_numeric_tag_stores_that_tag() {
	// The tag pseudo column is the one name outside the schema a tagged series must accept.
	let t = engine();

	t.command("INSERT s::m [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: 1 }]");

	assert_eq!(column_values(&t.query("FROM s::m"), "tag"), vec![Value::Uint1(1)]);
}

#[test]
fn tagged_series_insert_with_a_text_tag_is_an_error_with_its_fragment() {
	// A tag that is not a number must fail loud at the value, never panic in the column buffer.
	let t = engine();

	let err = command_err(&t, "INSERT s::m [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: 'abc' }]");

	assert_eq!(err.fragment.text(), "abc", "got: {err:?}");
}

