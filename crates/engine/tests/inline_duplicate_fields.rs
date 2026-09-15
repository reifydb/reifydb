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
	t.admin("CREATE ENUM s::shape { Circle { radius: float8 }, Square { side: float8 } }");
	t.admin("CREATE TABLE s::t { a: int8, b: int4 }");
	t.admin("CREATE RINGBUFFER s::r { a: int8, b: int4 } WITH { capacity: 10 }");
	t.admin("CREATE QUEUE s::q { a: int8, b: int4 } WITH { fifo: {} }");
	t.admin("CREATE SERIES s::x { a: int8, b: int4 } WITH { key: a }");
	t.admin("CREATE SERIES s::m { ts: datetime, val: int4 } WITH { key: ts, tag: s::status }");
	t.admin("CREATE DICTIONARY s::d FOR utf8 AS uint4");
	t.admin("CREATE TABLE s::g { id: int4, shape: s::shape }");
	t
}

fn command(t: &TestEngine, rql: &str) -> Result<Vec<Frame>, Diagnostic> {
	let r = t.inner().command_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => Err(e.diagnostic()),
		None => Ok(r.frames),
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

fn assert_duplicate_field(t: &TestEngine, rql: &str, field: &str) {
	let err = match command(t, rql) {
		Err(err) => err,
		Ok(frames) => panic!("expected a duplicate field error, got {frames:?}\nrql: {rql}"),
	};
	let second = rql.rfind(&format!("{field}:")).expect("the statement repeats the field");
	assert_eq!(err.code, "QUERY_009", "{rql}: {err:?}");
	assert_eq!(err.fragment.text(), field, "{rql}: the error must name the duplicate field");
	assert_eq!(*err.fragment.column() as usize, second + 1, "{rql}: the error must point at the second occurrence");
}

#[test]
fn inserting_a_row_that_gives_a_field_twice_is_an_error_in_every_target() {
	// Two values for one field are ambiguous, so no target may keep the last one silently.
	let t = engine();

	for (target, row) in [
		("s::t", "{ a: 1, b: 2, b: 3 }"),
		("s::r", "{ a: 1, b: 2, b: 3 }"),
		("s::q", "{ a: 1, b: 2, b: 3 }"),
		("s::x", "{ a: 1, a: 2, b: 3 }"),
		("s::m", "{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: 1, tag: 0 }"),
		("s::d", "{ value: 'a', value: 'b' }"),
	] {
		let field = if target == "s::m" {
			"tag"
		} else if target == "s::d" {
			"value"
		} else if target == "s::x" {
			"a"
		} else {
			"b"
		};
		assert_duplicate_field(&t, &format!("INSERT {target} [{row}]"), field);
		assert_eq!(TestEngine::row_count(&t.query(&format!("FROM {target}"))), 0, "{target}");
	}
}

#[test]
fn a_duplicate_field_in_a_later_row_is_an_error_and_inserts_no_row() {
	// The check must cover every row, not only the first one of the statement.
	let t = engine();

	assert_duplicate_field(&t, "INSERT s::t [{ a: 1, b: 2 }, { a: 3, b: 4, b: 5 }]", "b");

	assert_eq!(TestEngine::row_count(&t.query("FROM s::t")), 0);
}

#[test]
fn the_same_field_in_different_rows_is_not_a_duplicate() {
	// Uniqueness is per row, so repeating a field across rows must still insert every row.
	let t = engine();

	command(&t, "INSERT s::t [{ a: 1, b: 2 }, { a: 3, b: 4 }]").unwrap_or_else(|e| panic!("{e:?}"));

	let mut stored = column_values(&t.query("FROM s::t"), "b");
	stored.sort_by_key(|b| match b {
		Value::Int4(b) => *b,
		other => panic!("b must be int4, got {other:?}"),
	});
	assert_eq!(stored, vec![Value::Int4(2), Value::Int4(4)]);
}

#[test]
fn a_constructor_that_gives_a_field_twice_is_an_error_in_every_constructor_form() {
	// Two radius values are ambiguous, so every way of writing a constructor must refuse them.
	let t = engine();
	t.command("INSERT s::g [{ id: 1, shape: s::shape::Circle { radius: 0.5 } }]");

	for rql in [
		"INSERT s::g [{ id: 2, shape: s::shape::Circle { radius: 1.0, radius: 2.0 } }]",
		"INSERT s::g [{ id: 2, shape: Circle { radius: 1.0, radius: 2.0 } }]",
		"UPDATE s::g { shape: Circle { radius: 1.0, radius: 2.0 } } FILTER { id == 1 }",
		"UPDATE s::g { shape: s::shape::Circle { radius: 1.0, radius: 2.0 } } FILTER { id == 1 }",
	] {
		assert_duplicate_field(&t, rql, "radius");
	}
	assert_eq!(TestEngine::row_count(&t.query("FROM s::g")), 1);
}

#[test]
fn an_update_that_assigns_a_column_twice_is_an_error_and_changes_nothing() {
	// Two assignments to one column are ambiguous, so no target may keep either one silently.
	let t = engine();

	for target in ["s::t", "s::r", "s::x"] {
		t.command(&format!("INSERT {target} [{{ a: 1, b: 2 }}]"));

		assert_duplicate_field(&t, &format!("UPDATE {target} {{ b: 3, b: 4 }} FILTER {{ a == 1 }}"), "b");

		assert_eq!(column_values(&t.query(&format!("FROM {target}")), "b"), vec![Value::Int4(2)], "{target}");
	}
}

#[test]
fn a_map_that_gives_an_alias_twice_is_an_error() {
	// Two columns named a make a later insert of the frame keep one value silently.
	let t = engine();
	let rql = "MAP { a: 1, a: 2 }";

	let err = match command(&t, rql) {
		Err(err) => err,
		Ok(frames) => panic!("expected a duplicate alias error, got {frames:?}"),
	};

	assert_eq!(err.fragment.text(), "a", "{err:?}");
	assert_eq!(*err.fragment.column() as usize, rql.rfind("a:").unwrap() + 1, "{err:?}");
}
