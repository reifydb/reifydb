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
	t.admin("CREATE TABLE s::e { id: int4, status: s::status }");
	t.command("INSERT s::t [{ a: 1 }]");
	t.command("INSERT s::e [{ id: 1, status: Active }, { id: 2, status: Inactive }]");
	t
}

fn command_err(t: &TestEngine, rql: &str) -> Diagnostic {
	let r = t.inner().command_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => e.diagnostic(),
		None => panic!("expected an error, got frames {:?}\nrql: {rql}", r.frames),
	}
}

fn query(t: &TestEngine, rql: &str) -> Result<Vec<Frame>, Box<Diagnostic>> {
	let r = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => Err(e.0),
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

#[test]
fn updating_an_int_column_with_a_qualified_variant_is_the_error_an_insert_gives() {
	// INSERT refuses a variant for an int4 column with CA_101, so UPDATE must too, never panic the evaluator.
	let t = engine();

	let err = command_err(&t, "UPDATE s::t { a: s::status::Active } FILTER { a == 1 }");

	assert_eq!(err.code, "CA_101", "got: {err:?}");
	assert_eq!(err.fragment.text(), "Active", "got: {err:?}");
	assert_eq!(column_values(&t.query("FROM s::t"), "a"), vec![Value::Int4(1)]);
}

#[test]
fn updating_an_unknown_column_with_a_qualified_variant_is_column_not_found() {
	// INSERT names the unknown column with QUERY_001, so UPDATE must too, never panic the evaluator.
	let t = engine();

	let err = command_err(&t, "UPDATE s::t { nope: s::status::Active } FILTER { a == 1 }");

	assert_eq!(err.code, "QUERY_001", "got: {err:?}");
	assert_eq!(err.fragment.text(), "nope", "got: {err:?}");
}

#[test]
fn a_qualified_variant_in_a_map_is_a_value_or_an_error_never_a_panic() {
	// Any query can name a variant, so the evaluator must not assume inline data already expanded it.
	let t = engine();

	match query(&t, "FROM s::t MAP { x: s::status::Active }") {
		Ok(frames) => assert_eq!(TestEngine::row_count(&frames), 1, "one input row must map to one row"),
		Err(err) => {
			assert!(err.fragment.text().contains("Active"), "the error must point at the variant: {err:?}")
		}
	}
}

#[test]
fn filtering_an_enum_column_by_a_qualified_variant_keeps_its_rows_or_is_an_error_never_a_panic() {
	// Only row 1 is Active, so a filter that runs must keep exactly that row.
	let t = engine();

	match query(&t, "FROM s::e FILTER { status == s::status::Active }") {
		Ok(frames) => assert_eq!(column_values(&frames, "id"), vec![Value::Int4(1)]),
		Err(err) => {
			assert!(err.fragment.text().contains("Active"), "the error must point at the variant: {err:?}")
		}
	}
}
