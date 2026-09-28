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
	t.admin("CREATE SERIES s::m { ts: datetime, val: int4 } WITH { key: ts, tag: s::status }");
	t.command("INSERT s::m [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: 1 }]");
	t
}

fn command(t: &TestEngine, rql: &str) -> Result<Vec<Frame>, Box<Diagnostic>> {
	let r = t.inner().command_as(TestEngine::identity(), rql, Params::None);
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

fn assert_tag_update_refused(t: &TestEngine, rql: &str, value: &str) {
	let err = match command(t, rql) {
		Err(err) => err,
		Ok(frames) => panic!("expected the tag update to be refused, got {frames:?}\nrql: {rql}"),
	};
	assert_eq!(err.code, "UPDATE_004", "{rql}: {err:?}");
	assert!(err.message.contains("tag"), "the error must name the tag column: {err:?}");
	assert!(err.message.contains("s::m"), "the error must name the series: {err:?}");
	assert_eq!(err.fragment.text(), value, "{rql}: the error must point at the assigned value");
	assert_eq!(column_values(&t.query("FROM s::m"), "tag"), vec![Value::Uint1(1)], "{rql}");
	assert_eq!(column_values(&t.query("FROM s::m"), "val"), vec![Value::Int4(1)], "{rql}");
}

#[test]
fn updating_the_tag_of_a_series_row_is_an_error_on_the_assigned_value() {
	// The tag is part of the row key, so a changed tag would address no row and update nothing.
	let t = engine();

	assert_tag_update_refused(&t, "UPDATE s::m { tag: 0 } FILTER { val == 1 }", "0");
}

#[test]
fn updating_the_tag_to_its_current_value_is_the_same_error() {
	// The check must not depend on row data, or an update of the same tag still reports updated 0.
	let t = engine();

	assert_tag_update_refused(&t, "UPDATE s::m { tag: 1 } FILTER { val == 1 }", "1");
}

#[test]
fn updating_the_tag_by_variant_name_is_the_tag_error_not_column_not_found() {
	// A variant name is how users write a tag, so it must get the tag error instead of a lookup failure.
	let t = engine();

	assert_tag_update_refused(&t, "UPDATE s::m { tag: Inactive } FILTER { val == 1 }", "Inactive");
}

#[test]
fn updating_the_tag_together_with_another_column_changes_neither() {
	// A refused tag must fail the whole statement, never apply the other assignment.
	let t = engine();

	assert_tag_update_refused(&t, "UPDATE s::m { val: 5, tag: 0 } FILTER { val == 1 }", "0");
}

#[test]
fn updating_another_column_of_a_tagged_series_updates_the_row_and_keeps_its_tag() {
	// Refusing tag updates must not refuse updates of the other columns of a tagged series.
	let t = engine();

	let frames = command(&t, "UPDATE s::m { val: 5 } FILTER { val == 1 }").unwrap_or_else(|e| panic!("{e:?}"));

	assert_eq!(column_values(&frames, "updated"), vec![Value::Uint8(1)]);
	assert_eq!(column_values(&t.query("FROM s::m"), "val"), vec![Value::Int4(5)]);
	assert_eq!(column_values(&t.query("FROM s::m"), "tag"), vec![Value::Uint1(1)]);
}
