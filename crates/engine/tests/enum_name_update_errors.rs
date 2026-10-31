// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	params::Params,
	value::{Value, enum_variant::EnumVariant, frame::frame::Frame, value_type::ValueType},
};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin("CREATE ENUM s::status { Active, Inactive }");
	t.admin("CREATE ENUM s::other { Active, Gone }");
	t.admin("CREATE ENUM s::shape { Circle { radius: float8 }, Dot }");
	t.admin("CREATE TABLE s::t { a: int4 }");
	t.admin("CREATE TABLE s::e { id: int4, status: s::status }");
	t.admin("CREATE TABLE s::g { id: int4, shape: s::shape }");
	t.admin("CREATE RINGBUFFER s::r { a: int4 } WITH { capacity: 10 }");
	t.command("INSERT s::t [{ a: 1 }]");
	t.command("INSERT s::e [{ id: 1, status: 'active' }]");
	t.command("INSERT s::g [{ id: 1, shape: 'dot' }]");
	t.command("INSERT s::r [{ a: 1 }]");
	t
}

fn error_of(t: &TestEngine, rql: &str) -> String {
	match t.inner().command_as(TestEngine::identity(), rql, Params::None).error {
		Some(err) => {
			let diagnostic = err.diagnostic();
			format!("{} at {:?}", diagnostic.code, diagnostic.fragment.text())
		}
		None => "no error".to_string(),
	}
}

fn assert_update_rejects_like_insert(t: &TestEngine, source: &str, filter: &str, insert_row: &str, assignment: &str) {
	// The INSERT error is read at run time, so the UPDATE must follow whatever INSERT decides.
	let before = t.query(&format!("FROM {source}"));
	let insert = error_of(t, &format!("INSERT {source} [{{ {insert_row} }}]"));
	let update = format!("UPDATE {source} {{ {assignment} }} FILTER {{ {filter} }}");

	assert_ne!(insert, "no error", "INSERT {source} [{{ {insert_row} }}] must be rejected");
	assert_eq!(error_of(t, &update), insert, "{update}");
	assert_eq!(t.query(&format!("FROM {source}")), before, "{update} must leave the rows unchanged");
}

#[test]
fn an_update_rejects_a_name_its_enum_does_not_have_with_the_insert_error() {
	// An unknown name must be a diagnostic at the name, never a panic while expanding the enum columns.
	let t = engine();

	assert_update_rejects_like_insert(&t, "s::e", "id == 1", "id: 9, status: 'nope'", "status: 'nope'");
}

#[test]
fn an_update_rejects_a_field_the_variant_does_not_have_with_the_insert_error() {
	// An unknown field must be rejected, otherwise its value is dropped and the declared fields become none.
	let t = engine();

	assert_update_rejects_like_insert(
		&t,
		"s::g",
		"id == 1",
		"id: 9, shape: 'circle', shape_circle_nope: 1.0",
		"shape: 'circle', shape_circle_nope: 1.0",
	);
}

#[test]
fn a_query_patch_of_an_enum_column_gives_its_tag_the_type_a_table_read_gives() {
	// A name test only matches a tag of the stored type, so a patched tag of another type silently matches no row.
	let t = engine();

	let read = t.query("FROM s::e");
	let patched = t.query("FROM s::e PATCH { status: 'inactive' }");

	assert_eq!(status_name(&patched).0, status_name(&read).0);
	assert_eq!(status_name(&patched).1, [Value::Enum(EnumVariant::new("s::status", "inactive"))]);
}

fn status_name(frames: &[Frame]) -> (Option<String>, Vec<Value>) {
	let column = frames[0].column("status").unwrap().expect("column status");
	let enum_name = match column.get_type().inner_type() {
		ValueType::Enum(name) => Some(name.clone()),
		_ => None,
	};
	(enum_name, (0..column.len()).map(|i| column.get_value(i)).collect())
}

#[test]
fn an_update_rejects_a_field_value_out_of_its_range_with_the_insert_error() {
	// The stored field type is applied once when the row is written, so an UPDATE must fail exactly like INSERT.
	let t = engine();

	assert_update_rejects_like_insert(
		&t,
		"s::g",
		"id == 1",
		"id: 9, shape: 'circle', shape_circle_radius: 1e400",
		"shape: 'circle', shape_circle_radius: 1e400",
	);
}

#[test]
fn a_query_patch_of_an_enum_column_gives_its_field_columns_the_types_a_table_read_gives() {
	// A field column of another type than the stored one breaks unions and comparisons with a table read.
	let t = engine();

	let read = column_of(&t.query("FROM s::g"), "shape_circle_radius");
	for (patch, values) in [
		("FROM s::g PATCH { shape: 'circle', shape_circle_radius: 3.0 }", vec!["3"]),
		("FROM s::g PATCH { shape: 'dot' }", vec!["none"]),
	] {
		let patched = column_of(&t.query(patch), "shape_circle_radius");

		assert_eq!(patched, (read.0.clone(), values.iter().map(|v| v.to_string()).collect()), "{patch}");
	}
}

fn column_of(frames: &[Frame], name: &str) -> (String, Vec<String>) {
	let column = frames[0].column(name).unwrap().expect("column");
	let values = (0..column.len()).map(|i| column.get_value(i).to_string()).collect();
	(column.get_type().to_string(), values)
}
