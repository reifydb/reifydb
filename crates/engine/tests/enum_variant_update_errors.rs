// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{params::Params, value::frame::frame::Frame};

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
	t.command("INSERT s::e [{ id: 1, status: Active }]");
	t.command("INSERT s::g [{ id: 1, shape: s::shape::Dot }]");
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
fn an_update_rejects_a_qualified_variant_for_a_plain_or_unknown_column_with_the_insert_error() {
	// A table and a ringbuffer share the patch path, so neither may hand the variant to the evaluator.
	let t = engine();

	for source in ["s::t", "s::r"] {
		assert_update_rejects_like_insert(&t, source, "a == 1", "a: s::status::Active", "a: s::status::Active");
		assert_update_rejects_like_insert(
			&t,
			source,
			"a == 1",
			"nope: s::status::Active",
			"nope: s::status::Active",
		);
	}
}

#[test]
fn an_update_rejects_a_variant_its_enum_does_not_have_with_the_insert_error() {
	// An unknown variant must be a diagnostic at the variant, never a panic while expanding the enum columns.
	let t = engine();

	assert_update_rejects_like_insert(
		&t,
		"s::e",
		"id == 1",
		"id: 9, status: s::status::Nope",
		"status: s::status::Nope",
	);
}

#[test]
fn an_update_rejects_an_unqualified_name_its_enum_does_not_have_with_the_insert_error() {
	// A bare name under an enum column is a variant, so a typo must be the INSERT error, never a missing column.
	let t = engine();

	assert_update_rejects_like_insert(&t, "s::e", "id == 1", "id: 9, status: Nope", "status: Nope");
}

#[test]
fn an_update_rejects_a_variant_of_another_enum_with_the_insert_error() {
	// A variant is looked up in the enum it names, otherwise another enum's Active silently stores this enum's tag.
	let t = engine();

	for variant in ["s::other::Gone", "s::other::Active"] {
		assert_update_rejects_like_insert(
			&t,
			"s::e",
			"id == 1",
			&format!("id: 9, status: {variant}"),
			&format!("status: {variant}"),
		);
	}
}

#[test]
fn an_update_rejects_a_field_the_variant_does_not_have_with_the_insert_error() {
	// An unknown field must be rejected, otherwise its value is dropped and the declared fields become none.
	let t = engine();

	assert_update_rejects_like_insert(
		&t,
		"s::g",
		"id == 1",
		"id: 9, shape: s::shape::Circle { nope: 1.0 }",
		"shape: s::shape::Circle { nope: 1.0 }",
	);
}

#[test]
fn an_update_rejects_a_variant_of_an_unknown_enum_with_the_insert_error() {
	// The named enum must be resolved first, otherwise a typo in its path reads as a column type mismatch.
	let t = engine();

	for variant in ["s::nope::Active", "nope::status::Active"] {
		assert_update_rejects_like_insert(
			&t,
			"s::t",
			"a == 1",
			&format!("a: {variant}"),
			&format!("a: {variant}"),
		);
	}
}

#[test]
fn a_variant_inside_an_update_expression_is_a_value_or_an_error_never_a_panic() {
	// Only a variant directly under the column is expanded, so a nested one must not reach the evaluator unhandled.
	let t = engine();

	let error = error_of(&t, "UPDATE s::t { a: if a == 1 { s::status::Active } else { 2 } } FILTER { a == 1 }");

	assert!(error == "no error" || error.contains("Active"), "the error must point at the variant: {error}");
}

#[test]
fn a_qualified_variant_in_a_query_patch_is_a_value_or_an_error_never_a_panic() {
	// A query patch has no write target, so the variant must still be handled without an INSERT to expand it.
	let t = engine();

	for (rql, variant) in [
		("FROM s::t PATCH { a: s::status::Active }", "Active"),
		("FROM s::e PATCH { status: s::status::Nope }", "Nope"),
	] {
		let result = t.inner().query_as(TestEngine::identity(), rql, Params::None);

		if let Some(err) = result.error {
			let diagnostic = err.diagnostic();
			assert!(
				diagnostic.fragment.text().contains(variant),
				"{rql}: the error must point at the variant: {diagnostic:?}"
			);
		} else {
			assert_ne!(variant, "Nope", "{rql}: an unknown variant has no value");
		}
	}
}

#[test]
fn a_query_patch_of_an_enum_column_gives_its_tag_the_type_a_table_read_gives() {
	// IS only matches a tag of the stored type, so a patched tag of another type silently matches no row.
	let t = engine();

	let read = t.query("FROM s::e");
	let patched = t.query("FROM s::e PATCH { status: s::status::Inactive }");

	assert_eq!(status_tag(&patched).0, status_tag(&read).0);
	assert_eq!(status_tag(&patched).1, ["1"]);
}

fn status_tag(frames: &[Frame]) -> (String, Vec<String>) {
	let column = frames[0].columns.iter().find(|c| c.name == "status_tag").expect("column status_tag");
	let values = (0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect();
	(column.data.get_type().to_string(), values)
}

#[test]
fn an_update_rejects_a_field_value_out_of_its_range_with_the_insert_error() {
	// The stored field type is applied once when the row is written, so an UPDATE must fail exactly like INSERT.
	let t = engine();

	assert_update_rejects_like_insert(
		&t,
		"s::g",
		"id == 1",
		"id: 9, shape: s::shape::Circle { radius: 1e400 }",
		"shape: s::shape::Circle { radius: 1e400 }",
	);
}

#[test]
fn a_query_patch_of_an_enum_column_gives_its_field_columns_the_types_a_table_read_gives() {
	// A field column of another type than the stored one breaks unions and comparisons with a table read.
	let t = engine();

	let read = column_of(&t.query("FROM s::g"), "shape_circle_radius");
	for (patch, values) in [
		("FROM s::g PATCH { shape: s::shape::Circle { radius: 3.0 } }", vec!["3"]),
		("FROM s::g PATCH { shape: s::shape::Dot }", vec!["none"]),
	] {
		let patched = column_of(&t.query(patch), "shape_circle_radius");

		assert_eq!(patched, (read.0.clone(), values.iter().map(|v| v.to_string()).collect()), "{patch}");
	}
}

fn column_of(frames: &[Frame], name: &str) -> (String, Vec<String>) {
	let column = frames[0].columns.iter().find(|c| c.name == name).expect("column");
	let values = (0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect();
	(column.data.get_type().to_string(), values)
}
