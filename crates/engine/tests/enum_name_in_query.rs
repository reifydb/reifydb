// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::panic::{AssertUnwindSafe, catch_unwind};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{error::Diagnostic, params::Params, value::frame::frame::Frame};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin("CREATE ENUM s::status { Active, Inactive }");
	t.admin("CREATE ENUM s::other { Active, Gone }");
	t.admin("CREATE ENUM s::shape { Circle { radius: float8 }, Dot }");
	t.admin("CREATE TABLE s::t { a: int4 }");
	t.admin("CREATE TABLE s::e { id: int4, status: s::status }");
	t.command("INSERT s::t [{ a: 1 }]");
	t.command("INSERT s::e [{ id: 1, status: 'active' }, { id: 2, status: 'inactive' }]");
	t
}

fn query(t: &TestEngine, rql: &str) -> Result<Vec<Frame>, Box<Diagnostic>> {
	let r = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => Err(e.0),
		None => Ok(r.frames),
	}
}

fn command(t: &TestEngine, rql: &str) -> Result<Vec<Frame>, Box<Diagnostic>> {
	let r = t.inner().command_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => Err(e.0),
		None => Ok(r.frames),
	}
}

fn error_text(result: Result<Vec<Frame>, Box<Diagnostic>>) -> String {
	match result {
		Err(diagnostic) => format!("{} at {:?}", diagnostic.code, diagnostic.fragment.text()),
		Ok(frames) => format!("no error, got {frames:?}"),
	}
}

fn ids(frames: &[Frame]) -> Vec<String> {
	let column = frames[0].column("id").unwrap().expect("column id");
	let mut ids: Vec<String> = (0..column.len()).map(|row| column.get_value(row).to_string()).collect();
	ids.sort();
	ids
}

#[test]
fn filtering_an_enum_column_by_variant_equality_keeps_exactly_the_rows_of_that_variant() {
	// Equality with a variant must select rows by the stored tag, also under and, or and not.
	let t = engine();

	for (condition, expected) in [
		("status == 'active'", vec!["1"]),
		("status == 'inactive' or id == 1", vec!["1", "2"]),
		("status == 'active' and id == 2", vec![]),
		("not status == 'active'", vec!["2"]),
	] {
		assert_eq!(ids(&t.query(&format!("FROM s::e FILTER {{ {condition} }}"))), expected, "{condition}");
	}
}

#[test]
fn a_name_its_enum_does_not_have_is_an_error_at_the_name() {
	// A name that cannot be resolved must be a diagnostic, never the panic when the filter compiles.
	let t = engine();

	for (rql, name) in
		[("FROM s::e FILTER { status == 'nope' }", "nope"), ("FROM s::e FILTER { status != 'gone' }", "gone")]
	{
		let err = query(&t, rql).expect_err(rql);
		assert_eq!(err.code, "CA_100", "{rql}: {err:?}");
		assert!(err.fragment.text().contains(name), "{rql}: the error must point at the name: {err:?}");
	}
}

#[test]
fn a_name_on_a_column_that_is_not_an_enum_is_an_error_never_a_panic() {
	// A plain or unknown column has no tag to compare, so a name must be rejected before the filter compiles.
	let t = engine();

	for (rql, fragments) in [
		("FROM s::e FILTER { id == 'active' }", ["id", "Active"]),
		("FROM s::e FILTER { nope == 'active' }", ["nope", "Active"]),
	] {
		let err = query(&t, rql).expect_err(rql);
		assert!(
			fragments.iter().any(|f| err.fragment.text().contains(f)),
			"{rql}: the error must point at the column or the variant: {err:?}"
		);
	}
}

#[test]
fn a_name_after_an_extend_keeps_the_rows_of_that_variant_or_is_an_error_at_the_variant() {
	// An extend keeps the enum columns, so a name after it must still select by the tag, never panic.
	let t = engine();

	match query(&t, "FROM s::e | extend { y: 1 } | filter { status == 'active' }") {
		Ok(frames) => assert_eq!(ids(&frames), ["1"]),
		Err(err) => {
			assert!(err.fragment.text().contains("Active"), "the error must point at the variant: {err:?}")
		}
	}
}

#[test]
fn a_name_in_a_map_or_extend_gives_the_result_a_filter_gives() {
	// Every node must resolve a name the same way, otherwise a map matches rows a filter rejects or panics on them.
	let t = engine();

	for condition in [
		"status == 'active'",
		"status != 'active'",
		"status == 'nope'",
		"status == 'gone'",
		"id == 'active'",
		"nope == 'active'",
	] {
		let filter = match query(&t, &format!("FROM s::e FILTER {{ {condition} }}")) {
			Ok(frames) => format!("{:?}", ids(&frames)),
			Err(diagnostic) => error_text(Err(diagnostic)),
		};
		for rql in [
			format!("FROM s::e | map {{ id, x: {condition} }}"),
			format!("FROM s::e | extend {{ x: {condition} }}"),
		] {
			let got = match catch_unwind(AssertUnwindSafe(|| query(&t, &rql))) {
				Err(_) => "panicked".to_string(),
				Ok(Ok(frames)) => format!("{:?}", true_ids(&frames)),
				Ok(Err(diagnostic)) => error_text(Err(diagnostic)),
			};
			assert_eq!(got, filter, "{rql}");
		}
	}

	let insert = error_text(command(&t, "INSERT s::e [{ id: 9, status: 'nope' }]"));
	let rql = "FROM s::e | map { id, x: MATCH status { 'nope' => true, ELSE => false } }";
	let got = match catch_unwind(AssertUnwindSafe(|| query(&t, rql))) {
		Err(_) => "panicked".to_string(),
		Ok(result) => error_text(result),
	};
	assert_eq!(got, insert, "{rql}");
}

#[test]
fn a_name_inside_parentheses_keeps_the_rows_of_that_variant() {
	// Parentheses do not change a condition, so a name test inside them must select the same rows as without them.
	let t = engine();

	for (condition, expected) in [("(status == 'active')", vec!["1"]), ("not (status == 'active')", vec!["2"])] {
		let rql = format!("FROM s::e FILTER {{ {condition} }}");
		match catch_unwind(AssertUnwindSafe(|| query(&t, &rql))) {
			Err(_) => panic!("{rql}: panicked"),
			Ok(result) => assert_eq!(
				result.map(|frames| ids(&frames)).map_err(|d| error_text(Err(d))),
				Ok(expected.iter().map(|id| id.to_string()).collect()),
				"{rql}"
			),
		}
	}
}

#[test]
fn a_filter_after_a_map_that_renames_or_drops_an_enum_column_is_an_error_never_an_empty_result() {
	// A map can rename or drop the enum columns, so a later filter must not match against the table's enum.
	let t = engine();

	for rql in [
		"FROM s::e | map { status: id } | filter { status == 'active' }",
		"FROM s::e | map { status: id } | filter { status == 'active' }",
		"FROM s::e | map { id } | filter { status == 'active' }",
	] {
		let err = query(&t, rql).expect_err(rql);
		assert!(
			["status", "Active"].iter().any(|f| err.fragment.text().contains(f)),
			"{rql}: the error must point at the column or the variant: {err:?}"
		);
	}
}

#[test]
fn a_name_in_a_patch_or_update_assignment_selects_by_the_stored_tag() {
	// A patch reads the same enum columns as a filter, so a name in its assignment must resolve the tag.
	let t = engine();
	let assignment = "id: if status == 'active' { 10 } else { 20 }";

	let patched = catch_unwind(AssertUnwindSafe(|| query(&t, &format!("FROM s::e PATCH {{ {assignment} }}"))))
		.map_err(|_| "panicked");
	assert_eq!(
		patched.map(|result| result.map(|frames| ids(&frames))),
		Ok(Ok(vec!["10".to_string(), "20".to_string()]))
	);

	let updated = catch_unwind(AssertUnwindSafe(|| {
		command(&t, &format!("UPDATE s::e {{ {assignment} }} FILTER {{ id > 0 }}")).map(|_| ())
	}))
	.map_err(|_| "panicked");
	assert_eq!(updated, Ok(Ok(())));
	assert_eq!(ids(&t.query("FROM s::e")), ["10", "20"]);
}

fn true_ids(frames: &[Frame]) -> Vec<String> {
	let x = frames[0].column("x").unwrap().expect("column x");
	let id = frames[0].column("id").unwrap().expect("column id");
	let mut ids: Vec<String> = (0..x.len())
		.filter(|row| x.get_value(*row).to_string() == "true")
		.map(|row| id.get_value(row).to_string())
		.collect();
	ids.sort();
	ids
}

#[test]
fn a_filter_after_a_map_that_keeps_the_enum_tag_still_selects_the_rows_of_that_variant() {
	// A map that keeps the tag column must keep the enum known, otherwise a name after it is an error.
	let t = engine();

	for rql in [
		"FROM s::e | map { id, status_tag } | filter { status == 'active' }",
		"FROM s::e | map { id, status_tag: status_tag } | filter { status == 'active' }",
		"FROM s::e | map { id, status_tag } | filter { status == 'active' }",
	] {
		let got = query(&t, rql).map(|frames| ids(&frames)).map_err(|d| error_text(Err(d)));
		assert_eq!(got, Ok(vec!["1".to_string()]), "{rql}");
	}
}

#[test]
fn a_filter_after_a_map_that_overwrites_the_enum_tag_is_an_error_at_the_column() {
	// A tag column filled from another column no longer holds the enum, so a name after it must not match by tag.
	let t = engine();

	for rql in [
		"FROM s::e | map { id, status_tag: id } | filter { status == 'active' }",
		"FROM s::e | map { id, status_tag: id } | filter { status != 'active' }",
	] {
		let err = query(&t, rql).expect_err(rql);
		assert_eq!(err.fragment.text(), "status", "{rql}: {err:?}");
	}
}
