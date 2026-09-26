// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![allow(clippy::result_large_err)]

use std::panic::{AssertUnwindSafe, catch_unwind};

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
	t.admin("CREATE ENUM s::other { Active, Gone }");
	t.admin("CREATE ENUM s::shape { Circle { radius: float8 }, Dot }");
	t.admin("CREATE TABLE s::t { a: int4 }");
	t.admin("CREATE TABLE s::e { id: int4, status: s::status }");
	t.command("INSERT s::t [{ a: 1 }]");
	t.command("INSERT s::e [{ id: 1, status: Active }, { id: 2, status: Inactive }]");
	t
}

fn query(t: &TestEngine, rql: &str) -> Result<Vec<Frame>, Diagnostic> {
	let r = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => Err(e.diagnostic()),
		None => Ok(r.frames),
	}
}

fn command(t: &TestEngine, rql: &str) -> Result<Vec<Frame>, Diagnostic> {
	let r = t.inner().command_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => Err(e.diagnostic()),
		None => Ok(r.frames),
	}
}

fn error_text(result: Result<Vec<Frame>, Diagnostic>) -> String {
	match result {
		Err(diagnostic) => format!("{} at {:?}", diagnostic.code, diagnostic.fragment.text()),
		Ok(frames) => format!("no error, got {frames:?}"),
	}
}

fn columns(frames: &[Frame]) -> Vec<(String, String, Vec<String>)> {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let mut columns: Vec<(String, String, Vec<String>)> = frames[0]
		.columns
		.iter()
		.map(|c| {
			let values = (0..c.data.len()).map(|row| c.data.get_value(row).to_string()).collect();
			(c.name.clone(), c.data.get_type().to_string(), values)
		})
		.collect();
	columns.sort();
	columns
}

fn ids(frames: &[Frame]) -> Vec<String> {
	let column = frames[0].columns.iter().find(|c| c.name == "id").expect("column id");
	let mut ids: Vec<String> = (0..column.data.len()).map(|row| column.data.get_value(row).to_string()).collect();
	ids.sort();
	ids
}

#[test]
fn a_variant_in_a_map_gives_the_columns_inline_data_gives() {
	// A map must expand a variant into the tag and field columns inline data gives, never fail on the bare variant.
	let t = engine();

	for variant in ["s::status::Inactive", "s::shape::Circle { radius: 1.5 }", "s::shape::Dot"] {
		let inline = columns(&t.query(&format!("FROM [{{ x: {variant} }}]")));
		for rql in [format!("FROM s::t MAP {{ x: {variant} }}"), format!("map {{ x: {variant} }}")] {
			assert_eq!(columns(&t.query(&rql)), inline, "{rql}");
		}
	}
}

#[test]
fn filtering_an_enum_column_by_variant_equality_keeps_exactly_the_rows_of_that_variant() {
	// Equality with a variant must select rows by the stored tag, also under and, or and not.
	let t = engine();

	for (condition, expected) in [
		("status == s::status::Active", vec!["1"]),
		("status == s::status::Inactive or id == 1", vec!["1", "2"]),
		("status == s::status::Active and id == 2", vec![]),
		("not status == s::status::Active", vec!["2"]),
	] {
		assert_eq!(ids(&t.query(&format!("FROM s::e FILTER {{ {condition} }}"))), expected, "{condition}");
	}
}

#[test]
fn filtering_by_a_variant_the_column_cannot_hold_is_the_insert_error() {
	// A filter must resolve the variant as INSERT does, otherwise another enum's Active matches this enum's rows.
	let t = engine();

	for (condition, row) in [
		("status == s::other::Active", "id: 9, status: s::other::Active"),
		("status == s::status::Nope", "id: 9, status: s::status::Nope"),
		("id == s::status::Active", "id: s::status::Active"),
		("nope == s::status::Active", "id: 9, nope: s::status::Active"),
	] {
		let insert = error_text(command(&t, &format!("INSERT s::e [{{ {row} }}]")));
		let filter = error_text(query(&t, &format!("FROM s::e FILTER {{ {condition} }}")));

		assert!(!insert.starts_with("no error"), "INSERT s::e [{{ {row} }}] must be rejected");
		assert_eq!(filter, insert, "{condition}");
	}
	assert_eq!(ids(&t.query("FROM s::e")), ["1", "2"], "a rejected INSERT must not add a row");
}

#[test]
fn a_variant_nested_inside_an_expression_is_an_error_at_the_variant_in_every_position() {
	// Every node that compiles expressions must return the variant error, never panic on it.
	let mut failures = Vec::new();

	for (kind, rql) in [
		("query", "FROM s::t MAP { x: 1 + s::status::Active }"),
		("query", "map { x: 1 + s::status::Active }"),
		("query", "FROM s::t EXTEND { x: 1 + s::status::Active }"),
		("query", "extend { x: 1 + s::status::Active }"),
		("query", "FROM s::t FILTER { a == 1 + s::status::Active }"),
		("query", "FROM s::e FILTER { s::status::Active == status }"),
		("query", "FROM s::e FILTER { status != s::status::Active }"),
		("query", "FROM s::t PATCH { a: 1 + s::status::Active }"),
		("query", "FROM [{ x: 1 + s::status::Active }]"),
		("query", "UDF f ($x) { RETURN 1 }; FROM s::t | map { v: f(1 + s::status::Active) }"),
		("query", "UDF f ($x) { RETURN 1 }; map { v: f(1 + s::status::Active) }"),
		("command", "UPDATE s::t { a: 1 + s::status::Active } FILTER { a == 1 }"),
		("command", "INSERT s::t [{ a: 2 }] RETURNING { x: 1 + s::status::Active }"),
	] {
		let t = engine();
		let outcome = catch_unwind(AssertUnwindSafe(|| {
			if kind == "query" {
				query(&t, rql)
			} else {
				command(&t, rql)
			}
		}));
		match outcome {
			Err(_) => failures.push(format!("{rql}: panicked")),
			Ok(Err(diagnostic)) if diagnostic.fragment.text() == "Active" => {}
			Ok(result) => failures.push(format!("{rql}: {}", error_text(result))),
		}
	}

	assert!(failures.is_empty(), "every position must be an error at the variant, got {failures:#?}");
}

#[test]
fn is_with_a_variant_of_another_enum_never_matches_this_enums_rows() {
	// IS must resolve the variant in the enum it names, otherwise s::other::Active matches Active rows.
	let t = engine();

	match query(&t, "FROM s::e FILTER { status IS s::other::Active }") {
		Ok(frames) => assert_eq!(ids(&frames), Vec::<String>::new(), "no row holds a variant of s::other"),
		Err(err) => {
			assert!(err.fragment.text().contains("Active"), "the error must point at the variant: {err:?}")
		}
	}
}

#[test]
fn is_with_a_variant_its_enum_does_not_have_is_an_error_at_the_variant() {
	// An IS tag that cannot be resolved must be a diagnostic, never the panic when the filter compiles.
	let t = engine();

	for (rql, variant) in [
		("FROM s::e FILTER { status IS s::status::Nope }", "Nope"),
		("FROM s::e FILTER { status IS s::other::Gone }", "Gone"),
	] {
		let err = query(&t, rql).expect_err(rql);
		assert!(err.fragment.text().contains(variant), "{rql}: the error must point at the variant: {err:?}");
	}
}

#[test]
fn is_on_a_column_that_is_not_an_enum_is_an_error_never_a_panic() {
	// A plain or unknown column has no tag to compare, so IS must be rejected before the filter compiles.
	let t = engine();

	for (rql, fragments) in [
		("FROM s::e FILTER { id IS s::status::Active }", ["id", "Active"]),
		("FROM s::e FILTER { nope IS s::status::Active }", ["nope", "Active"]),
	] {
		let err = query(&t, rql).expect_err(rql);
		assert!(
			fragments.iter().any(|f| err.fragment.text().contains(f)),
			"{rql}: the error must point at the column or the variant: {err:?}"
		);
	}
}

#[test]
fn is_after_an_extend_keeps_the_rows_of_that_variant_or_is_an_error_at_the_variant() {
	// An extend keeps the enum columns, so IS after it must still select by the tag, never panic.
	let t = engine();

	match query(&t, "FROM s::e | extend { y: 1 } | filter { status IS s::status::Active }") {
		Ok(frames) => assert_eq!(ids(&frames), ["1"]),
		Err(err) => {
			assert!(err.fragment.text().contains("Active"), "the error must point at the variant: {err:?}")
		}
	}
}

#[test]
fn a_variant_in_a_script_variable_or_udf_result_is_never_a_silent_none() {
	// A variant has no scalar value, so a script must reject it or keep it, never hand back none.
	let t = engine();

	for rql in [
		"let $v = s::status::Active; map { v: $v }",
		"UDF m ($x) { RETURN s::status::Active }; FROM s::t | map { v: m(a) }",
	] {
		match query(&t, rql) {
			Ok(frames) => {
				let column = frames[0].columns.iter().find(|c| c.name == "v").expect("column v");
				let values: Vec<Value> =
					(0..column.data.len()).map(|row| column.data.get_value(row)).collect();
				assert!(
					values.iter().all(|v| !matches!(v, Value::None { .. })),
					"{rql}: the variant became none: {values:?}"
				);
			}
			Err(err) => {
				assert!(
					err.fragment.text().contains("Active"),
					"{rql}: the error must point at the variant: {err:?}"
				)
			}
		}
	}
}

#[test]
fn a_variant_in_an_extend_gives_the_columns_inline_data_gives() {
	// An extend must expand a variant into the columns inline data gives, never reject the bare variant.
	let t = engine();

	for variant in ["s::status::Inactive", "s::shape::Circle { radius: 1.5 }", "s::shape::Dot"] {
		let inline = columns(&t.query(&format!("FROM [{{ x: {variant} }}]")));
		let with_input: Vec<_> = columns(&t.query(&format!("FROM s::t EXTEND {{ x: {variant} }}")))
			.into_iter()
			.filter(|(name, _, _)| name != "a")
			.collect();

		assert_eq!(with_input, inline, "FROM s::t EXTEND {{ x: {variant} }}");
		assert_eq!(
			columns(&t.query(&format!("extend {{ x: {variant} }}"))),
			inline,
			"extend {{ x: {variant} }}"
		);
	}
}

#[test]
fn is_in_a_map_or_extend_gives_the_result_a_filter_gives() {
	// Every node must resolve IS the same way, otherwise a map matches rows a filter rejects or panics on them.
	let t = engine();

	for condition in [
		"status IS s::status::Active",
		"status IS s::other::Active",
		"status IS s::status::Nope",
		"status IS s::other::Gone",
		"id IS s::status::Active",
		"nope IS s::status::Active",
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

	let insert = error_text(command(&t, "INSERT s::e [{ id: 9, status: Nope }]"));
	let rql = "FROM s::e | map { id, x: MATCH status { Nope => true, ELSE => false } }";
	let got = match catch_unwind(AssertUnwindSafe(|| query(&t, rql))) {
		Err(_) => "panicked".to_string(),
		Ok(result) => error_text(result),
	};
	assert_eq!(got, insert, "{rql}");
}

#[test]
fn a_variant_in_a_join_condition_is_an_error_at_the_variant_never_a_panic() {
	// A join compiles its non-key conditions when it runs, so a variant there must come back as an error.
	let mut failures = Vec::new();

	for rql in [
		"FROM s::t INNER JOIN { FROM s::t } AS u USING (a, u.a) AND (a, 1 + s::status::Active)",
		"FROM s::t INNER JOIN { FROM s::t } AS u USING (a, u.a) OR (a, 1 + s::status::Active)",
		"FROM s::t LEFT JOIN { FROM s::t } AS u USING (a, u.a) AND (a, 1 + s::status::Active)",
		"FROM s::t LEFT JOIN { FROM s::t } AS u USING (a, u.a) OR (a, 1 + s::status::Active)",
	] {
		let t = engine();
		match catch_unwind(AssertUnwindSafe(|| query(&t, rql))) {
			Err(_) => failures.push(format!("{rql}: panicked")),
			Ok(Err(diagnostic)) if diagnostic.fragment.text() == "Active" => {}
			Ok(result) => failures.push(format!("{rql}: {}", error_text(result))),
		}
	}

	assert!(failures.is_empty(), "every join must return the variant error, got {failures:#?}");
}

#[test]
fn is_inside_parentheses_keeps_the_rows_of_that_variant() {
	// Parentheses do not change a condition, so IS inside them must select the same rows as without them.
	let t = engine();

	for (condition, expected) in
		[("(status IS s::status::Active)", vec!["1"]), ("not (status IS s::status::Active)", vec!["2"])]
	{
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
		"FROM s::e | map { status: id } | filter { status == s::status::Active }",
		"FROM s::e | map { status: id } | filter { status IS s::status::Active }",
		"FROM s::e | map { id } | filter { status IS s::status::Active }",
	] {
		let err = query(&t, rql).expect_err(rql);
		assert!(
			["status", "Active"].iter().any(|f| err.fragment.text().contains(f)),
			"{rql}: the error must point at the column or the variant: {err:?}"
		);
	}
}

#[test]
fn is_in_a_script_is_an_error_or_a_boolean_never_a_silent_none() {
	// A script value is never an enum column, so IS on it must be rejected or false, never none.
	let t = engine();
	let rql = "let $v = 1; let $b = $v IS s::status::Active; map { b: $b }";

	match query(&t, rql) {
		Ok(frames) => {
			let column = frames[0].columns.iter().find(|c| c.name == "b").expect("column b");
			assert_eq!(column.data.get_type().to_string(), "Boolean", "{rql}: IS became none");
		}
		Err(err) => assert!(
			["$v", "Active"].iter().any(|f| err.fragment.text().contains(f)),
			"{rql}: the error must point at the value or the variant: {err:?}"
		),
	}
}

#[test]
fn is_in_a_patch_or_update_assignment_selects_by_the_stored_tag() {
	// A patch reads the same enum columns as a filter, so IS in its assignment must resolve the tag, never panic.
	let t = engine();
	let assignment = "id: if status IS s::status::Active { 10 } else { 20 }";

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

#[test]
fn is_in_a_filter_keeps_the_rows_of_that_variant_or_gives_the_insert_error() {
	// IS must select by the stored tag and reject exactly what INSERT rejects, never answer every IS with an error.
	let t = engine();

	for (condition, expected) in [("status IS s::status::Active", ["1"]), ("status IS s::status::Inactive", ["2"])]
	{
		let got = query(&t, &format!("FROM s::e FILTER {{ {condition} }}")).map(|frames| ids(&frames));
		assert_eq!(got.map_err(|d| error_text(Err(d))), Ok(expected.map(String::from).to_vec()), "{condition}");
	}

	for (condition, insert_row) in [
		("status IS s::other::Active", "id: 9, status: s::other::Active"),
		("status IS s::other::Gone", "id: 9, status: s::other::Gone"),
		("status IS s::status::Nope", "id: 9, status: s::status::Nope"),
		("id IS s::status::Active", "id: s::status::Active"),
		("nope IS s::status::Active", "id: 9, nope: s::status::Active"),
	] {
		let insert = error_text(command(&t, &format!("INSERT s::e [{{ {insert_row} }}]")));
		assert!(!insert.starts_with("no error"), "INSERT s::e [{{ {insert_row} }}] must be rejected");
		assert_eq!(
			error_text(query(&t, &format!("FROM s::e FILTER {{ {condition} }}"))),
			insert,
			"{condition}"
		);
	}
}

fn true_ids(frames: &[Frame]) -> Vec<String> {
	let x = frames[0].columns.iter().find(|c| c.name == "x").expect("column x");
	let id = frames[0].columns.iter().find(|c| c.name == "id").expect("column id");
	let mut ids: Vec<String> = (0..x.data.len())
		.filter(|row| x.data.get_value(*row).to_string() == "true")
		.map(|row| id.data.get_value(row).to_string())
		.collect();
	ids.sort();
	ids
}

#[test]
fn a_variant_whose_columns_clash_with_another_field_is_the_duplicate_field_error() {
	// A variant column clash must fail like `y: 1, y: 2`, never double or overwrite a column.
	let t = engine();
	let mut failures = Vec::new();

	for (plain, clash) in [
		("FROM s::t | map { y: 1, y: 2 }", "FROM s::t | map { {} }"),
		("FROM [{ y: 1, y: 2 }]", "FROM [{ {} }]"),
		("FROM s::t | extend { y: 1, y: 2 }", "FROM s::t | extend { {} }"),
	] {
		let expected = query(&t, plain).expect_err(plain).code;
		for fields in [
			"x: s::status::Active, x_tag: 1",
			"x_tag: 1, x: s::status::Active",
			"x: s::shape::Circle { radius: 1.5 }, x_circle_radius: 2.5",
		] {
			let rql = clash.replace("{}", fields);
			match query(&t, &rql) {
				Err(diagnostic) if diagnostic.code == expected => {}
				other => {
					failures.push(format!("{rql}: expected {expected}, got {}", error_text(other)))
				}
			}
		}
	}

	assert!(failures.is_empty(), "every clash must be the duplicate field error, got {failures:#?}");
}

#[test]
fn a_filter_after_a_map_that_keeps_the_enum_tag_still_selects_the_rows_of_that_variant() {
	// A map that keeps the tag column must keep the enum known, otherwise IS after it is an error.
	let t = engine();

	for rql in [
		"FROM s::e | map { id, status_tag } | filter { status IS s::status::Active }",
		"FROM s::e | map { id, status_tag: status_tag } | filter { status IS s::status::Active }",
		"FROM s::e | map { id, status_tag } | filter { status == s::status::Active }",
	] {
		let got = query(&t, rql).map(|frames| ids(&frames)).map_err(|d| error_text(Err(d)));
		assert_eq!(got, Ok(vec!["1".to_string()]), "{rql}");
	}
}

#[test]
fn a_filter_after_a_map_that_overwrites_the_enum_tag_is_an_error_at_the_variant() {
	// A tag column filled from another column no longer holds the enum, so IS after it must not match by tag.
	let t = engine();

	for rql in [
		"FROM s::e | map { id, status_tag: id } | filter { status IS s::status::Active }",
		"FROM s::e | map { id, status_tag: id } | filter { status == s::status::Active }",
	] {
		let err = query(&t, rql).expect_err(rql);
		assert_eq!(err.fragment.text(), "Active", "{rql}: {err:?}");
	}
}

#[test]
fn a_variant_whose_columns_clash_with_another_field_in_a_map_without_input_is_the_duplicate_field_error() {
	// A map without input expands variants too, so a clash there must fail like `y: 1, y: 2`.
	let t = engine();

	for rql in [
		"map { x: s::status::Active, x_tag: 1 }",
		"map { x_tag: 1, x: s::status::Active }",
		"map { x: s::shape::Circle { radius: 1.5 }, x_circle_radius: 2.5 }",
	] {
		assert_eq!(error_text(query(&t, rql)).split(" at ").next(), Some("QUERY_009"), "{rql}");
	}
}
