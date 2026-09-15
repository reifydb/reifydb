// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{error::Diagnostic, params::Params, value::frame::column::FrameColumn};

const LOOP: &str = "LET $i = 0; WHILE $i < 100 { IF $i >= 1 { BREAK }; $i = $i + 1 };";

struct Path {
	name: &'static str,
	value: String,
	bound_type: String,
	type_from_result_column: bool,
}

fn engine() -> TestEngine {
	// Two rows, otherwise a udf body calling another udf falls back to the scalar path, never the batch paths.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4 }");
	t.command("INSERT test::t [{ a: 1 }, { a: 2 }]");
	t
}

fn paths(param_type: &str, arg: &str) -> Vec<Path> {
	// Scalar bodies must read the type via meta::type, otherwise per-row result widening hides the bound type.
	let vectorized = format!("UDF f ($x: {param_type}) {{ RETURN $x }}; ");
	let script = |returned: &str| format!("UDF f ($x: {param_type}) {{ RETURN {returned} }}; ");
	let per_row = |returned: &str| format!("UDF f ($x: {param_type}) {{ {LOOP} RETURN {returned} }}; ");
	let closure = |returned: &str| format!("let $f = ($x: {param_type}) {{ {returned} }}; ");
	let outer = "UDF outer ($y) { RETURN f($y) }; ";
	let scalar = |name, query: &dyn Fn(&str) -> String| Path {
		name,
		value: query("$x"),
		bound_type: query("meta::type($x)"),
		type_from_result_column: false,
	};
	let batch = |name, query: String| Path {
		name,
		value: query.clone(),
		bound_type: query,
		type_from_result_column: true,
	};
	vec![
		scalar("script call", &|r| format!("{}let $v = f({arg}); map {{ v: $v }}", script(r))),
		scalar("map without input", &|r| format!("{}map {{ v: f({arg}) }}", per_row(r))),
		batch("vectorized udf over rows", format!("{vectorized}FROM test::t | map {{ v: f({arg}) }}")),
		scalar("per-row udf over rows", &|r| format!("{}FROM test::t | map {{ v: f({arg}) }}", per_row(r))),
		batch(
			"vectorized udf called from a udf body",
			format!("{vectorized}{outer}FROM test::t | map {{ v: outer({arg}) }}"),
		),
		scalar("per-row udf called from a udf body", &|r| {
			format!("{}{outer}FROM test::t | map {{ v: outer({arg}) }}", per_row(r))
		}),
		scalar("closure script call", &|r| format!("{}let $v = $f({arg}); map {{ v: $v }}", closure(r))),
		scalar("closure without input", &|r| format!("{}map {{ v: $f({arg}) }}", closure(r))),
		scalar("closure over rows", &|r| format!("{}FROM test::t | map {{ v: $f({arg}) }}", closure(r))),
	]
}

fn run(t: &TestEngine, rql: &str) -> Result<FrameColumn, Diagnostic> {
	let result = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	if let Some(err) = result.error {
		return Err(err.diagnostic());
	}
	assert_eq!(result.frames.len(), 1, "expected one frame for {rql}, got {:?}", result.frames);
	let column = result.frames[0].columns.iter().find(|c| c.name == "v").cloned();
	Ok(column.unwrap_or_else(|| panic!("no column v for {rql}")))
}

fn same_text_on_every_row(path: &str, column: &FrameColumn) -> String {
	let texts: Vec<String> = (0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect();
	assert!(
		!texts.is_empty() && texts.iter().all(|v| *v == texts[0]),
		"{path}: every row binds the same argument, got {texts:?}"
	);
	texts[0].clone()
}

fn bound(t: &TestEngine, path: &Path) -> (String, String) {
	let succeed = |rql: &str| {
		run(t, rql).unwrap_or_else(|err| panic!("{}: the call must succeed, got {err:?}", path.name))
	};
	let value = same_text_on_every_row(path.name, &succeed(&path.value));
	let type_column = succeed(&path.bound_type);
	let ty = if path.type_from_result_column {
		type_column.data.get_type().to_string()
	} else {
		same_text_on_every_row(path.name, &type_column)
	};
	(ty, value)
}

#[test]
fn a_list_for_a_utf8_parameter_is_the_cast_error_naming_the_parameter_on_every_path() {
	// A path binding without the cast runs the body on a list, so every path must fail with the same error.
	let t = engine();

	for path in paths("utf8", "[1, 2]") {
		let err = run(&t, &path.value)
			.expect_err(&format!("{}: a list is not utf8, so the call must fail", path.name));

		assert_eq!(
			(err.code.as_str(), err.fragment.text()),
			("CAST_001", "x"),
			"{}: the error must be the cast error naming the parameter, got {err:?}",
			path.name
		);
	}
}

#[test]
fn a_procedure_given_a_list_for_a_utf8_parameter_fails_with_the_same_cast_error_as_a_udf() {
	// UDFs reuse the procedure binding rule, so both must reject the same argument with the same diagnostic.
	let t = engine();
	t.admin("CREATE PROCEDURE test::p { x: utf8 } AS { map { v: $x } }");

	let procedure = t
		.inner()
		.command_as(TestEngine::identity(), "CALL test::p([1, 2])", Params::None)
		.error
		.expect("a list is not utf8, so the procedure call must fail")
		.diagnostic();
	let udf = run(&t, "UDF f ($x: utf8) { RETURN $x }; let $v = f([1, 2]); map { v: $v }")
		.expect_err("a list is not utf8");

	assert_eq!((procedure.code.as_str(), procedure.fragment.text()), ("CAST_001", "x"), "{procedure:?}");
	assert_eq!(
		(udf.code, udf.fragment.text(), udf.message),
		(procedure.code, procedure.fragment.text(), procedure.message)
	);
}

#[test]
fn an_int_for_a_utf8_parameter_binds_as_its_text_on_every_path() {
	// The body must see the declared type, so the parameter holds text even though the argument was a number.
	let t = engine();

	for path in paths("utf8", "42") {
		let (ty, value) = bound(&t, &path);

		assert_eq!((ty.as_str(), value.as_str()), ("Utf8", "42"), "{}", path.name);
	}
}

#[test]
fn a_numeric_text_for_an_int4_parameter_binds_as_the_parsed_int4_on_every_path() {
	// Without the cast the body would see the text '5' instead of the int4 the parameter declares.
	let t = engine();

	for path in paths("int4", "'5'") {
		let (ty, value) = bound(&t, &path);

		assert_eq!((ty.as_str(), value.as_str()), ("Int4", "5"), "{}", path.name);
	}
}

#[test]
fn a_non_numeric_text_for_an_int4_parameter_is_the_cast_error_naming_the_parameter_on_every_path() {
	// The cast failure must surface as the parameter's error with the parse error kept as cause, never as a none.
	let t = engine();

	for path in paths("int4", "'abc'") {
		let err = run(&t, &path.value)
			.expect_err(&format!("{}: 'abc' is not an int4, so the call must fail", path.name));

		assert_eq!((err.code.as_str(), err.fragment.text()), ("CAST_002", "x"), "{}: {err:?}", path.name);
		assert_eq!(err.cause.as_ref().map(|c| c.code.as_str()), Some("NUMBER_001"), "{}: {err:?}", path.name);
	}
}

#[test]
fn none_for_a_non_option_int4_parameter_binds_as_a_none_of_int4_on_every_path() {
	// Procedures bind none into a plain T as a none of T, so a UDF must not leak an untyped none or reject it.
	let t = engine();

	for path in paths("int4", "none") {
		let (ty, value) = bound(&t, &path);

		assert_eq!((ty.as_str(), value.as_str()), ("Option(Int4)", "none"), "{}", path.name);
	}
}

#[test]
fn none_for_an_option_int4_parameter_binds_as_a_none_of_int4_on_every_path() {
	// Option(T) must accept none and type it exactly like plain T; closures cannot parse Option(T) yet.
	let t = engine();

	for path in paths("Option(int4)", "none").into_iter().filter(|p| !p.name.starts_with("closure")) {
		let (ty, value) = bound(&t, &path);

		assert_eq!((ty.as_str(), value.as_str()), ("Option(Int4)", "none"), "{}", path.name);
	}
}

#[test]
fn an_untyped_parameter_binds_the_argument_unchanged_on_every_path() {
	// Only a declared type may cast; an untyped parameter that cast would turn this list into text or an error.
	let t = engine();

	for mut path in paths("utf8", "[1, 2]") {
		path.value = path.value.replace("($x: utf8)", "($x)");
		path.bound_type = path.bound_type.replace("($x: utf8)", "($x)");
		let (ty, value) = bound(&t, &path);

		assert_eq!(
			(ty.as_str(), value.as_str()),
			("Any", "[1, 2]"),
			"{}: the list must reach the body unchanged",
			path.name
		);
	}
}
