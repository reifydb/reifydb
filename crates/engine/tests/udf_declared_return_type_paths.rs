// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{frame::frame::Frame, value_type::ValueType},
};

const LOOPY: &str =
	"UDF loopy ($x: int4): int4 { LET $i = 0; WHILE $i < 100 { IF $i >= $x { BREAK }; $i = $i + 1 }; RETURN $i }; ";
const TWICE: &str = "UDF twice ($x: int4): int2 { RETURN $x * 2 }; ";

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { g: int4, a: int4 }");
	t.command("INSERT test::t [{ g: 1, a: 10 }, { g: 2, a: 5 }]");
	t
}

fn column_type(frames: &[Frame], name: &str) -> ValueType {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let column = frames[0]
		.columns
		.iter()
		.find(|c| c.name == name)
		.unwrap_or_else(|| panic!("column {name} missing from {:?}", frames[0].columns));
	column.data.get_type()
}

fn column_strings(frames: &[Frame], name: &str) -> Vec<String> {
	let column = frames[0].columns.iter().find(|c| c.name == name).expect("column present");
	(0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect()
}

fn diagnostic(t: &TestEngine, rql: &str) -> Diagnostic {
	let result = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	let Some(err) = result.error else {
		panic!(
			"the value does not fit the declared return type, so the call must fail, got {:?}",
			result.frames
		);
	};
	err.diagnostic()
}

fn assert_names_function_and_declared_type(diagnostic: &Diagnostic, function: &str, declared: &str) {
	assert_eq!(
		(diagnostic.code.as_str(), diagnostic.fragment.text()),
		("CALLABLE_002", function),
		"the error must point at the call, got: {diagnostic:?}"
	);
	assert!(
		diagnostic.message.contains(&format!("`{function}`")) && diagnostic.message.contains(declared),
		"the error must name the function and its declared type, got: {}",
		diagnostic.message
	);
}

#[test]
fn a_udf_result_in_extend_has_the_declared_return_type() {
	// Extend shares the hoisted call with map, so it must not hand back the body's own type either.
	let t = engine();

	let per_row = column_type(&t.query(&format!("{LOOPY}FROM test::t | extend {{ x: loopy(a) }}")), "x");
	let vectorized = column_type(&t.query(&format!("{TWICE}FROM test::t | extend {{ x: twice(a) }}")), "x");

	assert_eq!((per_row, vectorized), (ValueType::Int4, ValueType::Int2), "(per-row udf, vectorized udf)");
}

#[test]
fn a_udf_result_without_an_input_has_the_declared_return_type() {
	// A map or extend with no input evaluates the call once outside the udf node, which must type it the same way.
	let t = engine();

	let map = column_type(&t.query(&format!("{LOOPY}map {{ x: loopy(3) }}")), "x");
	let extend = column_type(&t.query(&format!("{LOOPY}extend {{ x: loopy(3) }}")), "x");
	let vectorizable = column_type(&t.query(&format!("{TWICE}map {{ x: twice(300) }}")), "x");

	assert_eq!(
		(map, extend, vectorizable),
		(ValueType::Int4, ValueType::Int4, ValueType::Int2),
		"(map, extend, twice)"
	);
}

#[test]
fn a_udf_result_as_an_aggregate_input_has_the_declared_return_type() {
	// A max over the call keeps its input type, so a wrong udf column type leaks into the aggregate result.
	let t = engine();

	let per_row = column_type(
		&t.query(&format!("{LOOPY}FROM test::t | aggregate {{ x: math::max(loopy(a)) }} by {{ g }}")),
		"x",
	);
	let vectorized = column_type(
		&t.query(&format!("{TWICE}FROM test::t | aggregate {{ x: math::max(twice(a)) }} by {{ g }}")),
		"x",
	);

	assert_eq!((per_row, vectorized), (ValueType::Int4, ValueType::Int2), "(per-row udf, vectorized udf)");
}

#[test]
fn a_udf_that_returns_none_for_some_rows_gives_an_optional_column_of_its_declared_type() {
	// A none row must stay none and only add Option around the declared type, never an error or a default value.
	let t = engine();
	let vectorized = "UDF double_small ($x: int4): int2 { IF $x > 7 { RETURN none }; RETURN $x * 2 }; ";
	let per_row = "UDF double_small ($x: int4): int2 { LET $i = 0; WHILE $i < 100 { IF $i >= 1 { BREAK }; $i = $i + 1 }; IF $x > 7 { RETURN none }; RETURN $x * 2 }; ";

	for body in [vectorized, per_row] {
		let frames = t.query(&format!("{body}FROM test::t | sort {{ g: asc }} | map {{ x: double_small(a) }}"));

		assert_eq!(column_type(&frames, "x"), ValueType::Option(Box::new(ValueType::Int2)), "{body}");
		assert_eq!(column_strings(&frames, "x"), vec!["none", "10"], "{body}");
	}
}

#[test]
fn a_udf_declared_to_return_an_option_gives_an_optional_column_when_every_row_has_a_value() {
	// The declared Option is the exact type, so a result without a none row must not lose the Option.
	let t = engine();

	let per_row = column_type(
		&t.query(
			"UDF lo ($x: int4): Option(int4) { LET $i = 0; WHILE $i < 100 { IF $i >= $x { BREAK }; $i = $i + 1 }; RETURN $i }; FROM test::t | map { x: lo(a) }",
		),
		"x",
	);
	let vectorized = column_type(
		&t.query("UDF tw ($x: int4): Option(int2) { RETURN $x * 2 }; FROM test::t | map { x: tw(a) }"),
		"x",
	);
	let called_from_a_batch_body = column_type(
		&t.query(
			"UDF lo ($x: int4): Option(int4) { LET $i = 0; WHILE $i < 100 { IF $i >= $x { BREAK }; $i = $i + 1 }; RETURN $i }; UDF outer ($x: int4) { RETURN lo($x) }; FROM test::t | map { x: outer(a) }",
		),
		"x",
	);

	assert_eq!(
		(per_row, vectorized, called_from_a_batch_body),
		(
			ValueType::Option(Box::new(ValueType::Int4)),
			ValueType::Option(Box::new(ValueType::Int2)),
			ValueType::Option(Box::new(ValueType::Int4))
		),
		"(per-row udf, vectorized udf, per-row udf called from a vectorized body)"
	);
}

#[test]
fn a_udf_value_that_does_not_fit_its_declared_return_type_in_a_query_is_an_error_naming_the_function() {
	// Handing back the body's wider value instead would silently break the declared type every caller relies on.
	let t = engine();
	let vectorized = "UDF narrow ($x: int4): int1 { RETURN $x * 100 }; ";
	let per_row = "UDF narrow ($x: int4): int1 { LET $i = 0; WHILE $i < 100 { IF $i >= 1 { BREAK }; $i = $i + 1 }; RETURN $x * 100 }; ";

	for rql in [
		format!("{vectorized}FROM test::t | map {{ x: narrow(a) }}"),
		format!("{per_row}FROM test::t | map {{ x: narrow(a) }}"),
		format!("{vectorized}FROM test::t | filter {{ narrow(a) > 0 }}"),
		format!("{per_row}FROM test::t | extend {{ x: narrow(a) }}"),
	] {
		assert_names_function_and_declared_type(&diagnostic(&t, &rql), "narrow", "Int1");
	}

	let text = diagnostic(&t, "UDF bad ($x: int4): int4 { RETURN 'abc' }; FROM test::t | map { x: bad(a) }");
	assert_names_function_and_declared_type(&text, "bad", "Int4");
}

#[test]
fn a_udf_value_that_does_not_fit_its_declared_return_type_in_a_script_is_an_error_naming_the_function() {
	// A bare cast error names neither function nor declared type, so the caller cannot tell which call broke.
	let t = engine();

	let direct =
		diagnostic(&t, "UDF narrow ($x: int4): int1 { RETURN $x * 100 }; let $v = narrow(10); map { v: $v }");
	let vectorized_callee = diagnostic(
		&t,
		"UDF shrink ($x: int4): int1 { RETURN $x * 100 }; UDF outer ($x: int4): int8 { RETURN shrink($x) }; FROM test::t | map { x: outer(a) }",
	);
	let per_row_callee = diagnostic(
		&t,
		"UDF shrink ($x: int4): int1 { LET $i = 0; WHILE $i < 100 { IF $i >= 1 { BREAK }; $i = $i + 1 }; RETURN $x * 100 }; UDF outer ($x: int4): int8 { RETURN shrink($x) }; FROM test::t | map { x: outer(a) }",
	);

	assert_names_function_and_declared_type(&direct, "narrow", "Int1");
	assert_names_function_and_declared_type(&vectorized_callee, "shrink", "Int1");
	assert_names_function_and_declared_type(&per_row_callee, "shrink", "Int1");
}
