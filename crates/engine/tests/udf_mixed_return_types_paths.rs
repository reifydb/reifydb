// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::panic::{AssertUnwindSafe, catch_unwind};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

const LOOP: &str = "LET $i = 0; WHILE $i < 100 { IF $i >= 1 { BREAK }; $i = $i + 1 };";

fn engine(rows: &[i32]) -> TestEngine {
	// A udf body calling another udf only takes the batch paths over at least two rows.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4 }");
	for a in rows {
		t.command(&format!("INSERT test::t [{{ a: {a} }}]"));
	}
	t
}

fn outcome(t: &TestEngine, rql: &str) -> String {
	// A panic is an outcome too, otherwise one crashing path would hide how the others behave.
	match catch_unwind(AssertUnwindSafe(|| t.inner().query_as(TestEngine::identity(), rql, Params::None))) {
		Err(_) => "panic".to_string(),
		Ok(result) => match result.error {
			Some(err) => {
				let diagnostic = err.diagnostic();
				format!("error {} at {:?}", diagnostic.code, diagnostic.fragment.text())
			}
			None => {
				let column = result.frames[0].columns.iter().find(|c| c.name == "v").expect("column v");
				let values: Vec<String> =
					(0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect();
				format!("ok {} {:?}", column.data.get_type(), values)
			}
		},
	}
}

fn described_results(t: &TestEngine, rql: &str) -> Vec<String> {
	let result = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	let diagnostic = result.error.expect("mixed result types must fail").diagnostic();
	assert_eq!(diagnostic.code, "RUNTIME_012", "{diagnostic:?}");
	let mut described: Vec<String> = diagnostic
		.notes
		.iter()
		.filter_map(|note| note.split_once("yields: ").map(|(_, d)| d.to_string()))
		.collect();
	described.sort();
	described
}

fn paths(return_type: &str, body: &str) -> Vec<(&'static str, String)> {
	// Rows are sorted before the call so the values line up the same way on every path.
	let rows = "FROM test::t | sort { a: ASC } | map";
	let vectorized = format!("UDF m ($x){return_type} {{ {body} }}; ");
	let per_row = format!("UDF m ($x){return_type} {{ {LOOP} {body} }}; ");
	let outer = "UDF outer ($y) { RETURN m($y) }; ";
	let mut paths = vec![
		("vectorized udf over rows", format!("{vectorized}{rows} {{ v: m(a) }}")),
		("per-row udf over rows", format!("{per_row}{rows} {{ v: m(a) }}")),
		("vectorized udf called from a udf body", format!("{vectorized}{outer}{rows} {{ v: outer(a) }}")),
		("per-row udf called from a udf body", format!("{per_row}{outer}{rows} {{ v: outer(a) }}")),
	];
	if return_type.is_empty() {
		paths.push((
			"vectorized closure over rows",
			format!("let $m = ($x) {{ {body} }}; {rows} {{ v: $m(a) }}"),
		));
		paths.push((
			"per-row closure over rows",
			format!("let $m = ($x) {{ {LOOP} {body} }}; {rows} {{ v: $m(a) }}"),
		));
	}
	paths
}

#[test]
fn an_untyped_udf_giving_text_for_one_row_and_an_int_for_another_is_the_conditional_error_at_the_call() {
	// Every body shape that yields mixed types must fail like an if with mixed branches, never panic or stringify.
	let t = engine(&[5, 10]);
	let bodies = [
		"IF $x > 7 { RETURN 'big' }; RETURN 1",
		"IF $x > 7 { RETURN 1 }; RETURN 'big'",
		"RETURN if $x > 7 { 'big' } else { 1 }",
		"IF $x > 7 { 'big' } ELSE { 1 }",
		"IF $x > 7 { RETURN 1 }; IF $x > 6 { RETURN 2 }; RETURN 300",
		"RETURN 2 + (if $x > 7 { 1 } else { 300 })",
	];

	for body in bodies {
		for (path, rql) in paths("", body) {
			let function = if path.contains("closure") {
				"$m"
			} else {
				"m"
			};

			assert_eq!(
				outcome(&t, &rql),
				format!("error RUNTIME_012 at {function:?}"),
				"{path}, body {body}"
			);
		}
	}
}

#[test]
fn the_mixed_type_error_describes_both_results_under_the_function_name_on_every_path() {
	// The notes must name the called function, never an internal column like const or vm_add.
	let t = engine(&[5, 10]);

	for (path, rql) in paths("", "IF $x > 7 { RETURN 'big' }; RETURN 1") {
		let function = if path.contains("closure") {
			"$m"
		} else {
			"m"
		};

		assert_eq!(
			described_results(&t, &rql),
			[format!("[{function}: Int1]"), format!("[{function}: Utf8]")],
			"{path}"
		);
	}
	for (path, rql) in paths("", "RETURN 2 + (if $x > 7 { 1 } else { 300 })") {
		let function = if path.contains("closure") {
			"$m"
		} else {
			"m"
		};

		let described = described_results(&t, &rql);
		assert!(
			described.len() == 2 && described.iter().all(|d| d.starts_with(&format!("[{function}: "))),
			"{path}: {described:?}"
		);
	}
}

#[test]
fn an_untyped_udf_in_a_filter_giving_mixed_types_is_the_conditional_error_at_the_call() {
	// A filter hoists the call like a map does, so it must never compare against a silently widened column.
	let t = engine(&[5, 10]);

	for body in [
		"IF $x > 7 { RETURN 'big' }; RETURN 1".to_string(),
		format!("{LOOP} IF $x > 7 {{ RETURN 'big' }}; RETURN 1"),
	] {
		let outcome = outcome(&t, &format!("UDF m ($x) {{ {body} }}; FROM test::t | filter {{ m(a) == 1 }}"));

		assert_eq!(outcome, "error RUNTIME_012 at \"m\"", "body {body}");
	}
}

#[test]
fn a_none_return_pairs_with_an_int_return_in_either_order_on_every_path() {
	// A conditional accepts a none branch beside any type, so a udf must neither reject nor crash on it.
	let t = engine(&[5, 10]);
	let bodies = [
		("IF $x > 7 { RETURN none }; RETURN 1", "[\"1\", \"none\"]"),
		("IF $x > 7 { RETURN 1 }; RETURN none", "[\"none\", \"1\"]"),
		("RETURN if $x > 7 { none } else { 1 }", "[\"1\", \"none\"]"),
		("RETURN if $x > 7 { 1 } else { none }", "[\"none\", \"1\"]"),
	];

	for (body, values) in bodies {
		for (path, rql) in paths("", body) {
			assert_eq!(outcome(&t, &rql), format!("ok Option(Int1) {values}"), "{path}, body {body}");
		}
	}
}

#[test]
fn returns_of_one_type_from_different_branches_keep_that_type_on_every_path() {
	// The check must compare types, never values, otherwise two text returns from two branches would be rejected.
	let t = engine(&[5, 10]);

	for (path, rql) in paths("", "IF $x > 7 { RETURN 'big' }; RETURN 'small'") {
		assert_eq!(outcome(&t, &rql), "ok Utf8 [\"small\", \"big\"]", "{path}");
	}
}

#[test]
fn an_untyped_per_row_udf_keeps_the_type_it_returns_for_every_row_count() {
	// The column type must come from the values, otherwise each extra row widens int4 one step further.
	for row_count in 1..=4 {
		let rows: Vec<i32> = (1..=row_count).collect();
		let t = engine(&rows);
		let values: Vec<String> = rows.iter().map(|a| a.to_string()).collect();

		for (path, rql) in paths("", "RETURN $x").into_iter().filter(|(path, _)| path.starts_with("per-row")) {
			assert_eq!(outcome(&t, &rql), format!("ok Int4 {values:?}"), "{path}, {row_count} rows");
		}
	}
}

#[test]
fn a_typed_udf_adding_to_a_conditional_with_mixed_branch_types_never_panics_on_any_path() {
	// A declared return type must not skip the branch check, otherwise the batch merge of int1 and int2 panics.
	let t = engine(&[5, 10]);

	for (path, rql) in paths(": int4", "RETURN 2 + (if $x > 7 { 1 } else { 300 })") {
		let outcome = outcome(&t, &rql);

		assert!(
			outcome == "error RUNTIME_012 at \"m\"" || outcome == "ok Int4 [\"302\", \"3\"]",
			"{path}: expected the conditional error or the per-row sums, got {outcome}"
		);
	}
}

#[test]
fn an_untyped_udf_whose_variable_holds_text_on_one_row_and_an_int_on_another_fails_on_every_path() {
	// The per-row paths reject the mixed result, so the batch paths must not merge the variable into text.
	let t = engine(&[5, 10]);

	let outcomes: Vec<(&str, String)> = paths("", "LET $v = 1; IF $x > 7 { $v = 'big' }; RETURN $v")
		.iter()
		.map(|(path, rql)| (*path, outcome(&t, rql)))
		.collect();

	assert!(
		outcomes.iter().all(|(_, outcome)| outcome.starts_with("error RUNTIME_012")),
		"every path must reject the mixed result, got {outcomes:#?}"
	);
}

#[test]
fn a_variable_assigned_one_type_under_a_condition_keeps_that_type_on_every_path() {
	// A masked store must merge by the value types, never widen int1 to int2 or fail to pair a none with an int.
	let t = engine(&[5, 10]);

	for (body, expected) in [
		("LET $v = 1; IF $x > 7 { $v = 2 }; RETURN $v", "ok Int1 [\"1\", \"2\"]"),
		("LET $v = none; IF $x > 7 { $v = 1 }; RETURN $v", "ok Option(Int1) [\"none\", \"1\"]"),
		("LET $v = 1; IF $x > 7 { $v = none }; RETURN $v", "ok Option(Int1) [\"1\", \"none\"]"),
	] {
		for (path, rql) in paths("", body) {
			assert_eq!(outcome(&t, &rql), expected, "{path}, body {body}");
		}
	}
}

#[test]
fn a_masked_store_of_another_type_is_the_conditional_error_under_the_function_name_on_every_path() {
	// The store error must carry the call fragment and both types, exactly like the per-row result check.
	let t = engine(&[5, 10]);

	for (path, rql) in paths("", "LET $v = 1; IF $x > 7 { $v = 'big' }; RETURN $v") {
		let function = if path.contains("closure") {
			"$m"
		} else {
			"m"
		};

		assert_eq!(outcome(&t, &rql), format!("error RUNTIME_012 at {function:?}"), "{path}");
		assert_eq!(
			described_results(&t, &rql),
			[format!("[{function}: Int1]"), format!("[{function}: Utf8]")],
			"{path}"
		);
	}
}

#[test]
fn a_typed_udf_storing_a_wider_int_into_a_variable_returns_its_declared_type_on_every_path() {
	// A declared return type casts the result, so the store check must not reject int8 over int4 there.
	let t = engine(&[5, 10]);

	for (path, rql) in paths(": utf8", "LET $v = $x; IF $x > 7 { $v = $x + 1 }; RETURN $v") {
		assert_eq!(outcome(&t, &rql), "ok Utf8 [\"5\", \"11\"]", "{path}");
	}
}
