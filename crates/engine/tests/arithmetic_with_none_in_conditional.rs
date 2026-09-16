// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

#[test]
fn a_number_plus_none_joins_a_number_branch_of_a_conditional() {
	// A conditional accepts a none branch beside any type, so a none from arithmetic must never carry a boolean
	// type.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4 }");
	t.command("INSERT test::t [{ a: 5 }, { a: 10 }]");

	let result = t.inner().query_as(
		TestEngine::identity(),
		"FROM test::t | sort { a: ASC } | map { v: if a > 7 { 2 + none } else { 3 } }",
		Params::None,
	);

	if let Some(err) = result.error {
		panic!("the conditional must accept the none branch, got {:?}", err.diagnostic());
	}
	let column = result.frames[0].columns.iter().find(|c| c.name == "v").expect("column v");
	let values: Vec<String> = (0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect();
	assert_eq!(values, ["3", "none"]);
}

#[test]
fn a_number_and_none_under_every_arithmetic_operator_is_that_number_type_in_either_order() {
	// An untyped none must adopt the other operand's type, otherwise the result is a boolean no number can join.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4 }");
	t.command("INSERT test::t [{ a: 5 }, { a: 10 }]");
	let per_row = "LET $i = 0; WHILE $i < 100 { IF $i >= 1 { BREAK }; $i = $i + 1 };";

	for op in ["+", "-", "*", "/", "%"] {
		for (left, right) in [("{n}", "none"), ("none", "{n}")] {
			let expression =
				|n: &str| format!("{} {op} {}", left.replace("{n}", n), right.replace("{n}", n));
			let cases = [
				(format!("FROM test::t | map {{ v: {} }}", expression("a")), "Option(Int4)"),
				(format!("map {{ v: {} }}", expression("2")), "Option(Int1)"),
				(format!("LET $r = {}; map {{ v: $r }}", expression("2")), "Option(Int1)"),
				(
					format!(
						"UDF m ($x) {{ RETURN {} }}; FROM test::t | map {{ v: m(a) }}",
						expression("$x")
					),
					"Option(Int4)",
				),
				(
					format!(
						"UDF m ($x) {{ {per_row} RETURN {} }}; FROM test::t | map {{ v: m(a) }}",
						expression("$x")
					),
					"Option(Int4)",
				),
			];

			for (rql, expected) in cases {
				let (value_type, values) = column_v(&t, &rql);

				assert_eq!(value_type, expected, "{rql}");
				assert!(values.iter().all(|v| v == "none"), "{rql}: {values:?}");
			}
		}
	}
}

#[test]
fn a_comparison_with_none_stays_an_optional_boolean_in_either_order() {
	// Only arithmetic adopts the number type, a comparison yields a boolean whatever its operands are.
	let t = TestEngine::new();

	for op in ["==", "!="] {
		for rql in [format!("map {{ v: 2 {op} none }}"), format!("map {{ v: none {op} 2 }}")] {
			assert_eq!(column_v(&t, &rql).0, "Option(Boolean)", "{rql}");
		}
	}
}

fn column_v(t: &TestEngine, rql: &str) -> (String, Vec<String>) {
	let result = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	if let Some(err) = result.error {
		panic!("{rql} must run, got {:?}", err.diagnostic());
	}
	let column = result.frames[0].columns.iter().find(|c| c.name == "v").expect("column v");
	let values = (0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect();
	(column.data.get_type().to_string(), values)
}
