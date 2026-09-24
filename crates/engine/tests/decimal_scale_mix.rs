// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	params::Params,
	value::{Value, decimal::Decimal, frame::frame::Frame},
};

const PER_ROW: &str = "LET $i = 0; WHILE $i < 100 { IF $i >= 1 { BREAK }; $i = $i + 1 };";

fn command(t: &TestEngine, rql: &str, params: Params) -> Vec<Frame> {
	let r = t.inner().command_as(TestEngine::identity(), rql, params);
	if let Some(e) = r.error {
		panic!("command failed: {e:?}\nrql: {rql}")
	}
	r.frames
}

fn texts(frames: &[Frame], name: &str) -> Vec<String> {
	let column = frames.last().unwrap().columns.iter().find(|c| c.name == name).expect("column");
	(0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect()
}

fn table_of_two_rows() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4 }");
	t.command("INSERT test::t [{ a: 5 }, { a: 10 }]");
	t
}

#[test]
fn appending_a_finer_decimal_scale_to_a_variable_keeps_every_value() {
	// The variable column is decimal(76, 1), so 1.25 needs 77 digits and the builder panics instead of widening.
	let t = TestEngine::new();
	let frames = command(
		&t,
		"append $x from [{ v: 1 }, { v: 1.5 }]; append $x from [{ v: 1.25 }]; from $x",
		Params::None,
	);
	assert_eq!(texts(&frames, "v"), vec!["1.00", "1.50", "1.25"]);
}

#[test]
fn a_for_loop_over_decimals_of_mixed_scale_runs_once_per_item() {
	// Rows are built from the first item's decimal(76, 1), so the second item's 1.25 does not fit and panics.
	let t = TestEngine::new();
	let ids = Value::List(vec![
		Value::Decimal(Decimal::parse("1.5").unwrap()),
		Value::Decimal(Decimal::parse("1.25").unwrap()),
	]);
	let params = Params::from(HashMap::from([("ids".to_string(), ids)]));
	let frames = command(&t, "let $n = 0; for $v in $ids { $n = $n + 1 }; map { n: $n }", params);
	assert_eq!(texts(&frames, "n"), vec!["2"]);
}

fn udf_rows(t: &TestEngine, prefix: &str, five: &str, ten: &str) -> Vec<(String, String)> {
	let rql = format!(
		"UDF m ($x) {{ {prefix} IF $x == 5 {{ RETURN {five} }} ELSE {{ RETURN {ten} }} }}; FROM test::t | map {{ a: a, v: m(a) }}"
	);
	let frames = command(t, &rql, Params::None);
	let mut rows: Vec<(String, String)> = texts(&frames, "a").into_iter().zip(texts(&frames, "v")).collect();
	rows.sort();
	rows
}

#[test]
fn a_per_row_udf_returning_decimals_of_mixed_scale_keeps_every_value() {
	// The untyped result column takes the first row's decimal type, so a finer scale in a later row does not fit and panics.
	let t = table_of_two_rows();
	for (five, ten) in [("1.5", "1.25"), ("1.25", "1.5")] {
		let rows = udf_rows(&t, PER_ROW, five, ten);
		assert_eq!(rows, vec![("10".to_string(), format!("{ten:0<4}")), ("5".to_string(), format!("{five:0<4}"))]);
	}
}

#[test]
fn a_vectorized_udf_whose_branches_return_decimals_of_mixed_scale_keeps_every_value() {
	// The branch merge builds from one branch's decimal type and pushes the other's finer scale, which panics.
	let t = table_of_two_rows();
	for (five, ten) in [("1.5", "1.25"), ("1.25", "1.5")] {
		let rows = udf_rows(&t, "", five, ten);
		assert_eq!(rows, vec![("10".to_string(), format!("{ten:0<4}")), ("5".to_string(), format!("{five:0<4}"))]);
	}
}
