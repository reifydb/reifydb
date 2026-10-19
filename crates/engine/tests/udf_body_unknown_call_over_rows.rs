// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

fn error(t: &TestEngine, rql: &str) -> (String, String) {
	let result = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	let diagnostic = result.error.unwrap_or_else(|| panic!("{rql}: expected an error")).diagnostic();
	(diagnostic.code, diagnostic.message)
}

#[test]
fn an_unknown_function_inside_a_body_over_rows_reports_the_same_unknown_callable_as_on_one_row() {
	// A misspelled call must name the unknown callable on every path, never surface as an internal error.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, a: int4 }");
	t.command("INSERT test::t [{ id: 1, a: 1 }, { id: 2, a: 7 }]");
	for (body, name) in
		[("len(\"abcde\")", "len"), ("$x > len(\"abcde\")", "len"), ("nope::thing($x)", "nope::thing")]
	{
		let udf = format!("UDF m ($x) {{ RETURN {body} }};");
		let one_row = error(&t, &format!("{udf} map {{ v: m(1) }}"));
		let over_rows = error(&t, &format!("{udf} FROM test::t | map {{ v: m(a) }}"));
		assert_eq!(one_row, ("CALLABLE_001".to_string(), format!("Unknown callable: {name}")), "{body}");
		assert_eq!(over_rows, one_row, "{body}");
	}
}
