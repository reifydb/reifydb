// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

fn first_value(t: &TestEngine, rql: &str) -> String {
	let result = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	if let Some(err) = result.error {
		return format!("error {}", err.diagnostic().code);
	}
	let column = result.frames[0].columns.iter().find(|c| c.name == "v").unwrap_or_else(|| panic!("no v in {rql}"));
	column.data.get_value(0).to_string()
}

#[test]
fn in_with_a_none_item_gives_the_same_kleene_answer_on_the_compiled_and_the_vm_path() {
	// An in list is an or of equalities, so a match must win over a none item and a miss must stay none.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4 }");
	t.command("INSERT test::t [{ a: 1 }, { a: 2 }]");

	let cases = [("1", "", "true"), ("2", "", "none"), ("1", "not ", "false"), ("2", "not ", "none")];
	let mut observed = Vec::new();
	let mut expected = Vec::new();
	for (probe, not, answer) in cases {
		let condition = format!("{probe} {not}in [1, none]");
		let queries = [
			format!("map {{ v: {condition} }}"),
			format!("FROM test::t | filter {{ a == {probe} }} | map {{ v: a {not}in [1, none] }}"),
			format!("LET $r = {condition}; map {{ v: $r }}"),
		];
		for rql in queries {
			observed.push((rql.clone(), first_value(&t, &rql)));
			expected.push((rql, answer.to_string()));
		}
	}

	assert_eq!(observed, expected);
}
