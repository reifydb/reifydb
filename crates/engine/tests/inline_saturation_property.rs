// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::panic::{AssertUnwindSafe, catch_unwind};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

fn update_outcome(t: &TestEngine, rql: &str, table: &str) -> String {
	match catch_unwind(AssertUnwindSafe(|| t.inner().command_as(TestEngine::identity(), rql, Params::None))) {
		Err(_) => "panic".to_string(),
		Ok(result) => match result.error {
			Some(err) => format!("error {}", err.diagnostic().code),
			None => {
				let frames = t.query(&format!("FROM {table} | sort {{ id: ASC }}"));
				let column = frames[0].columns.iter().find(|c| c.name == "q").expect("column q");
				let values: Vec<String> =
					(0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect();
				format!("ok {values:?}")
			}
		},
	}
}

#[test]
fn inline_saturation_none_takes_effect_or_is_rejected_never_silently_dropped() {
	// An accepted column property must act like its standalone form, or the table silently stays in error mode.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::standalone { id: int4, x: int4, y: int4, q: Option(int4) }");
	t.admin("CREATE COLUMN PROPERTY ON test::standalone.q { saturation: none }");
	t.command("INSERT test::standalone [{ id: 1, x: 10, y: 0, q: 99 }]");
	let standalone = update_outcome(&t, "UPDATE test::standalone { q: x / y } FILTER { true }", "test::standalone");

	let created = t.inner().admin_as(
		TestEngine::identity(),
		"CREATE TABLE test::inline { id: int4, x: int4, y: int4, q: Option(int4) with { saturation: none } }",
		Params::None,
	);
	if created.error.is_some() {
		return;
	}
	t.command("INSERT test::inline [{ id: 1, x: 10, y: 0, q: 99 }]");
	let inline = update_outcome(&t, "UPDATE test::inline { q: x / y } FILTER { true }", "test::inline");

	assert_eq!(
		(standalone.as_str(), inline.as_str()),
		(r#"ok ["none"]"#, r#"ok ["none"]"#),
		"the standalone row proves none mode is live; the inline row must match it"
	);
}
