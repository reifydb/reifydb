// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::panic::{AssertUnwindSafe, catch_unwind};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

fn write_outcome(t: &TestEngine, rql: &str, source: &str, column: &str) -> String {
	match catch_unwind(AssertUnwindSafe(|| t.inner().command_as(TestEngine::identity(), rql, Params::None))) {
		Err(_) => "panic".to_string(),
		Ok(result) => match result.error {
			Some(err) => format!("error {}", err.diagnostic().code),
			None => {
				let frames = t.query(&format!("FROM {source} | sort {{ id: ASC }}"));
				let data = &frames[0].columns.iter().find(|c| c.name == column).expect("column").data;
				let values: Vec<String> =
					(0..data.len()).map(|i| data.get_value(i).to_string()).collect();
				format!("ok {values:?}")
			}
		},
	}
}

#[test]
fn inline_default_takes_effect_or_is_rejected_never_silently_dropped() {
	// An accepted inline default that is ignored leaves the omitted column without the value the DDL promised.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	let created = t.inner().admin_as(
		TestEngine::identity(),
		"CREATE TABLE test::t { id: int4, x: int4 with { default: 7 } }",
		Params::None,
	);
	if created.error.is_some() {
		return;
	}
	let inserted = write_outcome(&t, "INSERT test::t [{ id: 1 }]", "test::t", "x");

	assert_eq!(inserted, r#"ok ["7"]"#, "an insert without x must store the inline default");
}

#[test]
fn inline_saturation_none_on_a_ringbuffer_takes_effect_or_is_rejected_never_silently_dropped() {
	// An accepted ringbuffer property must act like its standalone table form, or the column stays in error mode.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::standalone { id: int4, x: int4, y: int4, q: Option(int4) }");
	t.admin("CREATE COLUMN PROPERTY ON test::standalone.q { saturation: none }");
	t.command("INSERT test::standalone [{ id: 1, x: 10, y: 0, q: 99 }]");
	let standalone =
		write_outcome(&t, "UPDATE test::standalone { q: x / y } FILTER { true }", "test::standalone", "q");

	let created = t.inner().admin_as(
		TestEngine::identity(),
		"CREATE RINGBUFFER test::rb { id: int4, x: int4, y: int4, q: Option(int4) with { saturation: none } } WITH { capacity: 10 }",
		Params::None,
	);
	if created.error.is_some() {
		return;
	}
	t.command("INSERT test::rb [{ id: 1, x: 10, y: 0, q: 99 }]");
	let inline = write_outcome(&t, "UPDATE test::rb { q: x / y } FILTER { true }", "test::rb", "q");

	assert_eq!(
		(standalone.as_str(), inline.as_str()),
		(r#"ok ["none"]"#, r#"ok ["none"]"#),
		"the standalone row proves none mode is live; the ringbuffer row must match it"
	);
}
