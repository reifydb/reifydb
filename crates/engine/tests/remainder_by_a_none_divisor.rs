// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::panic::{AssertUnwindSafe, catch_unwind};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{params::Params, value::frame::frame::Frame};

fn values(frames: &[Frame], column: &str) -> String {
	let column =
		frames[0].columns.iter().find(|c| c.name == column).unwrap_or_else(|| panic!("no column {column}"));
	let values: Vec<String> = (0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect();
	format!("ok {values:?}")
}

fn query_outcome(t: &TestEngine, rql: &str, column: &str) -> String {
	match catch_unwind(AssertUnwindSafe(|| t.inner().query_as(TestEngine::identity(), rql, Params::None))) {
		Err(_) => "panic".to_string(),
		Ok(result) => match result.error {
			Some(err) => format!("error {}", err.diagnostic().code),
			None => values(&result.frames, column),
		},
	}
}

fn update_outcome(t: &TestEngine, rql: &str, table: &str, column: &str) -> String {
	match catch_unwind(AssertUnwindSafe(|| t.inner().command_as(TestEngine::identity(), rql, Params::None))) {
		Err(_) => "panic".to_string(),
		Ok(result) => match result.error {
			Some(err) => format!("error {}", err.diagnostic().code),
			None => values(&t.query(&format!("FROM {table} | sort {{ id: ASC }}")), column),
		},
	}
}

#[test]
fn a_none_divisor_yields_none_for_its_row_under_remainder_in_every_saturation_mode() {
	// A none slot holds a zero placeholder, so a remainder by it must give none, never a division by zero error.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::e { id: int4, x: int4, y: Option(int4) }");
	t.admin("CREATE TABLE test::n { id: int4, x: int4, y: Option(int4), q: Option(int4) }");
	t.admin("CREATE COLUMN PROPERTY ON test::n.q { saturation: none }");
	t.command("INSERT test::e [{ id: 1, x: 10, y: 3 }, { id: 2, x: 10, y: none }]");
	t.command(
		"INSERT test::n [{ id: 1, x: 10, y: 3, q: 99 }, { id: 2, x: 10, y: none, q: 99 }, { id: 3, x: 10, y: 0, q: 99 }]",
	);

	let error_mode = query_outcome(&t, "FROM test::e | sort { id: ASC } | map { v: x % y }", "v");
	let none_mode = update_outcome(&t, "UPDATE test::n { q: x % y } FILTER { true }", "test::n", "q");

	assert_eq!(
		(error_mode.as_str(), none_mode.as_str()),
		(r#"ok ["1", "none"]"#, r#"ok ["1", "none", "none"]"#),
		"the error mode row is the failure; the none mode zero row proves that mode is live"
	);
}
