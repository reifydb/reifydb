// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::panic::{AssertUnwindSafe, catch_unwind};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

fn outcome(t: &TestEngine, rql: &str) -> String {
	match catch_unwind(AssertUnwindSafe(|| t.inner().query_as(TestEngine::identity(), rql, Params::None))) {
		Err(panic) => format!(
			"panic {}",
			panic.downcast_ref::<String>()
				.cloned()
				.or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
				.unwrap_or_default()
		),
		Ok(result) => match result.error {
			Some(err) => format!("error {}", err.diagnostic().code),
			None => {
				let column = result.frames[0]
					.columns
					.iter()
					.find(|c| c.name == "v")
					.unwrap_or_else(|| panic!("no column v in {rql}"));
				let values: Vec<String> =
					(0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect();
				format!("ok {values:?}")
			}
		},
	}
}

fn engine(column_type: &str, literal: &str) -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(&format!("CREATE TABLE test::t {{ x: {column_type} }}"));
	t.command(&format!("INSERT test::t [{{ x: cast('{literal}', {column_type}) }}]"));
	t
}

#[test]
fn negating_a_duration_column_gives_the_negated_duration_or_an_error_never_a_panic() {
	// Prefix minus on a duration reaches an unimplemented arm, so the statement panics instead of answering.
	let t = engine("duration", "P1D");
	let negated = outcome(&t, "FROM test::t | map { v: duration::negate(x) }");

	let result = outcome(&t, "FROM test::t | map { v: -x }");

	assert!(
		result.starts_with("error ") || result == negated,
		"-x on a duration must equal duration::negate(x) ({negated}) or be an error, got {result}"
	);
}

#[test]
fn negating_a_date_time_or_uuid_column_is_an_error_never_a_panic() {
	// These types have no negative, so prefix minus must be a type error instead of an unimplemented panic.
	let cases = [
		("date", "2024-01-02"),
		("datetime", "2024-01-02T03:04:05Z"),
		("time", "03:04:05"),
		("uuid4", "550e8400-e29b-41d4-a716-446655440000"),
		("uuid7", "01890a5d-ac96-774b-bcce-b302099a8057"),
	];

	let mut wrong = Vec::new();
	for (column_type, literal) in cases {
		let result = outcome(&engine(column_type, literal), "FROM test::t | map { v: -x }");
		if !result.starts_with("error ") {
			wrong.push((column_type, result));
		}
	}

	assert!(wrong.is_empty(), "-x on these types must be an error, got {wrong:#?}");
}
