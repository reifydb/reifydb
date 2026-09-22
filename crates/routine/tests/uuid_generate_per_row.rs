// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashSet,
	panic::{AssertUnwindSafe, catch_unwind},
};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

fn outcome(t: &TestEngine, rql: &str) -> Result<Vec<String>, String> {
	match catch_unwind(AssertUnwindSafe(|| t.inner().query_as(TestEngine::identity(), rql, Params::None))) {
		Err(panic) => Err(format!(
			"panic {}",
			panic.downcast_ref::<String>()
				.cloned()
				.or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
				.unwrap_or_default()
		)),
		Ok(result) => match result.error {
			Some(err) => Err(format!("error {}", err.diagnostic().code)),
			None => {
				let column = result.frames[0]
					.columns
					.iter()
					.find(|c| c.name == "v")
					.unwrap_or_else(|| panic!("no column v in {rql}"));
				Ok((0..column.data.len()).map(|i| column.data.get_value(i).to_string()).collect())
			}
		},
	}
}

fn three_rows() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4 }");
	t.command("INSERT test::t [{ id: 1 }, { id: 2 }, { id: 3 }]");
	t
}

#[test]
fn a_zero_argument_uuid_in_a_map_gives_one_fresh_uuid_per_row() {
	// A generator that ignores the batch size returns one value for many rows, so the map breaks its row count.
	let t = three_rows();

	let mut wrong = Vec::new();
	for function in ["uuid::v4()", "uuid::v7()"] {
		let result = outcome(&t, &format!("FROM test::t | map {{ v: {function} }}"));
		let distinct = result.as_ref().map(|values| values.iter().collect::<HashSet<_>>().len()).unwrap_or(0);
		if distinct != 3 {
			wrong.push((function, result));
		}
	}

	assert!(wrong.is_empty(), "each of the 3 rows must get its own uuid, got {wrong:#?}");
}
