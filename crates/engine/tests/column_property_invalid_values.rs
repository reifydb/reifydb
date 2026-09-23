// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::panic::{AssertUnwindSafe, catch_unwind};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

fn property_outcome(property: &str) -> String {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, q: Option(int4) }");
	let rql = format!("CREATE COLUMN PROPERTY ON test::t.q {{ {property} }}");
	match catch_unwind(AssertUnwindSafe(|| t.inner().admin_as(TestEngine::identity(), &rql, Params::None))) {
		Err(_) => "panic".to_string(),
		Ok(result) => match result.error {
			Some(err) => format!("error {}", err.diagnostic().code),
			None => "ok".to_string(),
		},
	}
}

#[test]
fn a_standalone_column_default_is_rejected_with_an_error() {
	// No default machinery exists, so accepting it would drop the value silently and a panic would kill the server.
	let outcome = property_outcome("default: 1");

	assert!(outcome.starts_with("error "), "a column default must be an error, got {outcome}");
}

#[test]
fn an_unknown_saturation_word_is_rejected_with_an_error() {
	// Only none and error are strategies; any other word must never panic or fall back to a strategy.
	let outcome = property_outcome("saturation: foo");

	assert!(outcome.starts_with("error "), "an unknown saturation word must be an error, got {outcome}");
}

#[test]
fn a_non_identifier_saturation_value_is_rejected_with_an_error() {
	// A literal must be checked before it is read as an identifier, otherwise the planner panics.
	let outcome = property_outcome("saturation: 1");

	assert!(outcome.starts_with("error "), "a numeric saturation value must be an error, got {outcome}");
}
