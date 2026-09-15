// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{params::Params, value::Value};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin("CREATE SERIES s::k { k: int8, val: float8 } WITH { key: k }");
	t.admin("CREATE SERIES s::x { ts: datetime, val: float8 } WITH { key: ts }");
	t
}

#[test]
fn inserting_a_negative_integer_key_into_a_series_stores_it_or_fails_never_a_generated_key() {
	// A key the series cannot order must fail loud, never be swapped for the next generated key.
	let t = engine();

	let r = t.inner().command_as(TestEngine::identity(), "INSERT s::k [{ k: -5, val: 1.0 }]", Params::None);

	let err = r.error.expect("a negative series key must be refused").diagnostic();
	assert_eq!(err.code, "SERIES_001", "{err:?}");
	assert_eq!(err.fragment.text(), "-5", "{err:?}");
}

#[test]
fn bulk_inserting_a_negative_integer_key_into_a_series_stores_it_or_fails_never_key_zero() {
	// A key the series cannot order must fail loud, never be stored as key 0.
	let t = engine();
	let mut builder = t.bulk_insert(TestEngine::identity());
	builder.series("s::k")
		.row(Params::from(HashMap::from([
			("k".to_string(), Value::Int8(-3)),
			("val".to_string(), Value::float8(1.0)),
		])))
		.done();

	let err = match builder.execute() {
		Err(e) => e.diagnostic(),
		Ok(_) => panic!("a negative series key must be refused"),
	};
	assert_eq!(err.code, "SERIES_001", "{err:?}");
	assert_eq!(err.fragment.text(), "-3", "{err:?}");
}

#[test]
fn updating_the_key_of_a_series_row_moves_the_row_or_fails_never_updates_nothing() {
	// The key addresses the stored row, so a changed key must move the row or fail loud, never report 0.
	let t = engine();
	t.command("INSERT s::x [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1.0 }]");

	let r = t.inner().command_as(
		TestEngine::identity(),
		"UPDATE s::x { ts: cast('2025-01-01T00:00:00Z', datetime) } FILTER { val == 1.0 }",
		Params::None,
	);

	let err = r.error.expect("an update of the series key must be refused").diagnostic();
	assert_eq!(err.code, "UPDATE_005", "{err:?}");
	assert_eq!(err.fragment.text(), "2025-01-01T00:00:00Z", "{err:?}");
}
