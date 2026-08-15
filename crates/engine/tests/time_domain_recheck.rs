// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

fn engine_with_source(time: &str) -> TestEngine {
	// The column list is identical across domains so only the `with { time: ... }` clause varies.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE td");
	engine.admin(&format!("CREATE TABLE td::src {{ id: int4, at: datetime, v: float8 }} with {{ time: {time} }}"));
	engine
}

fn rolling_view(window: &str) -> String {
	format!("CREATE DEFERRED VIEW td::v {{ id: int4, total: float8 }} AS {{ \
		 FROM td::src | window rolling {{ total: math::sum(v) }} with {{ {window} }} by {{ id }} }}")
}

#[test]
fn a_lagged_rolling_window_over_a_processing_time_source_is_rejected() {
	// `lag` shifts the window backwards along the timeline, which is meaningless when the timeline came from the
	// ingest clock.
	let engine = engine_with_source("processing");

	let err = engine.admin_err(&rolling_view("duration: 1m, lag: 1m"));

	assert!(err.contains("FLOW_043"), "expected FLOW_043, got: {err}");
}

#[test]
fn a_lagged_rolling_window_over_an_event_time_source_is_accepted() {
	// Without this the validation could reject everything and the test above would still pass.
	let engine = engine_with_source("event(at)");

	engine.admin(&rolling_view("duration: 1m, lag: 1m"));
}

#[test]
fn a_rolling_window_without_lag_over_a_processing_time_source_is_accepted() {
	// The rule must key off `lag`, never off rolling windows as a whole.
	let engine = engine_with_source("processing");

	engine.admin(&rolling_view("duration: 1m"));
}
