// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// This file used to hold three tests for `reconcile_time_domain`: a flow declared its own time
// domain, its source declared one too, and the pair could drift apart between DDL and restart.
// Flows no longer declare anything - #time is set once at the source and read everywhere
// downstream - so a flow-vs-source disagreement is unrepresentable and those three assertions have
// no subject left.
//
// What survives is the rule they shared a function with: a rolling window with `lag` shifts a
// window backwards along the timeline, which only means something when the timeline came from the
// data rather than from the ingest clock. Deleting the flow declaration would have deleted that
// rule silently, so it was carved out into `check_window_time_requirements`, backed by a
// `source_time_domain` that still has to be written.
//
// The test below is a tripwire, not a behaviour assertion. It pins that the validation is still
// REACHED on the definition path. Once `source_time_domain` is implemented it stops panicking and
// this test fails with "did not panic", which is the signal to replace it with the real
// assertions: a lagged rolling window over a processing-time source is rejected, and over an
// event-time source is accepted.

use reifydb_engine::test_harness::TestEngine;

#[test]
#[should_panic(expected = "walking to its sources")]
fn a_lagged_rolling_window_still_reaches_the_time_domain_validation() {
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE td");
	engine.admin("CREATE TABLE td::src { id: int4, at: datetime, v: float8 } with { time: processing }");
	engine.admin("CREATE DEFERRED VIEW td::v { id: int4, total: float8 } AS { \
		 FROM td::src | window rolling { total: math::sum(v) } \
		 with { interval: \"1m\", lag: \"1m\" } by { id } }");
}
