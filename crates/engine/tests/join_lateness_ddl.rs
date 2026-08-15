// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

fn engine_with_sources(time: &str) -> TestEngine {
	// Both sides declare the same domain so only the `with { time: ... }` clause varies across the cases.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE js");
	engine.admin(&format!("CREATE TABLE js::lhs {{ id: int4, at: datetime, v: float8 }} with {{ time: {time} }}"));
	engine.admin(&format!("CREATE TABLE js::rhs {{ id: int4, at: datetime, w: float8 }} with {{ time: {time} }}"));
	engine
}

fn join_view(clause: &str) -> String {
	format!("CREATE DEFERRED VIEW js::v {{ id: int4, w: float8 }} AS {{ \
		 FROM js::lhs \
		 LEFT JOIN {{ FROM js::rhs }} AS r USING (id, r.id) WITH {{ {clause} }} \
		 MAP {{ id: id, w: r_w }} }}")
}

#[test]
fn a_right_lateness_on_a_latest_join_is_rejected() {
	// `latest` collapses the right side to one slot overwritten in place, so a right lateness would never fire.
	let engine = engine_with_sources("event(at)");

	let err = engine.admin_err(&join_view("lateness: { right: 1h }, latest: true"));

	assert!(err.contains("FLOW_048"), "expected FLOW_048, got: {err}");
	assert!(err.contains("latest"), "the rejection must name the flag that caused it, got: {err}");
}

#[test]
fn a_right_lateness_on_a_snapshot_join_is_rejected() {
	// A pinned right row must outlive the left rows referencing it, so sealing it is inert by construction.
	let engine = engine_with_sources("event(at)");

	let err = engine.admin_err(&join_view("lateness: { right: 1h }, snapshot: true"));

	assert!(err.contains("FLOW_048"), "expected FLOW_048, got: {err}");
	assert!(err.contains("snapshot"), "the rejection must name the flag that caused it, got: {err}");
}

#[test]
fn a_left_lateness_on_a_latest_join_is_accepted() {
	// The rule must key off the right side only, or it rejects the one lateness these joins can honour.
	let engine = engine_with_sources("event(at)");

	engine.admin(&join_view("lateness: { left: 1h }, latest: true"));
}

#[test]
fn a_left_lateness_on_a_snapshot_join_is_accepted() {
	// Without this the check could reject every flagged join and the right-side tests would still pass.
	let engine = engine_with_sources("event(at)");

	engine.admin(&join_view("lateness: { left: 1h }, snapshot: true"));
}

#[test]
fn a_join_lateness_over_a_processing_time_source_is_rejected() {
	// A lateness frees a row once the watermark passes its event time, which a processing-time flow never supplies.
	let engine = engine_with_sources("processing");

	let err = engine.admin_err(&join_view("lateness: { left: 1h }"));

	assert!(err.contains("FLOW_049"), "expected FLOW_049, got: {err}");
}

#[test]
fn a_join_lateness_over_an_event_time_source_is_accepted() {
	// Without this the validation could reject everything and the test above would still pass.
	let engine = engine_with_sources("event(at)");

	engine.admin(&join_view("lateness: { left: 1h }"));
}

#[test]
fn a_join_without_a_lateness_over_a_processing_time_source_is_accepted() {
	// The rule must key off the lateness, never off joins as a whole.
	let engine = engine_with_sources("processing");

	engine.admin(&join_view("snapshot: true"));
}
