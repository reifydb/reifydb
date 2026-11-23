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
fn a_right_forget_on_a_latest_join_is_accepted() {
	// `latest` keeps every right row per key, so a right forget is the only thing bounding that growth.
	let engine = engine_with_sources("event(at)");

	engine.admin(&join_view("forget: { right: 1h }, latest: true"));
}

#[test]
fn a_right_forget_on_a_snapshot_join_is_accepted() {
	// A sealed right row stashes its bytes into the pin first, so the pairs it published survive without it.
	let engine = engine_with_sources("event(at)");

	engine.admin(&join_view("forget: { right: 1h }, emit: false"));
}

#[test]
fn a_left_forget_on_a_latest_join_is_accepted() {
	// The rule must key off the right side only, or it rejects the one forget these joins can honour.
	let engine = engine_with_sources("event(at)");

	engine.admin(&join_view("forget: { left: 1h }, latest: true"));
}

#[test]
fn a_left_forget_on_a_snapshot_join_is_accepted() {
	// Without this the check could reject every flagged join and the right-side tests would still pass.
	let engine = engine_with_sources("event(at)");

	engine.admin(&join_view("forget: { left: 1h }, emit: false"));
}

#[test]
fn a_join_forget_over_a_processing_time_source_is_rejected() {
	// A forget frees a row once the flow watermark passes its event time, which a processing-time flow never
	// supplies.
	let engine = engine_with_sources("processing");

	let err = engine.admin_err(&join_view("forget: { left: 1h }"));

	assert!(err.contains("FLOW_049"), "expected FLOW_049, got: {err}");
}

#[test]
fn a_join_forget_over_an_event_time_source_is_accepted() {
	// Without this the validation could reject everything and the test above would still pass.
	let engine = engine_with_sources("event(at)");

	engine.admin(&join_view("forget: { left: 1h }"));
}

#[test]
fn a_join_without_a_forget_over_a_processing_time_source_is_accepted() {
	// The rule must key off the forget, never off joins as a whole.
	let engine = engine_with_sources("processing");

	engine.admin(&join_view("emit: false"));
}

#[test]
fn a_join_with_a_drop_below_its_forget_is_accepted() {
	// A drop cuts late arrivals sooner than the stored rows are freed, so the pair must never be refused.
	let engine = engine_with_sources("event(at)");

	engine.admin(&join_view("drop: { left: 20s }, forget: { left: 1h }"));
}

#[test]
fn a_join_forget_below_the_drop_of_its_side_is_rejected() {
	// A row freed before its drop passes would be gone while changes for it are still accepted.
	let engine = engine_with_sources("event(at)");

	let err = engine.admin_err(&join_view("drop: { left: 1h }, forget: { left: 20s }"));

	assert!(err.contains("FLOW_081"), "expected FLOW_081, got: {err}");
}
