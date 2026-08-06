// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Each test pairs a view-over-view against the equivalent view-over-table, so a domain walk that
// skips view sources shows up as a divergence between two assertions that must agree.

use reifydb_engine::test_harness::TestEngine;
use reifydb_value::{params::Params, value::identity::IdentityId};

fn event_source_chain() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE cv");
	t.admin("CREATE TABLE cv::src { id: int4, at: datetime } WITH { time: event, ts: at }");
	t.admin(
		"CREATE DEFERRED VIEW cv::upstream { id: int4, at: datetime } WITH { time: event } AS { FROM cv::src }",
	);
	t
}

fn processing_source_chain() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE cv");
	t.admin("CREATE TABLE cv::src { id: int4, at: datetime }");
	t.admin("CREATE DEFERRED VIEW cv::upstream { id: int4, at: datetime } AS { FROM cv::src }");
	t
}

fn code(t: &TestEngine, rql: &str) -> Option<String> {
	t.inner().admin_as(IdentityId::system(), rql, Params::None).error.map(|e| e.diagnostic().code)
}

#[test]
fn an_undeclared_flow_over_an_event_time_view_is_rejected_like_one_over_a_table() {
	// Both halves are asserted together because a walk that skips view sources passes the table
	// half and silently accepts the view half.
	let t = event_source_chain();

	assert_eq!(
		code(&t, "CREATE DEFERRED VIEW cv::direct { id: int4, at: datetime } AS { FROM cv::src }").as_deref(),
		Some("FLOW_041"),
		"undeclared over an event-time table"
	);
	assert_eq!(
		code(&t, "CREATE DEFERRED VIEW cv::chained { id: int4, at: datetime } AS { FROM cv::upstream }")
			.as_deref(),
		Some("FLOW_041"),
		"undeclared over an event-time view must be rejected the same way"
	);
}

#[test]
fn an_event_time_flow_over_a_processing_view_is_rejected_like_one_over_a_table() {
	// With no #time to inherit, every row falls back to arrival while the flow claims to bucket
	// by event time. The upstream is undeclared, so the domain has to be read off the absence.
	let t = processing_source_chain();

	assert_eq!(
		code(
			&t,
			"CREATE DEFERRED VIEW cv::direct { id: int4, at: datetime } WITH { time: event } AS { FROM cv::src }"
		)
		.as_deref(),
		Some("FLOW_040"),
		"event time over a processing table"
	);
	assert_eq!(
		code(
			&t,
			"CREATE DEFERRED VIEW cv::chained { id: int4, at: datetime } WITH { time: event } AS { FROM cv::upstream }"
		)
		.as_deref(),
		Some("FLOW_040"),
		"event time over a processing view must be rejected the same way"
	);
}

#[test]
fn a_matching_declaration_down_the_chain_is_accepted() {
	// Without this, the rejection tests would also be satisfied by a walk that rejects every
	// chained view outright, making views unusable as sources.
	let t = event_source_chain();

	assert_eq!(
		code(
			&t,
			"CREATE DEFERRED VIEW cv::second { id: int4, at: datetime } WITH { time: event } AS { FROM cv::upstream }"
		),
		None,
		"an event-time view over an event-time view"
	);
	assert_eq!(
		code(
			&t,
			"CREATE DEFERRED VIEW cv::third { id: int4, at: datetime } WITH { time: event } AS { FROM cv::second }"
		),
		None,
		"the domain must keep propagating past the second link"
	);
}

#[test]
fn an_explicit_processing_override_over_an_event_time_view_is_accepted() {
	// Separates reconciling a chain from banning mixed domains: the only difference from the
	// rejected undeclared case is that the author said processing at all.
	let t = event_source_chain();

	assert_eq!(
		code(
			&t,
			"CREATE DEFERRED VIEW cv::down { id: int4, at: datetime } WITH { time: processing } AS { FROM cv::upstream }"
		),
		None,
		"an explicit override must be honoured over a view source"
	);
}

#[test]
fn an_undeclared_chain_over_processing_sources_stays_silent() {
	// An upstream that declared nothing runs as processing; reading it any other way makes every
	// processing-time pipeline more than one view deep uncreatable.
	let t = processing_source_chain();

	assert_eq!(
		code(&t, "CREATE DEFERRED VIEW cv::down { id: int4, at: datetime } AS { FROM cv::upstream }"),
		None,
		"the ordinary processing-time chain must create without a declaration anywhere"
	);
}
