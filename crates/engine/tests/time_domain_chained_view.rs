// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The time-domain rules applied one link down the chain: a view reading another view.
//
// A source view declares no `ts` column of its own - it inherits its domain from the flow that
// maintains it - so reconciling a chained view means resolving that upstream flow's declaration. The
// failure mode this file exists for is the domain walk simply skipping view sources: every rule below
// would then hold for a view over a table and silently lapse for a view over a view, which is where a
// long pipeline spends most of its nodes. Each test therefore pairs the chained case with the
// equivalent direct-over-table case, so a regression that re-skips views shows up as a divergence
// between two assertions that must agree.

use reifydb_engine::test_harness::TestEngine;
use reifydb_value::{params::Params, value::identity::IdentityId};

fn event_source_chain() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE cv");
	t.admin("CREATE TABLE cv::src { id: int4, at: datetime } WITH { time: event, ts: at }");
	t.admin("CREATE DEFERRED VIEW cv::upstream { id: int4, at: datetime } WITH { time: event } AS { FROM cv::src }");
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
// Intent: the FLOW_041 trap, one link down. An author who declares nothing over an event-time VIEW has
// made exactly the mistake the rule exists for - they believe the domain follows what they are reading
// - and the view being a view rather than a table changes nothing about how wrong the resulting flow
// is. Both halves are asserted together because a walk that skips view sources passes the table half
// and silently accepts the view half.
// Mutation: drop the SourceView arm from check_time_domain and only the chained assertion fails.
fn an_undeclared_flow_over_an_event_time_view_is_rejected_like_one_over_a_table() {
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
// Intent: the FLOW_040 trap, one link down. A flow cannot demand event time from an upstream view that
// supplies none - there is no #time to inherit, so every row would fall back to arrival while the flow
// claims to bucket by event time. The upstream here is undeclared, which is the common shape: nobody
// wrote `processing` anywhere, and the domain has to be read off the absence.
// Mutation: default an undeclared upstream flow to Event rather than Processing and the chained
// assertion starts passing, which is the silent-acceptance this test exists to prevent.
fn an_event_time_flow_over_a_processing_view_is_rejected_like_one_over_a_table() {
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
// Intent: an upstream that declared event time propagates it, so a downstream that declares the same
// domain is accepted and the chain can be arbitrarily long. Without this the two rejection tests above
// would be satisfied by a walk that rejects every chained view outright, which would make views
// unusable as sources rather than correctly reconciled.
fn a_matching_declaration_down_the_chain_is_accepted() {
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
// Intent: the explicit processing override works over a view exactly as it does over a table - this is
// the cell that separates "reconcile the chain" from "ban mixing domains in a chain". The author said
// processing on purpose, and the only difference from the rejected undeclared case above is that they
// said it at all.
fn an_explicit_processing_override_over_an_event_time_view_is_accepted() {
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
// Intent: an undeclared view over an undeclared view is the ordinary case and must stay silent. An
// upstream flow that declared nothing runs as processing - that is what FlowDag::time_domain does at
// runtime - and the walk has to read it the same way, or every processing-time pipeline more than one
// view deep would fail to create.
// Mutation: treat an undeclared upstream as an unknown domain and reject it, and every plain chained
// view in existence stops being creatable.
fn an_undeclared_chain_over_processing_sources_stays_silent() {
	let t = processing_source_chain();

	assert_eq!(
		code(&t, "CREATE DEFERRED VIEW cv::down { id: int4, at: datetime } AS { FROM cv::upstream }"),
		None,
		"the ordinary processing-time chain must create without a declaration anywhere"
	);
}

#[test]
// Intent: a transactional view is reconciled like a deferred one. Both kinds route through the same
// flow creation, so a domain honoured for one and dropped for the other is exactly the divergence the
// shared path exists to prevent - and the two are declared with different keywords, which is the kind
// of split where a skipped arm hides.
fn a_transactional_view_is_reconciled_like_a_deferred_one() {
	let t = event_source_chain();

	assert_eq!(
		code(&t, "CREATE TRANSACTIONAL VIEW cv::txn { id: int4, at: datetime } AS { FROM cv::upstream }")
			.as_deref(),
		Some("FLOW_041"),
		"an undeclared transactional view over an event-time view must be rejected too"
	);
}
