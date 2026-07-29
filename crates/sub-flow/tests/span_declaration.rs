// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// A retention span that the substrate cannot honor used to be accepted in silence: the node kept
// every row it ever saw while the catalog claimed it had a ttl. Registration now refuses both shapes
// that produce that outcome.

use reifydb::{WithSubsystem, embedded};
use reifydb_test_harness::db::TestDb;

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

fn rejection(db: &TestDb, rql: &str) -> Option<String> {
	match db.try_admin(rql) {
		Ok(_) => None,
		Err(err) => Some(err.diagnostic().code),
	}
}

#[test]
fn a_span_on_a_node_that_keeps_no_state_is_refused() {
	// Intent: spans only mean something on operators that hold keyed state. Declared on a map the
	// engine resolves the horizon to Perpetual and never consults the span again, so the author is
	// told their data ages when nothing does.
	// The grammar is the first line of defence and refuses this shape outright, which is what this
	// test pins - a grammar change that started accepting it would land here first. Registration
	// carries its own guard (FLOW_045) for the route the grammar cannot see: a DAG reloaded from
	// the catalog on restart, which is the same reason check_time_domain re-runs at registration.
	// Mutation: let the grammar accept `with { ttl }` on a map and this assertion fails, at which
	// point the registration guard is what stops the span being silently dropped.
	let db = setup();
	db.admin("CREATE NAMESPACE sp");
	db.admin("CREATE TABLE sp::t { id: int4, v: int4 }");

	assert_eq!(
		rejection(
			&db,
			"CREATE DEFERRED VIEW sp::v { id: int4, v: int4 } AS { \
			 FROM sp::t map { id, v } with { ttl: { duration: \"1s\" } } }"
		)
		.as_deref(),
		Some("AST_009"),
		"a span on a stateless node must be refused rather than accepted and ignored"
	);
}

#[test]
fn a_span_on_a_stateful_node_that_can_age_is_accepted() {
	// The control. Append holds keyed state and declares Reclaim, so the same span is legitimate
	// there - without this the test above would pass equally well against a rule that refused
	// every span, which would be a far worse defect than the one it fixes.
	let db = setup();
	db.admin("CREATE NAMESPACE sp");
	db.admin("CREATE TABLE sp::a { id: int4, v: int4 }");
	db.admin("CREATE TABLE sp::b { id: int4, v: int4 }");

	assert_eq!(
		rejection(
			&db,
			"CREATE DEFERRED VIEW sp::u { id: int4, v: int4 } AS { \
			 FROM sp::a append { FROM sp::b } with { ttl: { duration: \"1s\" } } }"
		),
		None,
		"append keeps keyed state and can reclaim, so its span is honored and must be accepted"
	);
}
