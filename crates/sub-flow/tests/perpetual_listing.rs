// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// A stateful node with no declared span is legal - perpetual retention is a deliberate choice, not an
// error - but it was invisible. Nothing in the catalog distinguished a node that ages from one that
// keeps every key it has ever seen, so the only way to find unbounded growth was to attach a profiler
// and read the resident set. system::flow_nodes now carries the resolved horizon, which turns that
// investigation into one query.

use std::time::Duration as StdDuration;

use reifydb::{WithSubsystem, embedded};
use reifydb_test_harness::{assert::FrameAssert, db::TestDb};
use reifydb_value::value::{Value, duration::Duration};

const PERPETUAL: &str = "FROM system::flow_nodes FILTER { stateful == true and horizon == 'perpetual' }";
const STATEFUL: &str = "FROM system::flow_nodes FILTER { stateful == true }";

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

// The horizon is published when the flow engine registers the node, which happens off the DDL
// transaction, so every assertion here has to settle rather than read once.
fn settled_count(db: &TestDb, rql: &str, want: usize) -> usize {
	db.await_exact_row_count(rql, want, StdDuration::from_secs(5))
}

#[test]
fn a_stateful_node_without_a_declared_span_is_listed_as_perpetual() {
	// Intent: this is the row the probe was missing. An append node with no ttl retains every key
	// forever - legitimate, but the operator has to be able to find it before the process runs out
	// of memory, not after. The node holds state AND has no span, and both halves are needed: a
	// listing keyed on span alone would sweep in every stateless map node and be useless.
	// Mutation: make adopt_horizon publish stateful: false and this returns 0 - the leak is legal
	// again and once more invisible.
	let db = setup();
	db.admin("CREATE NAMESPACE sp");
	db.admin("CREATE TABLE sp::a { id: int4, v: int4 }");
	db.admin("CREATE TABLE sp::b { id: int4, v: int4 }");
	db.admin("CREATE DEFERRED VIEW sp::u { id: int4, v: int4 } AS { FROM sp::a append { FROM sp::b } }");

	assert_eq!(
		settled_count(&db, PERPETUAL, 1),
		1,
		"an append node with no declared span retains forever and must be listed as perpetual"
	);
}

#[test]
fn a_declared_span_takes_the_node_off_the_perpetual_listing() {
	// The control, and the half that proves the listing tracks the resolved horizon rather than the
	// node type. The RQL differs from the test above by the ttl alone, so a listing that reported
	// every append node - or every stateful node - passes there and fails here.
	let db = setup();
	db.admin("CREATE NAMESPACE sp");
	db.admin("CREATE TABLE sp::a { id: int4, v: int4 }");
	db.admin("CREATE TABLE sp::b { id: int4, v: int4 }");
	db.admin("CREATE DEFERRED VIEW sp::u { id: int4, v: int4 } AS { \
		 FROM sp::a append { FROM sp::b } with { ttl: { duration: \"1s\" } } }");

	assert_eq!(settled_count(&db, STATEFUL, 1), 1, "the append node is still stateful once it declares a span");
	assert_eq!(
		db.row_count(PERPETUAL),
		0,
		"a node that declares a span it can honour must not be reported as perpetual"
	);
	// The declared duration itself has to survive, not merely the fact that one exists: a listing
	// that reported every span as present would hide a node whose ttl is far longer than its author
	// believes it to be.
	db.query(STATEFUL).assert().column("span", &[Value::Duration(Duration::from_seconds(1).unwrap())]);
}

#[test]
fn a_window_reports_the_seal_its_operator_derives_rather_than_perpetual() {
	// A window declares no ttl, so its DECLARED horizon is Perpetual - but it seals on its own
	// schedule and its state is bounded. Reporting the declared horizon here would put every
	// windowed node on the perpetual listing, which is the fastest way to make the listing useless:
	// the nodes that genuinely retain forever would be buried under the ones that do not.
	// Mutation: publish node.ty.declared_horizon(..) instead of node_horizon(..) and this window
	// joins the perpetual list with span none, while the two tests above still pass.
	let db = setup();
	db.admin("CREATE NAMESPACE sp");
	db.admin("CREATE TABLE sp::t { id: int4, g: int4, v: int4, ts: datetime } with { ts: ts }");
	db.admin(r#"CREATE DEFERRED VIEW sp::w { g: int4, total: int8 } with { time: event } AS {
			FROM sp::t
				| window tumbling { total: math::sum(v) }
					with { interval: "1s", grace: "0s" }
					by { g }
		}"#);

	assert_eq!(settled_count(&db, STATEFUL, 1), 1, "a window node holds group state");
	assert_eq!(
		db.row_count(PERPETUAL),
		0,
		"a window seals on its own schedule, so it must not be listed as retaining forever"
	);
}

#[test]
fn a_stateless_node_is_never_reported_as_stateful() {
	// Without this, `stateful` could be hardcoded true and both tests above would still pass. A map
	// node holds nothing between rows, so listing it as perpetual would bury the nodes that do hold
	// state in noise proportional to the size of every flow in the database.
	let db = setup();
	db.admin("CREATE NAMESPACE sp");
	db.admin("CREATE TABLE sp::t { id: int4, v: int4 }");
	db.admin("CREATE DEFERRED VIEW sp::v { id: int4, v: int4 } AS { FROM sp::t map { id, v } }");

	db.await_all_flows(StdDuration::from_secs(5));

	assert_eq!(
		db.row_count(STATEFUL),
		0,
		"source, map and sink nodes hold no reclaimable state and must not be listed"
	);
	assert!(
		db.row_count("FROM system::flow_nodes") >= 3,
		"the flow's nodes are still listed - it is the stateful flag that distinguishes them"
	);
}
