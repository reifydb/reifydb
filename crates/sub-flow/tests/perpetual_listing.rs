// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// A stateful node with no declared span is legal but invisible: system::operators carries the
// retention scale and the frontier actually reclaimed through so unbounded growth is one query
// rather than a profiler. `retains_forever` is a boolean because RQL has no none-predicate.

use std::time::Duration as StdDuration;

use reifydb::{
	ConfigKey, WithSubsystem, embedded,
	testing::db::{TestDb, await_value},
};
use reifydb_test_harness::assert::{FrameAssert, column_values};
use reifydb_value::value::{Value, datetime::DateTime, duration::Duration};

const PERPETUAL: &str = "FROM system::operators FILTER { stateful == true and retains_forever == true }";
const STATEFUL: &str = "FROM system::operators FILTER { stateful == true }";

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

fn settled_count(db: &TestDb, rql: &str, want: usize) -> usize {
	// The horizon is published when the flow engine registers the node, off the DDL transaction,
	// so every assertion here has to settle rather than read once.
	db.await_exact_row_count(rql, want, StdDuration::from_secs(5))
}

#[test]
fn a_stateful_node_without_a_declared_span_is_listed_as_perpetual() {
	// An append node with no ttl retains every key forever - legitimate, but findable before the
	// process runs out of memory. Both halves matter: a listing keyed on the missing span alone
	// would sweep in every stateless map node and be useless.
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
	// The declared duration itself has to survive, not merely the fact that one exists, or a node
	// whose ttl is far longer than its author believes stays hidden.
	db.query(STATEFUL).assert().column("retention_scale", &[Value::Duration(Duration::from_seconds(1).unwrap())]);
}

#[test]
fn a_node_that_has_swept_reports_the_frontier_it_reclaimed_through() {
	// The scale says how far back the node is willing to keep; only the frontier says how far it
	// actually got. A node registered but never swept reads as healthy on the scale alone, and the
	// frontier must carry the instant rather than a flag or a stalled node looks like a live one.
	let db = TestDb::from(
		embedded::memory()
			.with_flow(|f| f)
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build memory db with flow"),
	);
	db.admin("CREATE NAMESPACE sp");
	db.admin("CREATE TABLE sp::t { id: int4, g: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE DEFERRED VIEW sp::v { g: int4, total: int8 } AS { \
		 FROM sp::t AGGREGATE { total: math::count(id) } BY { g } WITH { ttl: { duration: \"1s\" } } }");

	// Event time, not the wall clock, so the frontier compaction reports is a value this test can
	// name. The base sits far from the epoch because an epoch-stamped state row is what the arena
	// treats as an unstamped writer.
	db.command(r#"INSERT sp::t [{ id: 1, g: 1, ts: "2026-01-01T00:00:00Z" }]"#);
	db.command(r#"INSERT sp::t [{ id: 2, g: 2, ts: "2026-01-01T00:10:00Z" }]"#);

	const BASE_2026_MS: u64 = 1_767_225_600_000;
	let want = vec![Value::DateTime(DateTime::from_millis(BASE_2026_MS + 599_000))];
	let got = await_value(want.clone(), StdDuration::from_secs(20), || {
		db.query(STATEFUL).first().map(|frame| column_values(frame, "frontier")).unwrap_or_default()
	});
	assert_eq!(
		got, want,
		"the aggregate's frontier must be its watermark less the declared ttl, not none and not the \
		 raw watermark"
	);
}

#[test]
fn a_window_reports_the_seal_its_operator_derives_rather_than_perpetual() {
	// A window declares no ttl, so its declared horizon is perpetual, but it seals on its own
	// schedule and its state is bounded. Publishing the declared horizon would bury the nodes that
	// genuinely retain forever under every windowed node.
	let db = setup();
	db.admin("CREATE NAMESPACE sp");
	db.admin("CREATE TABLE sp::t { id: int4, g: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin(r#"CREATE DEFERRED VIEW sp::w { g: int4, total: int8 } AS {
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
		db.row_count("FROM system::operators") >= 3,
		"the flow's nodes are still listed - it is the stateful flag that distinguishes them"
	);
}
