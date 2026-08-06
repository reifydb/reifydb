// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! An aggregate interns one group per `by` key and nothing upstream retracts them, so a declared
//! ttl is the only thing that bounds it. These drive the whole chain - horizon, activity grid,
//! intern stamp, flow tick sweep - through a real flow rather than any link of it in isolation.

use std::time::Duration as StdDuration;

use reifydb::{ConfigKey, Value, WithSubsystem, embedded};
use reifydb_test_harness::db::TestDb;

const TIMEOUT: StdDuration = StdDuration::from_secs(20);

// How long the control waits before concluding that nothing was reclaimed. The declared case reports
// well inside a second, and the flow tick interval is one second, so this covers several ticks.
const SETTLE: StdDuration = StdDuration::from_secs(4);

// One second of event time. Short enough that the second insert below carries the watermark well
// past it, long enough that it is not confusable with the millisecond coordinates themselves.
const TTL: &str = "1s";

fn setup() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_flow(|f| f)
			// The per-operator compaction counters are the only surface that reports what the tick pass
			// reclaimed; a short sample cadence keeps the polls inside their timeouts.
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build memory db with flow"),
	)
}

const RECLAIMED_A_GROUP: &str = "from system::metrics::runtime::operators::current filter { metric == 'state_compaction_dropped' and value > 0.0 }";

fn create_flow(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	// The ts populator makes the row's #time its event time, which is the coordinate the intern is
	// stamped with; without it both inserts land in the same wall-clock activity bucket.
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { ts: ts }");
	db.admin(&format!("CREATE DEFERRED VIEW app::v {{ g: int4, total: int8 }} with {{ time: event }} \
		 AS {{ FROM app::t AGGREGATE {{ total: math::count(id) }} BY {{ g }} \
		 WITH {{ ttl: {{ duration: \"{TTL}\" }} }} }}"));
}

#[test]
fn an_aggregates_idle_group_is_reclaimed_through_the_flow_tick() {
	// The assertion is on work_done rather than a row count because the failure is silent in every
	// other surface: a node absent from ticks() is never scheduled, so the sweep never runs while
	// system::operators still reports the node as carrying a bounded span.
	let db = setup();
	create_flow(&db);

	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "2026-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM app::v", 1, TIMEOUT);

	// A second key carries the flow's event watermark past group 1's ttl, so group 1 goes idle
	// while others keep arriving - not a flow that stopped entirely.
	db.command(r#"INSERT app::t [{ id: 2, g: 2, ts: "2026-01-01T00:10:00Z" }]"#);

	// Asserted, not merely awaited: `await_row_count` returns its last observation on timeout, so
	// discarding it would pass against a chain that reclaims nothing.
	assert_eq!(
		db.await_row_count(RECLAIMED_A_GROUP, 1, TIMEOUT),
		1,
		"the aggregate must report reclamation work; a break anywhere in the chain leaves this at zero"
	);
}

#[test]
fn an_aggregate_without_a_ttl_reclaims_nothing() {
	// The control: same flow and same inserts, no declared ttl, so the horizon is perpetual and the
	// sweep has no cutoff. If this went green for any reason other than the ttl, the pair would
	// lose all discriminating power.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { ts: ts }");
	db.admin("CREATE DEFERRED VIEW app::v { g: int4, total: int8 } with { time: event } \
		 AS { FROM app::t AGGREGATE { total: math::count(id) } BY { g } }");

	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "2026-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM app::v", 1, TIMEOUT);
	db.command(r#"INSERT app::t [{ id: 2, g: 2, ts: "2026-01-01T00:10:00Z" }]"#);
	// Both groups published, so the flow is live and has ticked through the same path the declared
	// case reclaims on. Anything that follows is absence of reclamation, not absence of activity.
	db.await_row_count("FROM app::v", 2, TIMEOUT);

	// This never reaches 1, so it waits the window out and hands back what it saw; asserting
	// immediately would pass before the first tick had a chance to run.
	let reclaimed = db.await_row_count(RECLAIMED_A_GROUP, 1, SETTLE);

	assert_eq!(
		reclaimed, 0,
		"an undeclared aggregate must retain its groups; reclaiming them would silently change the \
		 view's answer rather than free memory"
	);
}
