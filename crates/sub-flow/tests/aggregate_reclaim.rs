// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! An aggregate interns one group per `by` key and nothing upstream ever retracts them, so the
//! declared ttl is the only thing that bounds it. That ttl reaches the node as a horizon, the
//! horizon derives an event-domain activity grid, the substrate stamps every intern against it, and
//! the flow tick pass erases the groups that fall behind the cutoff. This drives that whole chain
//! through a real flow, because every link in it was already in place while the chain itself was
//! severed: `FlowNodeType::Aggregate` answered false to `ticks()`, both flow drivers gate the entire
//! tick on `flow.ticks()`, and an aggregate is the only node in this flow that asks for one. The
//! declaration was accepted, published as a bounded span, and reclaimed nothing forever.

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
			// The retention ledger is the only surface that reports what the tick pass actually
			// reclaimed; without a refresh cadence it stays empty (none means off).
			.with_config(ConfigKey::MetricsLifecycleRefreshInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build memory db with flow"),
	)
}

const RECLAIMED_A_GROUP: &str =
	"from system::metrics::lifecycle::current filter { class == 'operator-group-data' and work_done > 0 }";

fn create_flow(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	// The ts populator is what makes the row's #time its event time, which is the coordinate the
	// substrate stamps the intern with. Without it the stamp would come from the wall clock and the
	// two inserts below would land in the same activity bucket however far apart their ts columns
	// claimed to be.
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { ts: ts }");
	db.admin(&format!("CREATE DEFERRED VIEW app::v {{ g: int4, total: int8 }} with {{ time: event }} \
		 AS {{ FROM app::t AGGREGATE {{ total: math::count(id) }} BY {{ g }} \
		 WITH {{ ttl: {{ duration: \"{TTL}\" }} }} }}"));
}

#[test]
fn an_aggregates_idle_group_is_reclaimed_through_the_flow_tick() {
	// Intent: an aggregate that declares a ttl actually reclaims. The assertion is on work_done
	// rather than on a row count because the failure this guards is silent in every other surface:
	// with the node absent from ticks() the flow is never scheduled, so the sweep never runs, and
	// system::flow_nodes still reports the node as carrying a bounded span.
	let db = setup();
	create_flow(&db);

	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "1970-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM app::v", 1, TIMEOUT);

	// A second key carries the flow's event watermark far past group 1's ttl, so group 1 goes idle
	// without being touched itself. This is the shape that matters: a quiet key while others keep
	// arriving, not a flow that stopped entirely.
	db.command(r#"INSERT app::t [{ id: 2, g: 2, ts: "1970-01-01T00:10:00Z" }]"#);

	// Asserted, not merely awaited. `await_row_count` returns its last observation on timeout rather
	// than panicking, so discarding it makes this test pass against a chain that reclaims nothing at
	// all - it just takes the full timeout to do it.
	assert_eq!(
		db.await_row_count(RECLAIMED_A_GROUP, 1, TIMEOUT),
		1,
		"the aggregate must report reclamation work; a break anywhere in the chain leaves this at zero"
	);
}

#[test]
fn an_aggregate_without_a_ttl_reclaims_nothing() {
	// The control. Same flow, same two inserts, no declared ttl - so the node's horizon is
	// perpetual and the sweep has no cutoff to work from. If this went green alongside the test
	// above for a reason other than the ttl (a sink row ttl, an unrelated class reaching the same
	// metric row), the discriminating power of the pair would be gone and neither would be
	// evidence of anything.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { ts: ts }");
	db.admin("CREATE DEFERRED VIEW app::v { g: int4, total: int8 } with { time: event } \
		 AS { FROM app::t AGGREGATE { total: math::count(id) } BY { g } }");

	db.command(r#"INSERT app::t [{ id: 1, g: 1, ts: "1970-01-01T00:00:00Z" }]"#);
	db.await_row_count("FROM app::v", 1, TIMEOUT);
	db.command(r#"INSERT app::t [{ id: 2, g: 2, ts: "1970-01-01T00:10:00Z" }]"#);
	// Both groups published, so the flow is live and has ticked through the same path the declared
	// case reclaims on. Anything that follows is absence of reclamation, not absence of activity.
	db.await_row_count("FROM app::v", 2, TIMEOUT);

	// This never reaches 1, so it waits out the whole window and hands back what it saw. Asserting
	// immediately instead would pass before the first tick had a chance to run and would prove
	// nothing. SETTLE is comfortably longer than the declared case takes to report.
	let reclaimed = db.await_row_count(RECLAIMED_A_GROUP, 1, SETTLE);

	assert_eq!(
		reclaimed, 0,
		"an undeclared aggregate must retain its groups; reclaiming them would silently change the \
		 view's answer rather than free memory"
	);
}
