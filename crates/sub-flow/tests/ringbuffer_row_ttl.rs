// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// A ring buffer's row TTL is a timer armed at the head row's own #time plus the ttl and fired by
// the flow watermark, not a clock, so the ttl means the same thing replaying last month's data as
// it does live. A version-anchored eviction instead ages rows by how recently they were ingested.

use std::{thread::sleep, time::Duration as StdDuration};

use reifydb::{WithSubsystem, embedded};
use reifydb_test_harness::db::TestDb;

const TIMEOUT: StdDuration = StdDuration::from_secs(5);

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

/// An event-time ring buffer whose capacity is far above anything the tests insert, so every
/// eviction observed here is the row TTL and never the capacity bound.
fn event_ring(db: &TestDb, ttl: &str) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::events { id: int4, v: int4, ts: datetime } with { ts: ts }");
	db.admin(&format!("CREATE DEFERRED RINGBUFFER VIEW app::rb {{ id: int4, v: int4 }} \
		 WITH {{ capacity: 1000, time: event, row: {{ ttl: {{ duration: '{ttl}', announce: true }} }} }} \
		 AS {{ FROM app::events map {{ id, v }} }}"));
}

fn insert(db: &TestDb, id: i32, v: i32, ts: &str) {
	db.command(&format!(r#"INSERT app::events [{{ id: {id}, v: {v}, ts: "{ts}" }}]"#));
}

#[test]
fn a_row_expires_during_replay_once_the_event_watermark_passes_its_ttl() {
	// Replaying a day of history in a second must expire rows at the same event-time boundaries
	// the live feed would, so the buffer holds one ttl-window of data rather than of ingestion.
	// Nothing waits on wall time: the second insert is what carries the watermark past row 1.
	let db = setup();
	event_ring(&db, "1h");

	insert(&db, 1, 10, "2026-01-01T00:00:00Z");
	db.await_row_count("FROM app::rb", 1, TIMEOUT);

	insert(&db, 2, 20, "2026-01-01T02:00:00Z");

	let survivors = db.await_exact_row_count("FROM app::rb FILTER { id == 1 }", 0, TIMEOUT);
	assert_eq!(
		survivors,
		0,
		"row 1 is two hours old in event time and its ttl is one hour, so it must be gone; view now: {:?}",
		db.query_as_root("FROM app::rb", ())
	);
	assert_eq!(
		db.await_exact_row_count("FROM app::rb FILTER { id == 2 }", 1, TIMEOUT),
		1,
		"the row that advanced the watermark is itself still inside its ttl and must survive"
	);
}

#[test]
fn a_frozen_event_watermark_keeps_a_row_far_past_its_ttl_in_wall_time() {
	// The mirror of the test above, and the one that catches a clock read: wall time blows past a
	// 1s ttl while the event watermark stays put, and a stopped feed must hold rather than empty
	// itself. The sleep is not a race - the assertion is absence, so a longer one is only stricter.
	let db = setup();
	event_ring(&db, "1s");

	insert(&db, 1, 10, "2026-01-01T00:00:00Z");
	db.await_row_count("FROM app::rb", 1, TIMEOUT);

	sleep(StdDuration::from_secs(3));

	assert_eq!(
		db.await_exact_row_count("FROM app::rb FILTER { id == 1 }", 1, TIMEOUT),
		1,
		"the feed stopped, so the event watermark never reached the row's expiry; view now: {:?}",
		db.query_as_root("FROM app::rb", ())
	);
}

#[test]
fn an_idle_processing_ring_buffer_drains_while_an_idle_event_ring_holds() {
	// A processing watermark is the wall clock and advances with no input, so an idle ring drains;
	// an event watermark moves only on rows, so an idle ring holds. Both rings are fed from the
	// same table in the same command, so nothing but the declared domain can separate them.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::events { id: int4, v: int4, ts: datetime } with { ts: ts }");
	db.admin("CREATE DEFERRED RINGBUFFER VIEW app::held { id: int4, v: int4 } \
		 WITH { capacity: 1000, time: event, row: { ttl: { duration: '1s', announce: true } } } \
		 AS { FROM app::events map { id, v } }");
	db.admin("CREATE DEFERRED RINGBUFFER VIEW app::drained { id: int4, v: int4 } \
		 WITH { capacity: 1000, time: processing, row: { ttl: { duration: '1s', announce: true } } } \
		 AS { FROM app::events map { id, v } }");

	insert(&db, 1, 10, "2026-01-01T00:00:00Z");
	db.await_row_count("FROM app::held", 1, TIMEOUT);
	db.await_row_count("FROM app::drained", 1, TIMEOUT);

	let drained = db.await_exact_row_count("FROM app::drained", 0, TIMEOUT);
	assert_eq!(
		drained,
		0,
		"a processing ring buffer ages off the wall clock and must drain while idle; view now: {:?}",
		db.query_as_root("FROM app::drained", ())
	);
	assert_eq!(
		db.await_exact_row_count("FROM app::held", 1, TIMEOUT),
		1,
		"the event ring saw no new row, so its watermark never reached the expiry and it must hold"
	);
}
