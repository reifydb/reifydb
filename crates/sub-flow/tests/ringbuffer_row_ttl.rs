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
fn an_idle_processing_ring_buffer_holds_and_drains_on_the_next_arrival() {
	// A processing watermark is arrival time - event time over the arrival stamps - so it moves
	// only when rows arrive. An idle processing ring must therefore hold its rows however long
	// the wall clock runs (a clock-driven drain would evict different rows when the stream is
	// replayed later), and it is the next arrival, stamped past the ttl, that finally expires
	// the head. The event ring is fed the same rows with a fixed ts, so it holds throughout.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::events { id: int4, v: int4, ts: datetime } with { ts: ts }");
	db.admin("CREATE DEFERRED RINGBUFFER VIEW app::held { id: int4, v: int4 } \
		 WITH { capacity: 1000, time: event, row: { ttl: { duration: '1h', announce: true } } } \
		 AS { FROM app::events map { id, v } }");
	db.admin("CREATE DEFERRED RINGBUFFER VIEW app::drained { id: int4, v: int4 } \
		 WITH { capacity: 1000, time: processing, row: { ttl: { duration: '1s', announce: true } } } \
		 AS { FROM app::events map { id, v } }");

	insert(&db, 1, 10, "2026-01-01T00:00:00Z");
	db.await_row_count("FROM app::held", 1, TIMEOUT);
	db.await_row_count("FROM app::drained", 1, TIMEOUT);

	// Wall time blows past the 1s ttl with no input. The sleep is not a race - the assertion is
	// that nothing was evicted, so a longer sleep is only stricter. It also guarantees the next
	// arrival is stamped more than one ttl after the head row.
	sleep(StdDuration::from_secs(2));
	assert_eq!(
		db.await_exact_row_count("FROM app::drained FILTER { id == 1 }", 1, TIMEOUT),
		1,
		"an idle processing ring must hold: its arrival watermark never reached the expiry; view now: {:?}",
		db.query_as_root("FROM app::drained", ())
	);

	insert(&db, 2, 20, "2026-01-01T00:00:01Z");

	assert_eq!(
		db.await_exact_row_count("FROM app::drained FILTER { id == 1 }", 0, TIMEOUT),
		0,
		"the second arrival is stamped more than one ttl after row 1, so it must carry the \
		 watermark past row 1's expiry; view now: {:?}",
		db.query_as_root("FROM app::drained", ())
	);
	assert_eq!(
		db.await_exact_row_count("FROM app::held FILTER { id == 1 }", 1, TIMEOUT),
		1,
		"the event ring's rows sit one second apart in ts, far inside its 1h ttl, so it must hold"
	);
}
