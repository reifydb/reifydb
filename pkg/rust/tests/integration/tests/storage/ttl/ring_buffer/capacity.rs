// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TempDbPath;

use crate::storage::ttl::{STRADDLE_TTL_SECS, age_past, age_past_ttl, await_evicted, await_survivor, ttl_db};

#[test]
fn capacity_overflow_and_a_long_ttl_agree_on_the_survivors() {
	// Capacity and ttl are independent evictors, and a live ttl must not disturb the capacity window.
	let path = TempDbPath::new("ttl_capacity_overflow");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin("create ringbuffer test::rb { n: int4 } with { time: processing, capacity: 4, row: { ttl: 1h } }");
	db.command("insert test::rb [{ n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }, { n: 5 }, { n: 6 }]");

	assert_eq!(db.row_count("from test::rb"), 4, "capacity must cap the buffer at four rows");
	assert_eq!(db.row_count("from test::rb filter n >= 3"), 4, "the four newest writes must be the survivors");
	db.stop();
}

#[test]
fn a_wrapped_buffer_drains_completely_once_every_row_expires() {
	// A buffer whose head has wrapped past the tail must still be reachable by the expiry scan.
	let path = TempDbPath::new("ttl_capacity_wrapped_drain");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin("create ringbuffer test::rb { n: int4 } with { time: processing, capacity: 4, row: { ttl: 1s } }");
	db.command("insert test::rb [{ n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }, { n: 5 }, { n: 6 }]");
	assert_eq!(db.row_count("from test::rb"), 4);

	age_past_ttl();
	await_evicted(&db, "from test::rb", 0);

	db.command("insert test::rb [{ n: 10 }]");
	assert_eq!(db.row_count("from test::rb filter n == 10"), 1, "a drained wrapped buffer must stay writable");
	db.stop();
}

#[test]
fn capacity_eviction_of_already_expired_rows_keeps_the_newest_writes() {
	// Both evictors can claim the same row, and a double removal corrupts the count it decrements.
	let path = TempDbPath::new("ttl_capacity_overlaps_ttl");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin("create ringbuffer test::rb { n: int4 } with { time: processing, capacity: 3, row: { ttl: 10s } }");
	db.command("insert test::rb [{ n: 1 }, { n: 2 }, { n: 3 }]");

	age_past(STRADDLE_TTL_SECS);
	db.command("insert test::rb [{ n: 100 }, { n: 101 }, { n: 102 }, { n: 103 }, { n: 104 }]");

	await_survivor(&db, "from test::rb", 3);
	assert_eq!(db.row_count("from test::rb filter n >= 102"), 3, "only the three newest writes may remain");
	db.stop();
}

#[test]
fn capacity_is_counted_per_partition_alongside_the_ttl() {
	// Capacity is enforced per partition, so a busy partition must not consume another one's room.
	let path = TempDbPath::new("ttl_capacity_per_partition");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(
		"create ringbuffer test::rb { p: utf8, n: int4 } with { time: processing, capacity: 2, row: { ttl: 1h }, partition: { by: { p } } }",
	);
	db.command("insert test::rb [{ p: \"a\", n: 1 }, { p: \"a\", n: 2 }, { p: \"a\", n: 3 }, { p: \"b\", n: 4 }]");

	assert_eq!(db.row_count("from test::rb"), 3, "capacity two per partition must leave two plus one rows");
	assert_eq!(db.row_count("from test::rb filter p == \"b\""), 1, "a quiet partition keeps its only row");
	db.stop();
}

#[test]
fn a_wrapped_partition_drains_without_touching_a_younger_one() {
	// Wrapping rewrites head and tail, and a stale head sends the expiry scan into the wrong partition.
	let path = TempDbPath::new("ttl_capacity_wrapped_partition");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(
		"create ringbuffer test::rb { p: utf8, n: int4 } with { time: processing, capacity: 2, row: { ttl: 10s }, partition: { by: { p } } }",
	);
	db.command("insert test::rb [{ p: \"old\", n: 1 }, { p: \"old\", n: 2 }, { p: \"old\", n: 3 }]");

	age_past(STRADDLE_TTL_SECS);
	db.command("insert test::rb [{ p: \"new\", n: 100 }]");

	await_survivor(&db, "from test::rb", 1);
	assert_eq!(db.row_count("from test::rb filter p == \"new\""), 1, "the younger partition must survive");
	db.stop();
}
