// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TempDbPath;

use crate::storage::ttl::{BACKLOG_ROWS, age_past_ttl, await_drained, backlog, one_row_per_tick, ttl_db};

const DDL: &str = "create ringbuffer test::rb { n: int4 } with { time: processing, capacity: 100, row: { ttl: 1s } }";

#[test]
fn a_backlog_larger_than_the_tick_budget_drains_across_ticks() {
	// Each partial batch rewrites count and head, so a wrong head strands the rows the next tick needs.
	let path = TempDbPath::new("ttl_batching_ringbuffer");

	let mut db = ttl_db(&path, one_row_per_tick());
	db.admin("create namespace test");
	db.admin(DDL);
	db.command(&backlog("test::rb", ""));
	assert_eq!(db.row_count("from test::rb"), BACKLOG_ROWS);

	age_past_ttl();
	assert!(
		db.row_count("from test::rb") >= 4,
		"a one row tick budget must not clear an eight row backlog in a single pass"
	);

	await_drained(&db, "from test::rb", 0);
	db.stop();
}

#[test]
fn a_buffer_drained_one_row_at_a_time_still_accepts_writes() {
	// Metadata rewritten on every partial batch must land on the true head, not the batch local one.
	let path = TempDbPath::new("ttl_batching_ringbuffer_rewrite");

	let mut db = ttl_db(&path, one_row_per_tick());
	db.admin("create namespace test");
	db.admin(DDL);
	db.command(&backlog("test::rb", ""));

	age_past_ttl();
	await_drained(&db, "from test::rb", 0);

	db.command("insert test::rb [{ n: 99 }]");
	assert_eq!(db.row_count("from test::rb filter n == 99"), 1, "a piecemeal drain must leave the buffer writable");
	db.stop();
}

#[test]
fn a_partitioned_backlog_larger_than_the_tick_budget_drains_across_ticks() {
	// A budget spent mid partition must resume on the same partition, not skip to the next.
	let path = TempDbPath::new("ttl_batching_ringbuffer_partitioned");

	let mut db = ttl_db(&path, one_row_per_tick());
	db.admin("create namespace test");
	db.admin(
		"create ringbuffer test::rb { p: utf8, n: int4 } with { time: processing, capacity: 100, row: { ttl: 1s }, partition: { by: { p } } }",
	);
	db.command("insert test::rb [{ p: \"a\", n: 1 }, { p: \"a\", n: 2 }, { p: \"b\", n: 3 }, { p: \"b\", n: 4 }]");
	assert_eq!(db.row_count("from test::rb"), 4);

	age_past_ttl();
	await_drained(&db, "from test::rb", 0);

	db.command("insert test::rb [{ p: \"a\", n: 5 }, { p: \"b\", n: 6 }]");
	assert_eq!(db.row_count("from test::rb"), 2, "both partitions must accept writes after a piecemeal drain");
	db.stop();
}
