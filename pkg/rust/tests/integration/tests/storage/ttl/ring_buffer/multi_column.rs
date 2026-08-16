// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TempDbPath;

use crate::storage::ttl::{
	STRADDLE_TTL_SECS, age_past, age_past_ttl, assert_evictor_ran, await_evicted, await_survivor, seed_canary,
	ttl_db,
};

const TWO_COL: &str = "create ringbuffer test::rb { a: utf8, b: utf8, n: int4 } with { time: processing, capacity: 100, row: { ttl: %TTL%s }, partition: { by: { a, b } } }";

const FOUR_COL: &str = "create ringbuffer test::rb { a: utf8, b: utf8, c: utf8, d: utf8, n: int4 } with { time: processing, capacity: 100, row: { ttl: %TTL%s }, partition: { by: { a, b, c, d } } }";

fn ddl(template: &str, ttl: &str) -> String {
	template.replace("%TTL%", ttl)
}

const TWO_COL_ROWS: &str =
	"insert test::rb [{ a: \"x\", b: \"1\", n: 1 }, { a: \"x\", b: \"2\", n: 2 }, { a: \"y\", b: \"1\", n: 3 }]";

const FOUR_COL_ROWS: &str = "insert test::rb [{ a: \"x\", b: \"1\", c: \"p\", d: \"q\", n: 1 }, { a: \"x\", b: \"2\", c: \"p\", d: \"q\", n: 2 }, { a: \"y\", b: \"1\", c: \"r\", d: \"s\", n: 3 }]";

#[test]
fn two_column_partitions_evict_nothing_while_every_row_is_inside_the_ttl() {
	let path = TempDbPath::new("ttl_ringbuffer_2col_none");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(&ddl(TWO_COL, "3600"));
	db.command(TWO_COL_ROWS);
	seed_canary(&db);

	assert_evictor_ran(&db);
	assert_eq!(db.row_count("from test::rb"), 3, "a live evictor must leave every in-ttl row alone");
	db.stop();
}

#[test]
fn two_column_partitions_drain_completely_once_every_row_expires() {
	let path = TempDbPath::new("ttl_ringbuffer_2col_full");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(&ddl(TWO_COL, "1"));
	db.command(TWO_COL_ROWS);
	assert_eq!(db.row_count("from test::rb"), 3);

	age_past_ttl();
	await_evicted(&db, "from test::rb", 0);
	db.stop();
}

#[test]
fn two_column_partitions_evict_only_the_aged_partition() {
	// Each composite partition owns a count and head, so removals must not cross between them.
	let path = TempDbPath::new("ttl_ringbuffer_2col_partial");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(&ddl(TWO_COL, "10"));
	db.command("insert test::rb [{ a: \"old\", b: \"1\", n: 1 }, { a: \"old\", b: \"2\", n: 2 }]");

	age_past(STRADDLE_TTL_SECS);
	db.command("insert test::rb [{ a: \"new\", b: \"1\", n: 3 }]");

	await_survivor(&db, "from test::rb", 1);
	assert_eq!(db.row_count("from test::rb filter a == \"new\""), 1, "the younger partition must survive");
	assert_eq!(db.row_count("from test::rb filter a == \"old\""), 0, "both aged partitions must be emptied");
	db.stop();
}

#[test]
fn two_column_partitions_accept_writes_after_a_partial_eviction() {
	// A head left past the live rows strands later writes in the partition that was evicted.
	let path = TempDbPath::new("ttl_ringbuffer_2col_rewrite");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(&ddl(TWO_COL, "1"));
	db.command(TWO_COL_ROWS);

	age_past_ttl();
	await_evicted(&db, "from test::rb", 0);

	db.command("insert test::rb [{ a: \"x\", b: \"1\", n: 10 }, { a: \"y\", b: \"1\", n: 11 }]");
	assert_eq!(db.row_count("from test::rb"), 2, "every evicted partition must accept writes again");
	db.stop();
}

#[test]
fn four_column_partitions_evict_nothing_while_every_row_is_inside_the_ttl() {
	let path = TempDbPath::new("ttl_ringbuffer_4col_none");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(&ddl(FOUR_COL, "3600"));
	db.command(FOUR_COL_ROWS);
	seed_canary(&db);

	assert_evictor_ran(&db);
	assert_eq!(db.row_count("from test::rb"), 3, "a live evictor must leave every in-ttl row alone");
	db.stop();
}

#[test]
fn four_column_partitions_drain_completely_once_every_row_expires() {
	let path = TempDbPath::new("ttl_ringbuffer_4col_full");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(&ddl(FOUR_COL, "1"));
	db.command(FOUR_COL_ROWS);
	assert_eq!(db.row_count("from test::rb"), 3);

	age_past_ttl();
	await_evicted(&db, "from test::rb", 0);
	db.stop();
}

#[test]
fn four_column_partitions_evict_only_the_aged_partition() {
	// Partition width must not change which rows an eviction reaches.
	let path = TempDbPath::new("ttl_ringbuffer_4col_partial");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(&ddl(FOUR_COL, "10"));
	db.command(
		"insert test::rb [{ a: \"old\", b: \"1\", c: \"p\", d: \"q\", n: 1 }, { a: \"old\", b: \"2\", c: \"p\", d: \"q\", n: 2 }]",
	);

	age_past(STRADDLE_TTL_SECS);
	db.command("insert test::rb [{ a: \"new\", b: \"1\", c: \"r\", d: \"s\", n: 3 }]");

	await_survivor(&db, "from test::rb", 1);
	assert_eq!(db.row_count("from test::rb filter a == \"new\""), 1, "the younger partition must survive");
	assert_eq!(db.row_count("from test::rb filter a == \"old\""), 0, "both aged partitions must be emptied");
	db.stop();
}
