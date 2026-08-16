// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TempDbPath;

use crate::storage::ttl::{
	PARTITIONS, STRADDLE_TTL_SECS, age_past, age_past_ttl, assert_evictor_ran, await_evicted, await_survivor,
	seed_canary, spread, ttl_db,
};

const DDL: &str = "create ringbuffer test::rb { p: utf8, n: int4 } with { time: processing, capacity: 100, row: { ttl: %TTL%s }, partition: { by: { p } } }";

fn ddl(ttl: &str) -> String {
	DDL.replace("%TTL%", ttl)
}

#[test]
fn many_partitions_survive_while_every_row_is_inside_the_ttl() {
	// Partition count must not push any group past a cutoff it has not reached.
	let path = TempDbPath::new("ttl_many_ringbuffer_none");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(&ddl("3600"));
	db.command(&spread("test::rb", "", "p", PARTITIONS, 0));
	seed_canary(&db);

	assert_evictor_ran(&db);
	assert_eq!(db.row_count("from test::rb"), PARTITIONS * 2, "a live evictor must leave every in ttl row alone");
	db.stop();
}

#[test]
fn many_partitions_drain_completely_once_every_row_expires() {
	// Every partition owns metadata that must be looked up and rewritten, once per group.
	let path = TempDbPath::new("ttl_many_ringbuffer_full");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(&ddl("1"));
	db.command(&spread("test::rb", "", "p", PARTITIONS, 0));
	assert_eq!(db.row_count("from test::rb"), PARTITIONS * 2);

	age_past_ttl();
	await_evicted(&db, "from test::rb", 0);
	db.stop();
}

#[test]
fn many_partitions_evict_only_the_aged_half() {
	// Grouping by partition must not let an aged group reach rows filed under a younger one.
	let path = TempDbPath::new("ttl_many_ringbuffer_partial");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(&ddl("10"));
	db.command(&spread("test::rb", "", "old", 32, 0));

	age_past(STRADDLE_TTL_SECS);
	db.command(&spread("test::rb", "", "new", 32, 1000));

	await_survivor(&db, "from test::rb", 64);
	assert_eq!(db.row_count("from test::rb filter n >= 1000"), 64, "every younger partition must survive intact");
	db.stop();
}

#[test]
fn every_drained_partition_accepts_writes_again() {
	// A head left past the live rows strands later writes, and many partitions means many heads.
	let path = TempDbPath::new("ttl_many_ringbuffer_rewrite");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(&ddl("1"));
	db.command(&spread("test::rb", "", "p", PARTITIONS, 0));

	age_past_ttl();
	await_evicted(&db, "from test::rb", 0);

	db.command(&spread("test::rb", "", "p", PARTITIONS, 2000));
	assert_eq!(db.row_count("from test::rb"), PARTITIONS * 2, "every drained partition must accept writes again");
	db.stop();
}
