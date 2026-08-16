// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TempDbPath;

use crate::storage::ttl::{
	PARTITIONS, STRADDLE_TTL_SECS, age_past, age_past_ttl, assert_evictor_ran, await_evicted, await_survivor,
	seed_canary, spread, ttl_db,
};

const DDL: &str = "create table test::t { p: utf8, n: int4 } with { time: processing, row: { ttl: %TTL%s }, partition: { by: { p } } }";

fn ddl(ttl: &str) -> String {
	DDL.replace("%TTL%", ttl)
}

#[test]
fn many_partitions_survive_while_every_row_is_inside_the_ttl() {
	// Partition count must not push any group past a cutoff it has not reached.
	let path = TempDbPath::new("ttl_many_table_none");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(&ddl("3600"));
	db.command(&spread("test::t", "", "p", PARTITIONS, 0));
	seed_canary(&db);

	assert_evictor_ran(&db);
	assert_eq!(db.row_count("from test::t"), PARTITIONS * 2, "a live evictor must leave every in ttl row alone");
	db.stop();
}

#[test]
fn many_partitions_drain_completely_once_every_row_expires() {
	// Expired keys are grouped by partition in memory, so a dropped group leaves rows alive with no error.
	let path = TempDbPath::new("ttl_many_table_full");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(&ddl("1"));
	db.command(&spread("test::t", "", "p", PARTITIONS, 0));
	assert_eq!(db.row_count("from test::t"), PARTITIONS * 2);

	age_past_ttl();
	await_evicted(&db, "from test::t", 0);
	db.stop();
}

#[test]
fn many_partitions_evict_only_the_aged_half() {
	// A shared batch spanning both ages must split cleanly, or fresh rows die with the old ones.
	let path = TempDbPath::new("ttl_many_table_partial");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(&ddl("10"));
	db.command(&spread("test::t", "", "old", 32, 0));

	age_past(STRADDLE_TTL_SECS);
	db.command(&spread("test::t", "", "new", 32, 1000));

	await_survivor(&db, "from test::t", 64);
	assert_eq!(db.row_count("from test::t filter n >= 1000"), 64, "every younger partition must survive intact");
	db.stop();
}
