// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TempDbPath;

use crate::storage::ttl::{BACKLOG_ROWS, age_past_ttl, await_drained, backlog, one_row_per_tick, ttl_db};

#[test]
fn a_backlog_larger_than_the_tick_budget_drains_across_ticks() {
	// One row per tick must leave the rest behind a cursor that resumes, or the tail is lost forever.
	let path = TempDbPath::new("ttl_batching_table");

	let mut db = ttl_db(&path, one_row_per_tick());
	db.admin("create namespace test");
	db.admin("create table test::t { n: int4 } with { time: processing, row: { ttl: 1s } }");
	db.command(&backlog("test::t", ""));
	assert_eq!(db.row_count("from test::t"), BACKLOG_ROWS);

	age_past_ttl();
	assert!(
		db.row_count("from test::t") >= 4,
		"a one row tick budget must not clear an eight row backlog in a single pass"
	);

	await_drained(&db, "from test::t", 0);
	db.stop();
}

#[test]
fn a_partitioned_backlog_larger_than_the_tick_budget_drains_across_ticks() {
	// A partitioned table is scanned through a second keyspace that shares the same tick budget.
	let path = TempDbPath::new("ttl_batching_table_partitioned");

	let mut db = ttl_db(&path, one_row_per_tick());
	db.admin("create namespace test");
	db.admin(
		"create table test::t { p: utf8, n: int4 } with { time: processing, row: { ttl: 1s }, partition: { by: { p } } }",
	);
	db.command("insert test::t [{ p: \"a\", n: 1 }, { p: \"a\", n: 2 }, { p: \"b\", n: 3 }, { p: \"b\", n: 4 }]");
	assert_eq!(db.row_count("from test::t"), 4);

	age_past_ttl();
	await_drained(&db, "from test::t", 0);
	db.stop();
}
