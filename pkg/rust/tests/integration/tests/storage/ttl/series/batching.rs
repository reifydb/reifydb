// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TempDbPath;

use crate::storage::ttl::{BACKLOG_ROWS, age_past_ttl, await_drained, backlog, one_row_per_tick, ttl_db};

#[test]
fn a_backlog_larger_than_the_tick_budget_drains_across_ticks() {
	// The cursor must survive between ticks, or every pass restarts and the backlog never ends.
	let path = TempDbPath::new("ttl_batching_series");

	let mut db = ttl_db(&path, one_row_per_tick());
	db.admin("create namespace test");
	db.admin("create series test::s { ts: int8, n: int4 } with { time: processing, key: ts, row: { ttl: 1s } }");
	db.command(&backlog("test::s", "ts: 1, "));
	assert_eq!(db.row_count("from test::s"), BACKLOG_ROWS);

	age_past_ttl();
	assert!(
		db.row_count("from test::s") >= 4,
		"a one row tick budget must not clear an eight row backlog in a single pass"
	);

	await_drained(&db, "from test::s", 0);
	db.stop();
}

#[test]
fn a_partitioned_backlog_larger_than_the_tick_budget_drains_across_ticks() {
	// Partitioned series take the indexed path while unpartitioned ones full scan, so both must resume.
	let path = TempDbPath::new("ttl_batching_series_partitioned");

	let mut db = ttl_db(&path, one_row_per_tick());
	db.admin("create namespace test");
	db.admin(
		"create series test::s { ts: int8, p: utf8, n: int4 } with { time: processing, key: ts, row: { ttl: 1s }, partition: { by: { p } } }",
	);
	db.command(
		"insert test::s [{ ts: 1, p: \"a\", n: 1 }, { ts: 2, p: \"a\", n: 2 }, { ts: 3, p: \"b\", n: 3 }, { ts: 4, p: \"b\", n: 4 }]",
	);
	assert_eq!(db.row_count("from test::s"), 4);

	age_past_ttl();
	await_drained(&db, "from test::s", 0);
	db.stop();
}
