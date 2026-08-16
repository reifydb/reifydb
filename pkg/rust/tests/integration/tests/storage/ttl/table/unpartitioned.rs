// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TempDbPath;

use crate::storage::ttl::{STRADDLE_TTL_SECS, age_past, age_past_ttl, await_evicted, await_survivor, ttl_db};

#[test]
fn expired_rows_are_evicted_and_stay_gone_after_reopen() {
	// Clearing only the in-memory buffer reads as a pass until sqlite hands the rows back.
	let path = TempDbPath::new("ttl_table_unpartitioned_reopen");

	{
		let mut db = ttl_db(&path, []);
		db.admin("create namespace test");
		db.admin("create table test::t { n: int4 } with { time: processing, row: { ttl: 1s } }");
		db.command("insert test::t [{ n: 1 }, { n: 2 }, { n: 3 }]");
		assert_eq!(db.row_count("from test::t"), 3);

		age_past_ttl();
		await_evicted(&db, "from test::t", 0);
		db.stop();
	}

	{
		let mut db = ttl_db(&path, []);
		assert_eq!(db.row_count("from test::t"), 0, "evicted rows must not come back from the persistent tier");
		db.stop();
	}
}

#[test]
fn rows_inside_the_ttl_are_not_evicted() {
	// A cutoff applied to the whole object instead of per row would take rows that are still live.
	let path = TempDbPath::new("ttl_table_unpartitioned_live");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin("create table test::live { n: int4 } with { time: processing, row: { ttl: 1h } }");
	db.command("insert test::live [{ n: 1 }, { n: 2 }]");

	age_past_ttl();
	assert_eq!(db.row_count("from test::live"), 2, "a 1h ttl must survive a tick that runs seconds later");
	db.stop();
}

#[test]
fn only_the_aged_rows_are_evicted_when_writes_straddle_the_ttl() {
	// Expiry is per row, so a write made after the cutoff must outlive one made before it.
	let path = TempDbPath::new("ttl_table_unpartitioned_straddle");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin("create table test::t { n: int4 } with { time: processing, row: { ttl: 10s } }");
	db.command("insert test::t [{ n: 1 }, { n: 2 }]");

	age_past(STRADDLE_TTL_SECS);
	db.command("insert test::t [{ n: 3 }]");

	await_survivor(&db, "from test::t", 1);
	assert_eq!(
		db.row_count("from test::t filter n == 3"),
		1,
		"the row written after the cutoff must be the one that survives"
	);
	db.stop();
}
