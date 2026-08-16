// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TempDbPath;

use crate::storage::ttl::{STRADDLE_TTL_SECS, age_past, age_past_ttl, await_evicted, await_survivor, ttl_db};

const DDL: &str = "create ringbuffer test::rb { n: int4 } with { time: processing, capacity: 100, row: { ttl: 1s } }";

#[test]
fn expired_rows_are_evicted_and_stay_gone_after_reopen() {
	// Ring buffer eviction also rewrites partition metadata, so a torn commit shows up on reopen.
	let path = TempDbPath::new("ttl_ringbuffer_unpartitioned_reopen");

	{
		let mut db = ttl_db(&path, []);
		db.admin("create namespace test");
		db.admin(DDL);
		db.command("insert test::rb [{ n: 1 }, { n: 2 }, { n: 3 }]");
		assert_eq!(db.row_count("from test::rb"), 3);

		age_past_ttl();
		await_evicted(&db, "from test::rb", 0);
		db.stop();
	}

	{
		let mut db = ttl_db(&path, []);
		assert_eq!(db.row_count("from test::rb"), 0, "evicted ring buffer rows must not survive in sqlite");
		db.stop();
	}
}

#[test]
fn rows_inside_the_ttl_are_not_evicted() {
	// Nothing may be dropped on a guess about age.
	let path = TempDbPath::new("ttl_ringbuffer_unpartitioned_live");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin("create ringbuffer test::live { n: int4 } with { time: processing, capacity: 100, row: { ttl: 1h } }");
	db.command("insert test::live [{ n: 1 }, { n: 2 }]");

	age_past_ttl();
	assert_eq!(db.row_count("from test::live"), 2, "a 1h ttl must survive a tick seconds later");
	db.stop();
}

#[test]
fn only_the_aged_rows_are_evicted_when_writes_straddle_the_ttl() {
	// The head must advance to the oldest surviving row, never past it.
	let path = TempDbPath::new("ttl_ringbuffer_unpartitioned_straddle");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin("create ringbuffer test::rb { n: int4 } with { time: processing, capacity: 100, row: { ttl: 10s } }");
	db.command("insert test::rb [{ n: 1 }, { n: 2 }]");

	age_past(STRADDLE_TTL_SECS);
	db.command("insert test::rb [{ n: 3 }]");

	await_survivor(&db, "from test::rb", 1);
	assert_eq!(
		db.row_count("from test::rb filter n == 3"),
		1,
		"the row written after the cutoff must be the one that survives"
	);
	db.stop();
}

#[test]
fn writes_after_an_eviction_are_readable() {
	// A head left pointing past the live rows strands every later write.
	let path = TempDbPath::new("ttl_ringbuffer_unpartitioned_rewrite");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(DDL);
	db.command("insert test::rb [{ n: 1 }, { n: 2 }]");

	age_past_ttl();
	await_evicted(&db, "from test::rb", 0);

	db.command("insert test::rb [{ n: 10 }]");
	assert_eq!(db.row_count("from test::rb filter n == 10"), 1, "a write after an eviction must be readable");
	db.stop();
}
