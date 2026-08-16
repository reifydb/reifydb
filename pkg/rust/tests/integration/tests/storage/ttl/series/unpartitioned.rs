// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TempDbPath;

use crate::storage::ttl::{age_past_ttl, await_evicted, ttl_db};

const DDL: &str = "create series test::s { ts: int8, n: int4 } with { time: processing, key: ts, row: { ttl: 1s } }";

#[test]
fn expired_rows_are_evicted_and_stay_gone_after_reopen() {
	// An unpartitioned series is keyed differently from a table, so its eviction path is its own.
	let path = TempDbPath::new("ttl_series_unpartitioned_reopen");

	{
		let mut db = ttl_db(&path, []);
		db.admin("create namespace test");
		db.admin(DDL);
		db.command("insert test::s [{ ts: 1, n: 1 }, { ts: 2, n: 2 }, { ts: 3, n: 3 }]");
		assert_eq!(db.row_count("from test::s"), 3);

		age_past_ttl();
		await_evicted(&db, "from test::s", 0);
		db.stop();
	}

	{
		let mut db = ttl_db(&path, []);
		assert_eq!(db.row_count("from test::s"), 0, "evicted series rows must not survive in sqlite");
		db.stop();
	}
}

#[test]
fn rows_inside_the_ttl_are_not_evicted() {
	// Nothing may be dropped on a guess about age.
	let path = TempDbPath::new("ttl_series_unpartitioned_live");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin("create series test::live { ts: int8, n: int4 } with { time: processing, key: ts, row: { ttl: 1h } }");
	db.command("insert test::live [{ ts: 1, n: 1 }, { ts: 2, n: 2 }]");

	age_past_ttl();
	assert_eq!(db.row_count("from test::live"), 2, "a 1h ttl must survive a tick seconds later");
	db.stop();
}

#[test]
fn writes_after_an_eviction_are_readable() {
	// Series metadata is rewritten in the eviction commit; a stale row count strands later writes.
	let path = TempDbPath::new("ttl_series_unpartitioned_rewrite");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(DDL);
	db.command("insert test::s [{ ts: 1, n: 1 }, { ts: 2, n: 2 }]");

	age_past_ttl();
	await_evicted(&db, "from test::s", 0);

	db.command("insert test::s [{ ts: 10, n: 10 }]");
	assert_eq!(db.row_count("from test::s"), 1, "a write after an eviction must be readable");
	db.stop();
}
