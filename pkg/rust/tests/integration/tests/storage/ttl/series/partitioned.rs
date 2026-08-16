// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TempDbPath;

use crate::storage::ttl::{STRADDLE_TTL_SECS, age_past, age_past_ttl, await_evicted, await_survivor, ttl_db};

const DDL: &str = "create series test::s { ts: int8, region: utf8, n: int4 } with { time: processing, key: ts, row: { ttl: 1s }, partition: { by: { region } } }";

#[test]
fn expired_rows_are_evicted_from_every_partition_and_stay_gone_after_reopen() {
	// A partitioned series is scanned object-wide, so every partition must drain in the same pass.
	let path = TempDbPath::new("ttl_series_partitioned_reopen");

	{
		let mut db = ttl_db(&path, []);
		db.admin("create namespace test");
		db.admin(DDL);
		db.command(
			"insert test::s [{ ts: 1, region: \"us\", n: 1 }, { ts: 2, region: \"eu\", n: 2 }, { ts: 3, region: \"us\", n: 3 }]",
		);
		assert_eq!(db.row_count("from test::s"), 3);

		age_past_ttl();
		await_evicted(&db, "from test::s", 0);
		db.stop();
	}

	{
		let mut db = ttl_db(&path, []);
		assert_eq!(db.row_count("from test::s"), 0, "no partition may resurrect rows from the persistent tier");
		db.stop();
	}
}

#[test]
fn a_partition_written_after_the_cutoff_survives_while_the_older_one_is_evicted() {
	// One expiry index serves every partition, so a misapplied answer would take live rows too.
	let path = TempDbPath::new("ttl_series_partitioned_straddle");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin("create series test::s { ts: int8, region: utf8, n: int4 } with { time: processing, key: ts, row: { ttl: 10s }, partition: { by: { region } } }");
	db.command("insert test::s [{ ts: 1, region: \"old\", n: 1 }, { ts: 2, region: \"old\", n: 2 }]");

	age_past(STRADDLE_TTL_SECS);
	db.command("insert test::s [{ ts: 3, region: \"new\", n: 3 }]");

	await_survivor(&db, "from test::s", 1);
	assert_eq!(
		db.row_count("from test::s filter region == \"new\""),
		1,
		"only the partition written after the cutoff may survive"
	);
	assert_eq!(db.row_count("from test::s filter region == \"old\""), 0, "the aged partition must be emptied");
	db.stop();
}

#[test]
fn writes_after_an_eviction_are_readable_in_every_partition() {
	// Series metadata is rewritten in the eviction commit; a stale count strands later writes.
	let path = TempDbPath::new("ttl_series_partitioned_rewrite");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(DDL);
	db.command("insert test::s [{ ts: 1, region: \"us\", n: 1 }, { ts: 2, region: \"eu\", n: 2 }]");

	age_past_ttl();
	await_evicted(&db, "from test::s", 0);

	db.command("insert test::s [{ ts: 10, region: \"us\", n: 10 }, { ts: 11, region: \"eu\", n: 11 }]");
	assert_eq!(db.row_count("from test::s"), 2, "both partitions must accept writes after an eviction");
	db.stop();
}
