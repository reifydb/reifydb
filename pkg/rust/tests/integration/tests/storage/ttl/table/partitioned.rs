// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TempDbPath;

use crate::storage::ttl::{STRADDLE_TTL_SECS, age_past, age_past_ttl, await_evicted, await_survivor, ttl_db};

const DDL: &str = "create table test::t { region: utf8, n: int4 } with { time: processing, row: { ttl: 1s }, partition: { by: { region } } }";

#[test]
fn expired_rows_are_evicted_from_every_partition_and_stay_gone_after_reopen() {
	// A partitioned table uses a different keyspace, so it needs its own durability proof.
	let path = TempDbPath::new("ttl_table_partitioned_reopen");

	{
		let mut db = ttl_db(&path, []);
		db.admin("create namespace test");
		db.admin(DDL);
		db.command(
			"insert test::t [{ region: \"us\", n: 1 }, { region: \"eu\", n: 2 }, { region: \"us\", n: 3 }]",
		);
		assert_eq!(db.row_count("from test::t"), 3);

		age_past_ttl();
		await_evicted(&db, "from test::t", 0);
		db.stop();
	}

	{
		let mut db = ttl_db(&path, []);
		assert_eq!(db.row_count("from test::t"), 0, "no partition may resurrect rows from the persistent tier");
		db.stop();
	}
}

#[test]
fn a_partition_written_after_the_cutoff_survives_while_the_older_one_is_evicted() {
	// Partitions share one expiry index, so a misapplied answer would take live rows too.
	let path = TempDbPath::new("ttl_table_partitioned_straddle");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin("create table test::t { region: utf8, n: int4 } with { time: processing, row: { ttl: 10s }, partition: { by: { region } } }");
	db.command("insert test::t [{ region: \"old\", n: 1 }, { region: \"old\", n: 2 }]");

	age_past(STRADDLE_TTL_SECS);
	db.command("insert test::t [{ region: \"new\", n: 3 }]");

	await_survivor(&db, "from test::t", 1);
	assert_eq!(
		db.row_count("from test::t filter region == \"new\""),
		1,
		"only the partition written after the cutoff may survive"
	);
	assert_eq!(db.row_count("from test::t filter region == \"old\""), 0, "the aged partition must be emptied");
	db.stop();
}

#[test]
fn rows_inside_the_ttl_are_not_evicted_from_any_partition() {
	// A per-object cutoff instead of a per-row one would empty every partition at once.
	let path = TempDbPath::new("ttl_table_partitioned_live");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(
		"create table test::live { region: utf8, n: int4 } with { time: processing, row: { ttl: 1h }, partition: { by: { region } } }",
	);
	db.command("insert test::live [{ region: \"us\", n: 1 }, { region: \"eu\", n: 2 }]");

	age_past_ttl();
	assert_eq!(db.row_count("from test::live"), 2, "a 1h ttl must outlive a tick that runs seconds later");
	db.stop();
}
