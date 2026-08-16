// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TempDbPath;

use crate::storage::ttl::{STRADDLE_TTL_SECS, age_past, age_past_ttl, await_evicted, await_survivor, ttl_db};

const DDL: &str = "create ringbuffer test::rb { region: utf8, n: int4 } with { time: processing, capacity: 100, row: { ttl: 1s }, partition: { by: { region } } }";

#[test]
fn expired_rows_are_evicted_from_every_partition_and_stay_gone_after_reopen() {
	// Every partition carries its own count and head, so each must be rewritten in the same pass.
	let path = TempDbPath::new("ttl_ringbuffer_partitioned_reopen");

	{
		let mut db = ttl_db(&path, []);
		db.admin("create namespace test");
		db.admin(DDL);
		db.command(
			"insert test::rb [{ region: \"us\", n: 1 }, { region: \"eu\", n: 2 }, { region: \"us\", n: 3 }]",
		);
		assert_eq!(db.row_count("from test::rb"), 3);

		age_past_ttl();
		await_evicted(&db, "from test::rb", 0);
		db.stop();
	}

	{
		let mut db = ttl_db(&path, []);
		assert_eq!(
			db.row_count("from test::rb"),
			0,
			"no partition may resurrect rows from the persistent tier"
		);
		db.stop();
	}
}

#[test]
fn a_partition_written_after_the_cutoff_survives_while_the_older_one_is_evicted() {
	// One index answer spans every partition, so a count applied to the wrong one corrupts its head.
	let path = TempDbPath::new("ttl_ringbuffer_partitioned_straddle");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(
		"create ringbuffer test::rb { region: utf8, n: int4 } with { time: processing, capacity: 100, row: { ttl: 10s }, partition: { by: { region } } }",
	);
	db.command("insert test::rb [{ region: \"old\", n: 1 }, { region: \"old\", n: 2 }]");

	age_past(STRADDLE_TTL_SECS);
	db.command("insert test::rb [{ region: \"new\", n: 3 }]");

	await_survivor(&db, "from test::rb", 1);
	assert_eq!(
		db.row_count("from test::rb filter region == \"new\""),
		1,
		"only the partition written after the cutoff may survive"
	);
	assert_eq!(db.row_count("from test::rb filter region == \"old\""), 0, "the aged partition must be emptied");
	db.stop();
}

#[test]
fn writes_after_an_eviction_are_readable_in_every_partition() {
	// A head left pointing past the live rows strands every later write in that partition.
	let path = TempDbPath::new("ttl_ringbuffer_partitioned_rewrite");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(DDL);
	db.command("insert test::rb [{ region: \"us\", n: 1 }, { region: \"eu\", n: 2 }, { region: \"us\", n: 3 }]");

	age_past_ttl();
	await_evicted(&db, "from test::rb", 0);

	db.command("insert test::rb [{ region: \"us\", n: 10 }, { region: \"eu\", n: 11 }]");
	assert_eq!(db.row_count("from test::rb filter region == \"us\""), 1, "the us partition must accept writes");
	assert_eq!(db.row_count("from test::rb filter region == \"eu\""), 1, "the eu partition must accept writes");
	db.stop();
}

#[test]
fn a_partition_left_untouched_keeps_its_rows_when_another_is_evicted() {
	// Grouping by partition is what keeps one partition's removals off another's metadata.
	let path = TempDbPath::new("ttl_ringbuffer_partitioned_isolation");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(
		"create ringbuffer test::rb { region: utf8, n: int4 } with { time: processing, capacity: 100, row: { ttl: 10s }, partition: { by: { region } } }",
	);
	db.command("insert test::rb [{ region: \"aged\", n: 1 }, { region: \"aged\", n: 2 }]");

	age_past(STRADDLE_TTL_SECS);
	db.command("insert test::rb [{ region: \"fresh\", n: 3 }, { region: \"fresh\", n: 4 }]");

	await_survivor(&db, "from test::rb", 2);
	assert_eq!(
		db.row_count("from test::rb filter region == \"fresh\""),
		2,
		"the untouched partition must keep every row"
	);
	db.stop();
}
