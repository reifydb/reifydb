// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration as StdDuration;

use reifydb::{
	WithSubsystem, embedded,
	testing::db::{TestDb, await_value},
};

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|c| c).build().expect("build memory db with flow"))
}

fn err_code(db: &TestDb, rql: &str) -> String {
	match db.try_command(rql) {
		Ok(_) => panic!("expected command to fail, but it succeeded\nrql: {rql}"),
		Err(e) => e.diagnostic().code.clone(),
	}
}

fn agg_group(db: &TestDb, region: &str) -> Option<(i64, i32)> {
	// None means the group has no row at all: a full retraction must make it disappear, which is
	// observably different from a lingering zero row.
	let rql = format!("FROM test::agg FILTER region == \"{region}\"");
	let frames = db.query(&rql);
	for f in &frames {
		if f.row_count() > 0 {
			let c = f.get::<i64>("c", 0).expect("get c").expect("c defined");
			let s = f.get::<i32>("s", 0).expect("get s").expect("s defined");
			return Some((c, s));
		}
	}
	None
}

fn await_agg_group(db: &TestDb, region: &str, want: Option<(i64, i32)>) -> Option<(i64, i32)> {
	await_value(want, StdDuration::from_secs(5), || agg_group(db, region))
}

fn create_events_table(db: &TestDb) {
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::events { region: utf8, n: int4 }");
}

fn create_agg_over_rb(db: &TestDb) {
	db.admin("CREATE DEFERRED VIEW test::agg { region: utf8, c: int8, s: int4 } \
		 AS { FROM test::rb AGGREGATE { c: math::count(region), s: math::sum(n) } BY { region } }");
}

#[test]
fn global_eviction_retracts_the_downstream_aggregate() {
	// The non-partitioned head-counter path. Rows go in one statement at a time so each eviction
	// removes a prior batch's row, which is the storage read-back path.
	let db = setup();
	create_events_table(&db);
	db.admin("CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2 } AS { FROM test::events }");
	create_agg_over_rb(&db);

	db.command("INSERT test::events [{ region: \"us\", n: 1 }]");
	db.command("INSERT test::events [{ region: \"us\", n: 2 }]");
	assert_eq!(
		await_agg_group(&db, "us", Some((2, 3))),
		Some((2, 3)),
		"both us rows fit the buffer, so the aggregate sees both"
	);

	db.command("INSERT test::events [{ region: \"eu\", n: 3 }]");
	db.command("INSERT test::events [{ region: \"eu\", n: 4 }]");

	assert_eq!(
		await_agg_group(&db, "us", None),
		None,
		"every us row was evicted, so the us group must be retracted, not left stale or zeroed"
	);
	assert_eq!(
		await_agg_group(&db, "eu", Some((2, 7))),
		Some((2, 7)),
		"the surviving eu rows must aggregate normally"
	);
}

#[test]
fn partitioned_eviction_retracts_only_that_partitions_contribution() {
	// The per-partition marker path, a separate code path from the global head counter.
	let db = setup();
	create_events_table(&db);
	db.admin("CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } \
		 AS { FROM test::events }");
	create_agg_over_rb(&db);

	db.command("INSERT test::events [{ region: \"us\", n: 1 }]");
	db.command("INSERT test::events [{ region: \"us\", n: 2 }]");
	db.command("INSERT test::events [{ region: \"eu\", n: 3 }]");
	assert_eq!(await_agg_group(&db, "us", Some((2, 3))), Some((2, 3)), "us starts with both rows");

	db.command("INSERT test::events [{ region: \"us\", n: 4 }]");
	db.command("INSERT test::events [{ region: \"us\", n: 5 }]");

	assert_eq!(
		await_agg_group(&db, "us", Some((2, 9))),
		Some((2, 9)),
		"us evicted n=1 and n=2, so its aggregate must reflect only n=4 and n=5"
	);
	assert_eq!(
		await_agg_group(&db, "eu", Some((1, 3))),
		Some((1, 3)),
		"eu never evicted, so its aggregate must be untouched by us's evictions"
	);
}

#[test]
fn global_within_batch_overflow_nets_to_capacity() {
	// A batch larger than capacity evicts rows assigned earlier in the same batch and never
	// stored, so the remove has to net them out of the insert diff. No TTL is configured, which
	// also pins that the default is propagate-on.
	let db = setup();
	create_events_table(&db);
	db.admin("CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2 } AS { FROM test::events }");
	create_agg_over_rb(&db);

	db.command(
		"INSERT test::events [{ region: \"us\", n: 1 }, { region: \"us\", n: 2 }, { region: \"us\", n: 3 }]",
	);

	assert_eq!(
		await_agg_group(&db, "us", Some((2, 5))),
		Some((2, 5)),
		"n=1 was inserted and evicted within one batch; downstream must net to the surviving n=2 and n=3"
	);
}

#[test]
fn partitioned_within_batch_overflow_nets_to_capacity() {
	// The same within-batch overflow per partition, with a quiet partition in the batch as the
	// control, and again with no row TTL configured.
	let db = setup();
	create_events_table(&db);
	db.admin("CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } AS { FROM test::events }");
	create_agg_over_rb(&db);

	db.command(
		"INSERT test::events [{ region: \"us\", n: 1 }, { region: \"us\", n: 2 }, \
		 { region: \"us\", n: 3 }, { region: \"eu\", n: 4 }]",
	);

	assert_eq!(
		await_agg_group(&db, "us", Some((2, 5))),
		Some((2, 5)),
		"us overflowed within the batch and must net to its surviving n=2 and n=3"
	);
	assert_eq!(
		await_agg_group(&db, "eu", Some((1, 4))),
		Some((1, 4)),
		"eu stayed under capacity and must be unaffected by us's within-batch eviction"
	);
}

#[test]
fn update_driven_partition_move_is_rejected_and_downstream_is_unaffected() {
	// A cross-partition move is refused at compile time, so it never reaches the eviction path and
	// neither partition's membership nor the aggregate over them changes.
	let db = setup();
	create_events_table(&db);
	db.admin("CREATE DEFERRED RINGBUFFER VIEW test::rb { region: utf8, n: int4 } \
		 WITH { capacity: 2, partition: { by: { region } } } AS { FROM test::events }");
	create_agg_over_rb(&db);

	db.command(
		"INSERT test::events [{ region: \"eu\", n: 10 }, { region: \"eu\", n: 20 }, \
		 { region: \"us\", n: 1 }, { region: \"us\", n: 2 }]",
	);
	assert_eq!(await_agg_group(&db, "eu", Some((2, 30))), Some((2, 30)), "eu starts with both its rows");
	assert_eq!(await_agg_group(&db, "us", Some((2, 3))), Some((2, 3)), "us starts with both its rows");

	assert_eq!(
		err_code(&db, "UPDATE test::events { region: \"eu\" } FILTER n == 1"),
		"PART_004",
		"a cross-partition move must be rejected"
	);

	assert_eq!(
		await_agg_group(&db, "eu", Some((2, 30))),
		Some((2, 30)),
		"eu must be unaffected by the rejected update"
	);
	assert_eq!(
		await_agg_group(&db, "us", Some((2, 3))),
		Some((2, 3)),
		"us must be unaffected by the rejected update"
	);
}
