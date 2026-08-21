// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{params, value::frame::frame::Frame};

fn collect_n(frames: &[Frame]) -> Vec<i32> {
	let mut out: Vec<i32> =
		frames.iter().flat_map(|f| f.rows().map(|r| r.get::<i32>("n").unwrap().unwrap())).collect();
	out.sort();
	out
}

#[test]
fn bulk_inserted_partitioned_series_rows_are_reachable_through_the_partitioned_read_path() {
	// The bulk path had no partitioned branch and wrote every row under an unpartitioned series
	// key, while every reader of a partitioned series scans the partitioned keyspace. The rows
	// were durable and unreachable at the same time, which reads as silent data loss.
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin(
		"CREATE SERIES test::s { ts: int8, region: utf8, n: int4 } WITH { key: ts, partition: { by: { region } } }",
	);

	let mut builder = t.bulk_insert(identity);
	builder.series("test::s")
		.row(params! { ts: 10, region: "us", n: 1 })
		.row(params! { ts: 20, region: "eu", n: 2 })
		.row(params! { ts: 30, region: "us", n: 3 })
		.done();
	let result = builder.execute().unwrap();

	assert_eq!(result.series[0].inserted, 3);
	assert_eq!(collect_n(&t.query("FROM test::s")), vec![1, 2, 3], "an object-wide scan must see every row");
	assert_eq!(
		collect_n(&t.query("FROM test::s FILTER region == \"us\"")),
		vec![1, 3],
		"a pruned partition scan must find the rows the bulk path wrote into that partition"
	);
	assert_eq!(collect_n(&t.query("FROM test::s FILTER region == \"eu\"")), vec![2]);
}

#[test]
fn bulk_inserted_partitioned_series_rows_answer_a_pushed_key_span() {
	// The pruned scan bounds the key span inside the partition prefix, so a row written under the
	// wrong prefix or with a mis-sequenced key falls outside the window rather than failing loudly.
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin(
		"CREATE SERIES test::s { ts: int8, region: utf8, n: int4 } WITH { key: ts, partition: { by: { region } } }",
	);

	let mut builder = t.bulk_insert(identity);
	builder.series("test::s")
		.row(params! { ts: 10, region: "us", n: 1 })
		.row(params! { ts: 20, region: "us", n: 2 })
		.row(params! { ts: 30, region: "us", n: 3 })
		.row(params! { ts: 20, region: "eu", n: 4 })
		.done();
	builder.execute().unwrap();

	assert_eq!(
		collect_n(&t.query("FROM test::s FILTER region == \"us\" and ts >= 20")),
		vec![2, 3],
		"the pushed span must keep exactly the in-range rows of the pruned partition"
	);
	assert_eq!(
		collect_n(&t.query("FROM test::s FILTER region == \"us\" and ts >= 20 and ts <= 20")),
		vec![2],
		"a single-key span must not pick up the other partition's row at the same key"
	);
}

#[test]
fn bulk_inserted_unpartitioned_series_rows_stay_reachable() {
	// The partitioned branch is selected off the series definition, so a series without partition
	// columns must keep taking the plain series key.
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE SERIES test::s { ts: int8, n: int4 } WITH { key: ts }");

	let mut builder = t.bulk_insert(identity);
	builder.series("test::s").row(params! { ts: 10, n: 1 }).row(params! { ts: 20, n: 2 }).done();
	builder.execute().unwrap();

	assert_eq!(collect_n(&t.query("FROM test::s")), vec![1, 2]);
	assert_eq!(collect_n(&t.query("FROM test::s FILTER ts >= 20")), vec![2]);
}
