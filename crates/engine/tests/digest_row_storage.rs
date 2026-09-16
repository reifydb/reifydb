// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::execution::ExecutionResult;
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{Value, frame::frame::Frame, value_type::ValueType},
};

const AGGREGATE: &str = "FROM test::src | aggregate { d: stats::digest(v, 0.01) } by { k }";

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::src { k: int4, v: float8, n: int4 }");
	let rows: Vec<String> = (0..300)
		.map(|i| {
			let magnitude = 1.03f64.powi(i % 200);
			let v = if i % 3 == 0 {
				-magnitude
			} else {
				magnitude
			};
			format!("{{ k: {}, v: {v}, n: {i} }}", i % 2)
		})
		.collect();
	t.command(&format!("INSERT test::src [{}]", rows.join(", ")));
	t
}

fn run(result: ExecutionResult) -> Result<Vec<Frame>, Diagnostic> {
	match result.error {
		Some(e) => Err(e.diagnostic()),
		None => Ok(result.frames),
	}
}

fn admin(t: &TestEngine, rql: &str) -> Result<Vec<Frame>, Diagnostic> {
	run(t.inner().admin_as(TestEngine::identity(), rql, Params::None))
}

fn command(t: &TestEngine, rql: &str) -> Result<Vec<Frame>, Diagnostic> {
	run(t.inner().command_as(TestEngine::identity(), rql, Params::None))
}

fn column_values(frames: &[Frame], name: &str) -> Vec<Value> {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let column = frames[0].columns.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("no column {name}"));
	(0..column.data.len()).map(|row| column.data.get_value(row)).collect()
}

fn column_type(frames: &[Frame], name: &str) -> ValueType {
	let column = frames[0].columns.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("no column {name}"));
	column.data.get_type()
}

fn digest_type() -> ValueType {
	ValueType::Digest {
		inner: Box::new(ValueType::Float8),
		accuracy: 10_000,
	}
}

#[test]
fn a_digest_inserted_from_a_query_reads_back_equal_to_the_query_result() {
	// A stored digest that differs from the one the query built would move every later percentile.
	let t = engine();
	t.admin("CREATE TABLE test::t { k: int4, d: digest(float8, 0.01) }");
	let expected = t.query(&format!("{AGGREGATE} | sort {{ k }}"));

	command(&t, &format!("LET $agg = {AGGREGATE}; INSERT test::t $agg")).unwrap();

	let stored = t.query("FROM test::t | sort { k }");
	assert_eq!(column_type(&stored, "d"), digest_type());
	assert_eq!(column_values(&stored, "k"), column_values(&expected, "k"));
	let digests = column_values(&stored, "d");
	assert_eq!(digests.len(), 2);
	assert_eq!(digests, column_values(&expected, "d"));

	let from_storage = t.query("FROM test::t | sort { k } | map { k, p: stats::approx_percentile(d, 0.99) }");
	let from_query =
		t.query(&format!("{AGGREGATE} | sort {{ k }} | map {{ k, p: stats::approx_percentile(d, 0.99) }}"));
	assert_eq!(column_values(&from_storage, "p"), column_values(&from_query, "p"));
}

#[test]
fn inserting_a_digest_of_another_accuracy_or_inner_type_is_an_error_not_a_panic() {
	// A mismatched digest must be refused on the write, never reach the row encoder that panics on it.
	let t = engine();
	t.admin("CREATE TABLE test::t { k: int4, d: digest(float8, 0.01) }");
	let mismatches = [
		"FROM test::src | aggregate { d: stats::digest(v, 0.05) } by { k }",
		"FROM test::src | aggregate { d: stats::digest(n, 0.01) } by { k }",
	];
	for source in mismatches {
		let err = command(&t, &format!("LET $agg = {source}; INSERT test::t $agg")).expect_err(source);
		assert!(err.message.to_lowercase().contains("digest"), "{source}: {err:?}");
	}
	assert_eq!(TestEngine::row_count(&t.query("FROM test::t")), 0, "a refused write must not leave a row behind");
}

#[test]
fn partitioning_a_ringbuffer_by_a_digest_column_is_an_error_not_a_panic() {
	// A digest has no key encoding, so the partition metadata key must never be built from one.
	let t = engine();
	let created = admin(
		&t,
		"CREATE RINGBUFFER test::rb { d: digest(float8, 0.01) } WITH { capacity: 2, partition: { by: { d } } }",
	);
	let inserted = created
		.and_then(|_| command(&t, &format!("LET $agg = {AGGREGATE} | map {{ d }}; INSERT test::rb $agg")));

	let err = inserted.expect_err("a digest partition column must be rejected");
	assert_eq!(err.code, "PART_005", "{err:?}");
	assert!(err.message.contains("`d`"), "{err:?}");
}

#[test]
fn every_partitioned_storage_kind_rejects_a_digest_partition_column_at_create_time() {
	// A kind that let a digest partition through would build a partition from a value with no equality.
	let cases = [
		"CREATE TABLE test::x { d: digest(float8, 0.01) } WITH { partition: { by: { d } } }",
		"CREATE TABLE test::x { n: int4, d: Option(digest(int4, 0.05)) } WITH { partition: { by: { n, d } } }",
		"CREATE SERIES test::x { ts: int8, d: digest(float8, 0.01) } WITH { key: ts, partition: { by: { d } } }",
		"CREATE RINGBUFFER test::x { d: digest(duration, 0.01) } WITH { capacity: 2, partition: { by: { d } } }",
		"CREATE DEFERRED VIEW test::x { k: int4, d: digest(float8, 0.01) } WITH { partition: { by: { d } } } AS { FROM test::src | aggregate { d: stats::digest(v, 0.01) } by { k } }",
		"CREATE DEFERRED RINGBUFFER VIEW test::x { k: int4, d: digest(float8, 0.01) } WITH { capacity: 2, partition: { by: { d } } } AS { FROM test::src | aggregate { d: stats::digest(v, 0.01) } by { k } }",
		"CREATE DEFERRED SERIES VIEW test::x { ts: int8, d: digest(float8, 0.01) } WITH { key: ts, partition: { by: { d } } } AS { FROM test::src | map { ts: k, d: v } }",
		"CREATE TRANSACTIONAL RINGBUFFER VIEW test::x { k: int4, d: digest(float8, 0.01) } WITH { capacity: 2, partition: { by: { d } } } AS { FROM test::src | aggregate { d: stats::digest(v, 0.01) } by { k } }",
	];
	for rql in cases {
		let t = engine();
		let err = admin(&t, rql).expect_err(rql);
		assert_eq!(err.code, "PART_005", "{rql}: {err:?}");
		assert!(err.message.contains("`d`"), "{rql}: {err:?}");
	}
}

#[test]
fn a_digest_column_next_to_a_partition_column_is_still_allowed() {
	// The check must look at the partition columns only, or every partitioned table loses digest columns.
	let t = engine();
	t.admin(
		"CREATE RINGBUFFER test::rb { k: int4, d: digest(float8, 0.01) } WITH { capacity: 4, partition: { by: { k } } }",
	);

	command(&t, &format!("LET $agg = {AGGREGATE}; INSERT test::rb $agg")).unwrap();

	let stored = t.query("FROM test::rb | sort { k }");
	let expected = t.query(&format!("{AGGREGATE} | sort {{ k }}"));
	assert_eq!(column_values(&stored, "d"), column_values(&expected, "d"));
}
