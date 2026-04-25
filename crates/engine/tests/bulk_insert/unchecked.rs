// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Unchecked mode tests for the bulk_insert module.
//!
//! Tests verify that unchecked mode skips validation/coercion and the OCC
//! conflict-detection registration. See `bulk_insert_unchecked` for the
//! safety contract these tests assume.

use reifydb_engine::test_prelude::*;

#[test]
fn test_unchecked_mode_basic_insert() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::unchecked_tbl { id: int4, name: utf8 }");

	let mut builder = t.bulk_insert_unchecked(identity);
	builder.table("test::unchecked_tbl")
		.row(params! { id: 1i32, name: "Alice" })
		.row(params! { id: 2i32, name: "Bob" })
		.done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 2);

	let frames = t.query("FROM test::unchecked_tbl");
	assert_eq!(TestEngine::row_count(&frames), 2);

	let mut values: Vec<_> = frames[0]
		.rows()
		.map(|r| (r.get::<i32>("id").unwrap().unwrap(), r.get::<String>("name").unwrap().unwrap()))
		.collect();
	values.sort_by_key(|(id, _)| *id);
	assert_eq!(values, vec![(1, "Alice".to_string()), (2, "Bob".to_string())]);
}

#[test]
fn test_unchecked_mode_ringbuffer() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::unchecked_rb { seq: int4, data: utf8 } WITH { capacity: 100 }");

	let mut builder = t.bulk_insert_unchecked(identity);
	builder.ringbuffer("test::unchecked_rb")
		.row(params! { seq: 1i32, data: "first" })
		.row(params! { seq: 2i32, data: "second" })
		.done();
	let result = builder.execute().unwrap();

	assert_eq!(result.ringbuffers[0].inserted, 2);

	let frames = t.query("FROM test::unchecked_rb");
	assert_eq!(TestEngine::row_count(&frames), 2);

	let mut values: Vec<_> = frames[0]
		.rows()
		.map(|r| (r.get::<i32>("seq").unwrap().unwrap(), r.get::<String>("data").unwrap().unwrap()))
		.collect();
	values.sort_by_key(|(seq, _)| *seq);
	assert_eq!(values, vec![(1, "first".to_string()), (2, "second".to_string())]);
}

#[test]
fn test_unchecked_mode_mixed_batch() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t1 { a: int4 }");
	t.admin("CREATE TABLE test::t2 { b: int4 }");
	t.admin("CREATE RINGBUFFER test::rb1 { c: int4 } WITH { capacity: 50 }");

	let mut builder = t.bulk_insert_unchecked(identity);
	builder.table("test::t1").row(params! { a: 10i32 }).done();
	builder.table("test::t2").row(params! { b: 20i32 }).row(params! { b: 30i32 }).done();
	builder.ringbuffer("test::rb1").row(params! { c: 100i32 }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables.len(), 2);
	assert_eq!(result.tables[0].inserted, 1);
	assert_eq!(result.tables[1].inserted, 2);
	assert_eq!(result.ringbuffers.len(), 1);
	assert_eq!(result.ringbuffers[0].inserted, 1);
}

#[test]
fn test_unchecked_mode_large_batch() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::large { n: int4 }");

	let rows: Vec<_> = (1..=1000).map(|n| params! { n: n as i32 }).collect();

	let mut builder = t.bulk_insert_unchecked(identity);
	builder.table("test::large").rows(rows).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 1000);

	let frames = t.query("FROM test::large");
	assert_eq!(TestEngine::row_count(&frames), 1000);
}
