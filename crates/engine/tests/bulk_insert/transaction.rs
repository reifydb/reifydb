// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_engine::test_harness::TestEngine;
use reifydb_value::params;

#[test]
fn test_commit_on_success() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::commits { id: int4, val: utf8 }");

	let mut builder = t.bulk_insert(identity);
	builder.table("test::commits")
		.row(params! { id: 1, val: "first" })
		.row(params! { id: 2, val: "second" })
		.done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 2);

	// execute() must commit; a builder that only staged would report inserted and persist nothing.
	let frames = t.query("FROM test::commits");
	assert_eq!(TestEngine::row_count(&frames), 2);

	let mut values: Vec<_> = frames[0]
		.rows()
		.map(|r| (r.get::<i32>("id").unwrap().unwrap(), r.get::<String>("val").unwrap().unwrap()))
		.collect();
	values.sort_by_key(|(id, _)| *id);
	assert_eq!(values, vec![(1, "first".to_string()), (2, "second".to_string())]);
}

#[test]
fn test_rollback_on_error_namespace_not_found() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::data { id: int4 }");

	// The valid insert is staged first, so a per-target commit would leak it past the failure.
	let mut builder = t.bulk_insert(identity);
	builder.table("test::data").row(params! { id: 1 }).done();
	builder.table("nonexistent::table").row(params! { id: 2 }).done();
	let result = builder.execute();

	assert!(result.is_err());

	let frames = t.query("FROM test::data");
	assert_eq!(TestEngine::row_count(&frames), 0, "First insert should be rolled back");
}

#[test]
fn test_rollback_on_error_table_not_found() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::real { x: int4 }");

	// A missing table inside an existing namespace must roll the batch back like a missing
	// namespace does; the two resolve on different paths.
	let mut builder = t.bulk_insert(identity);
	builder.table("test::real").row(params! { x: 100 }).done();
	builder.table("test::fake").row(params! { x: 200 }).done();
	let result = builder.execute();

	assert!(result.is_err());

	let frames = t.query("FROM test::real");
	assert_eq!(TestEngine::row_count(&frames), 0, "First insert should be rolled back");
}

#[test]
fn test_multiple_tables_all_succeed() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t1 { a: int4 }");
	t.admin("CREATE TABLE test::t2 { b: int4 }");
	t.admin("CREATE TABLE test::t3 { c: int4 }");

	// Per-target counts must stay attributed to their own target, not summed across the batch.
	let mut builder = t.bulk_insert(identity);
	builder.table("test::t1").row(params! { a: 1 }).done();
	builder.table("test::t2").row(params! { b: 2 }).row(params! { b: 3 }).done();
	builder.table("test::t3").row(params! { c: 4 }).row(params! { c: 5 }).row(params! { c: 6 }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables.len(), 3);
	assert_eq!(result.tables[0].inserted, 1);
	assert_eq!(result.tables[1].inserted, 2);
	assert_eq!(result.tables[2].inserted, 3);

	let frames1 = t.query("FROM test::t1");
	let frames2 = t.query("FROM test::t2");
	let frames3 = t.query("FROM test::t3");
	assert_eq!(TestEngine::row_count(&frames1), 1);
	assert_eq!(TestEngine::row_count(&frames2), 2);
	assert_eq!(TestEngine::row_count(&frames3), 3);
}

#[test]
fn test_mixed_tables_and_ringbuffers_atomic() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::atomic_table { id: int4 }");
	t.admin("CREATE RINGBUFFER test::atomic_rb { seq: int4 } WITH { capacity: 100 }");

	// Tables and ringbuffers write through different paths but must share one transaction.
	let mut builder = t.bulk_insert(identity);
	builder.table("test::atomic_table").row(params! { id: 10 }).row(params! { id: 20 }).done();
	builder.ringbuffer("test::atomic_rb").row(params! { seq: 100 }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 2);
	assert_eq!(result.ringbuffers[0].inserted, 1);

	let table_frames = t.query("FROM test::atomic_table");
	let rb_frames = t.query("FROM test::atomic_rb");
	assert_eq!(TestEngine::row_count(&table_frames), 2);
	assert_eq!(TestEngine::row_count(&rb_frames), 1);
}

#[test]
fn test_rollback_mixed_batch_on_error() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::rollback_tbl { val: int4 }");
	t.admin("CREATE RINGBUFFER test::rollback_rb { data: int4 } WITH { capacity: 100 }");

	// A ringbuffer write must roll back with the batch; its capacity eviction is destructive,
	// so a partial commit would be unrecoverable.
	let mut builder = t.bulk_insert(identity);
	builder.table("test::rollback_tbl").row(params! { val: 1 }).done();
	builder.ringbuffer("test::rollback_rb").row(params! { data: 2 }).done();
	builder.table("invalid::namespace").row(params! { x: 3 }).done();
	let result = builder.execute();

	assert!(result.is_err());

	let table_frames = t.query("FROM test::rollback_tbl");
	let rb_frames = t.query("FROM test::rollback_rb");
	assert_eq!(TestEngine::row_count(&table_frames), 0, "Table should be rolled back");
	assert_eq!(TestEngine::row_count(&rb_frames), 0, "Ringbuffer should be rolled back");
}
