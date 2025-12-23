// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Transaction atomicity tests for the bulk_insert module.
//!
//! Tests verify that bulk inserts are atomic - all succeed or all fail.

use reifydb_type::params;

use crate::{create_namespace, create_table, create_test_engine, query_table, row_count, test_identity};

#[tokio::test]
async fn test_commit_on_success() {
	let engine = create_test_engine().await;
	let identity = test_identity();

	create_namespace(&engine, "test").await;
	create_table(&engine, "test", "commits", "id: int4, val: utf8").await;

	// Insert some rows
	let mut builder = engine.bulk_insert(&identity);
	builder.table("test.commits").row(params! { id: 1, val: "first" }).row(params! { id: 2, val: "second" }).done();
	let result = builder.execute().await.unwrap();

	assert_eq!(result.tables[0].inserted, 2);

	// Data should be persisted after commit
	let frames = query_table(&engine, "test.commits").await;
	assert_eq!(row_count(&frames), 2);

	// Verify values survive query
	let mut values: Vec<_> = frames[0]
		.rows()
		.map(|r| (r.get::<i32>("id").unwrap().unwrap(), r.get::<String>("val").unwrap().unwrap()))
		.collect();
	values.sort_by_key(|(id, _)| *id);
	assert_eq!(values, vec![(1, "first".to_string()), (2, "second".to_string())]);
}

#[tokio::test]
async fn test_rollback_on_error_namespace_not_found() {
	let engine = create_test_engine().await;
	let identity = test_identity();

	create_namespace(&engine, "test").await;
	create_table(&engine, "test", "data", "id: int4").await;

	// Insert into valid table first, then invalid namespace (should fail entire batch)
	let mut builder = engine.bulk_insert(&identity);
	builder.table("test.data").row(params! { id: 1 }).done();
	builder.table("nonexistent.table").row(params! { id: 2 }).done(); // This should fail
	let result = builder.execute().await;

	assert!(result.is_err());

	let frames = query_table(&engine, "test.data").await;
	assert_eq!(row_count(&frames), 0, "First insert should be rolled back");
}

#[tokio::test]
async fn test_rollback_on_error_table_not_found() {
	let engine = create_test_engine().await;
	let identity = test_identity();

	create_namespace(&engine, "test").await;
	create_table(&engine, "test", "real", "x: int4").await;

	// Insert into valid table, then nonexistent table
	let mut builder = engine.bulk_insert(&identity);
	builder.table("test.real").row(params! { x: 100 }).done();
	builder.table("test.fake").row(params! { x: 200 }).done(); // This should fail
	let result = builder.execute().await;

	assert!(result.is_err());

	// The real table should NOT have data due to rollback
	let frames = query_table(&engine, "test.real").await;
	assert_eq!(row_count(&frames), 0, "First insert should be rolled back");
}

#[tokio::test]
async fn test_multiple_tables_all_succeed() {
	let engine = create_test_engine().await;
	let identity = test_identity();

	create_namespace(&engine, "test").await;
	create_table(&engine, "test", "t1", "a: int4").await;
	create_table(&engine, "test", "t2", "b: int4").await;
	create_table(&engine, "test", "t3", "c: int4").await;

	// Insert into multiple tables in one batch
	let mut builder = engine.bulk_insert(&identity);
	builder.table("test.t1").row(params! { a: 1 }).done();
	builder.table("test.t2").row(params! { b: 2 }).row(params! { b: 3 }).done();
	builder.table("test.t3").row(params! { c: 4 }).row(params! { c: 5 }).row(params! { c: 6 }).done();
	let result = builder.execute().await.unwrap();

	assert_eq!(result.tables.len(), 3);
	assert_eq!(result.tables[0].inserted, 1);
	assert_eq!(result.tables[1].inserted, 2);
	assert_eq!(result.tables[2].inserted, 3);

	// Verify all tables have data
	let frames1 = query_table(&engine, "test.t1").await;
	let frames2 = query_table(&engine, "test.t2").await;
	let frames3 = query_table(&engine, "test.t3").await;
	assert_eq!(row_count(&frames1), 1);
	assert_eq!(row_count(&frames2), 2);
	assert_eq!(row_count(&frames3), 3);
}

#[tokio::test]
async fn test_mixed_tables_and_ringbuffers_atomic() {
	let engine = create_test_engine().await;
	let identity = test_identity();

	create_namespace(&engine, "test").await;
	create_table(&engine, "test", "atomic_table", "id: int4").await;
	crate::create_ringbuffer(&engine, "test", "atomic_rb", 100, "seq: int4").await;

	// Insert into both table and ringbuffer in one batch
	let mut builder = engine.bulk_insert(&identity);
	builder.table("test.atomic_table").row(params! { id: 10 }).row(params! { id: 20 }).done();
	builder.ringbuffer("test.atomic_rb").row(params! { seq: 100 }).done();
	let result = builder.execute().await.unwrap();

	assert_eq!(result.tables[0].inserted, 2);
	assert_eq!(result.ringbuffers[0].inserted, 1);

	// Verify both have data
	let table_frames = query_table(&engine, "test.atomic_table").await;
	let rb_frames = crate::query_ringbuffer(&engine, "test.atomic_rb").await;
	assert_eq!(row_count(&table_frames), 2);
	assert_eq!(row_count(&rb_frames), 1);
}

#[tokio::test]
async fn test_rollback_mixed_batch_on_error() {
	let engine = create_test_engine().await;
	let identity = test_identity();

	create_namespace(&engine, "test").await;
	create_table(&engine, "test", "rollback_tbl", "val: int4").await;
	crate::create_ringbuffer(&engine, "test", "rollback_rb", 100, "data: int4").await;

	// Insert into valid table and ringbuffer, then fail on invalid namespace
	let mut builder = engine.bulk_insert(&identity);
	builder.table("test.rollback_tbl").row(params! { val: 1 }).done();
	builder.ringbuffer("test.rollback_rb").row(params! { data: 2 }).done();
	builder.table("invalid.namespace").row(params! { x: 3 }).done(); // This should fail
	let result = builder.execute().await;

	assert!(result.is_err());

	// Both table and ringbuffer should be empty due to rollback
	let table_frames = query_table(&engine, "test.rollback_tbl").await;
	let rb_frames = crate::query_ringbuffer(&engine, "test.rollback_rb").await;
	assert_eq!(row_count(&table_frames), 0, "Table should be rolled back");
	assert_eq!(row_count(&rb_frames), 0, "Ringbuffer should be rolled back");
}
