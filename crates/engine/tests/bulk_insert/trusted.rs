// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Trusted mode tests for the bulk_insert module.
//!
//! Tests verify that trusted mode skips validation and coercion.

use reifydb_engine::test_utils::create_test_engine;
use reifydb_type::params;

use crate::{create_namespace, create_table, query_table, row_count, test_identity};

#[test]
fn test_trusted_mode_basic_insert() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_table(&engine, "test", "trusted_tbl", "id: int4, name: utf8");

	// Use bulk_insert_trusted instead of bulk_insert
	let mut builder = engine.bulk_insert_trusted(identity);
	builder.table("test::trusted_tbl")
		.row(params! { id: 1i32, name: "Alice" })
		.row(params! { id: 2i32, name: "Bob" })
		.done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 2);

	let frames = query_table(&engine, "test::trusted_tbl");
	assert_eq!(row_count(&frames), 2);

	let mut values: Vec<_> = frames[0]
		.rows()
		.map(|r| (r.get::<i32>("id").unwrap().unwrap(), r.get::<String>("name").unwrap().unwrap()))
		.collect();
	values.sort_by_key(|(id, _)| *id);
	assert_eq!(values, vec![(1, "Alice".to_string()), (2, "Bob".to_string())]);
}

#[test]
fn test_trusted_mode_ringbuffer() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	crate::create_ringbuffer(&engine, "test", "trusted_rb", 100, "seq: int4, data: utf8");

	let mut builder = engine.bulk_insert_trusted(identity);
	builder.ringbuffer("test::trusted_rb")
		.row(params! { seq: 1i32, data: "first" })
		.row(params! { seq: 2i32, data: "second" })
		.done();
	let result = builder.execute().unwrap();

	assert_eq!(result.ringbuffers[0].inserted, 2);

	let frames = crate::query_ringbuffer(&engine, "test::trusted_rb");
	assert_eq!(row_count(&frames), 2);

	// Verify values
	let mut values: Vec<_> = frames[0]
		.rows()
		.map(|r| (r.get::<i32>("seq").unwrap().unwrap(), r.get::<String>("data").unwrap().unwrap()))
		.collect();
	values.sort_by_key(|(seq, _)| *seq);
	assert_eq!(values, vec![(1, "first".to_string()), (2, "second".to_string())]);
}

#[test]
fn test_trusted_mode_mixed_batch() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_table(&engine, "test", "t1", "a: int4");
	create_table(&engine, "test", "t2", "b: int4");
	crate::create_ringbuffer(&engine, "test", "rb1", 50, "c: int4");

	let mut builder = engine.bulk_insert_trusted(identity);
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
fn test_trusted_mode_large_batch() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_table(&engine, "test", "large", "n: int4");

	// Insert 1000 rows in trusted mode for performance
	let rows: Vec<_> = (1..=1000).map(|n| params! { n: n as i32 }).collect();

	let mut builder = engine.bulk_insert_trusted(identity);
	builder.table("test::large").rows(rows).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 1000);

	let frames = query_table(&engine, "test::large");
	assert_eq!(row_count(&frames), 1000);
}
