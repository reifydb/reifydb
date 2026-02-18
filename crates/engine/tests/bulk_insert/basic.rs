// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Basic API tests for the bulk_insert module.
//!
//! Tests cover core functionality: table inserts, ringbuffer inserts,
//! different parameter styles, and result verification.

use reifydb_engine::test_utils::create_test_engine;
use reifydb_type::params;

use crate::{
	create_namespace, create_ringbuffer, create_table, query_ringbuffer, query_table, row_count, test_identity,
};

#[test]
fn test_table_insert_named_params() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_table(&engine, "test", "users", "id: int4, name: utf8");

	let mut builder = engine.bulk_insert(&identity);
	builder.table("test.users").row(params! { id: 1, name: "Alice" }).row(params! { id: 2, name: "Bob" }).done();

	let result = builder.execute().unwrap();

	assert_eq!(result.tables.len(), 1);
	assert_eq!(result.tables[0].namespace, "test");
	assert_eq!(result.tables[0].table, "users");
	assert_eq!(result.tables[0].inserted, 2);

	let frames = query_table(&engine, "test.users");
	assert_eq!(row_count(&frames), 2);

	let mut values: Vec<_> = frames[0]
		.rows()
		.map(|r| (r.get::<i32>("id").unwrap().unwrap(), r.get::<String>("name").unwrap().unwrap()))
		.collect();
	values.sort_by_key(|(id, _)| *id);
	assert_eq!(values, vec![(1, "Alice".to_string()), (2, "Bob".to_string())]);
}

#[test]
fn test_table_insert_positional_params() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_table(&engine, "test", "items", "id: int4, value: float8");

	let mut builder = engine.bulk_insert(&identity);
	builder.table("test.items").row(params![1, 10.5]).row(params![2, 20.5]).row(params![3, 30.5]).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 3);

	let frames = query_table(&engine, "test.items");
	assert_eq!(row_count(&frames), 3);

	let mut values: Vec<_> = frames[0]
		.rows()
		.map(|r| (r.get::<i32>("id").unwrap().unwrap(), r.get::<f64>("value").unwrap().unwrap()))
		.collect();

	values.sort_by_key(|(id, _)| *id);
	assert_eq!(values.len(), 3);
	assert_eq!(values[0], (1, 10.5));
	assert_eq!(values[1], (2, 20.5));
	assert_eq!(values[2], (3, 30.5));
}

#[test]
fn test_table_insert_multiple_rows_chained() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_table(&engine, "test", "data", "x: int4");

	let mut builder = engine.bulk_insert(&identity);
	builder.table("test.data")
		.row(params! { x: 1 })
		.row(params! { x: 2 })
		.row(params! { x: 3 })
		.row(params! { x: 4 })
		.row(params! { x: 5 })
		.done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 5);
}

#[test]
fn test_table_insert_rows_iterator() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_table(&engine, "test", "batch", "n: int4");

	let rows: Vec<_> = (1..=10).map(|n| params! { n: n }).collect();

	let mut builder = engine.bulk_insert(&identity);
	builder.table("test.batch").rows(rows).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 10);
}

#[test]
fn test_ringbuffer_insert_basic() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_ringbuffer(&engine, "test", "events", 100, "id: int4, msg: utf8");

	let mut builder = engine.bulk_insert(&identity);
	builder.ringbuffer("test.events")
		.row(params! { id: 1, msg: "event1" })
		.row(params! { id: 2, msg: "event2" })
		.done();
	let result = builder.execute().unwrap();

	assert_eq!(result.ringbuffers.len(), 1);
	assert_eq!(result.ringbuffers[0].namespace, "test");
	assert_eq!(result.ringbuffers[0].ringbuffer, "events");
	assert_eq!(result.ringbuffers[0].inserted, 2);

	let frames = query_ringbuffer(&engine, "test.events");
	assert_eq!(row_count(&frames), 2);

	let mut values: Vec<_> = frames[0]
		.rows()
		.map(|r| (r.get::<i32>("id").unwrap().unwrap(), r.get::<String>("msg").unwrap().unwrap()))
		.collect();
	values.sort_by_key(|(id, _)| *id);
	assert_eq!(values, vec![(1, "event1".to_string()), (2, "event2".to_string())]);
}

#[test]
fn test_mixed_table_and_ringbuffer() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_table(&engine, "test", "logs", "id: int4");
	create_ringbuffer(&engine, "test", "stream", 50, "seq: int4");

	let mut builder = engine.bulk_insert(&identity);
	builder.table("test.logs").row(params! { id: 1 }).row(params! { id: 2 }).done();
	builder.ringbuffer("test.stream")
		.row(params! { seq: 100 })
		.row(params! { seq: 101 })
		.row(params! { seq: 102 })
		.done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables.len(), 1);
	assert_eq!(result.tables[0].inserted, 2);
	assert_eq!(result.ringbuffers.len(), 1);
	assert_eq!(result.ringbuffers[0].inserted, 3);

	// Verify table values (order-independent)
	let table_frames = query_table(&engine, "test.logs");
	let mut table_ids: Vec<_> = table_frames[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	table_ids.sort();
	assert_eq!(table_ids, vec![1, 2]);

	// Verify ringbuffer values (order-independent)
	let rb_frames = query_ringbuffer(&engine, "test.stream");
	let mut rb_seqs: Vec<_> = rb_frames[0].rows().map(|r| r.get::<i32>("seq").unwrap().unwrap()).collect();
	rb_seqs.sort();
	assert_eq!(rb_seqs, vec![100, 101, 102]);
}

#[test]
fn test_multiple_tables() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_table(&engine, "test", "table_a", "a: int4");
	create_table(&engine, "test", "table_b", "b: int4");

	let mut builder = engine.bulk_insert(&identity);
	builder.table("test.table_a").row(params! { a: 1 }).done();
	builder.table("test.table_b").row(params! { b: 2 }).row(params! { b: 3 }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables.len(), 2);
	assert_eq!(result.tables[0].table, "table_a");
	assert_eq!(result.tables[0].inserted, 1);
	assert_eq!(result.tables[1].table, "table_b");
	assert_eq!(result.tables[1].inserted, 2);

	// Verify table_a values
	let frames_a = query_table(&engine, "test.table_a");
	let values_a: Vec<_> = frames_a[0].rows().map(|r| r.get::<i32>("a").unwrap().unwrap()).collect();
	assert_eq!(values_a, vec![1]);

	// Verify table_b values (order-independent)
	let frames_b = query_table(&engine, "test.table_b");
	let mut values_b: Vec<_> = frames_b[0].rows().map(|r| r.get::<i32>("b").unwrap().unwrap()).collect();
	values_b.sort();
	assert_eq!(values_b, vec![2, 3]);
}

#[test]
fn test_qualified_name_with_namespace() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "myns");
	create_table(&engine, "myns", "mytable", "val: int4");

	let mut builder = engine.bulk_insert(&identity);
	builder.table("myns.mytable").row(params! { val: 42 }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].namespace, "myns");
	assert_eq!(result.tables[0].table, "mytable");
	assert_eq!(result.tables[0].inserted, 1);
}

#[test]
fn test_qualified_name_default_namespace() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_table(&engine, "default", "simple", "x: int4");

	let mut builder = engine.bulk_insert(&identity);
	builder.table("simple").row(params! { x: 1 }).done(); // No namespace prefix, should use "default"
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].namespace, "default");
	assert_eq!(result.tables[0].table, "simple");
	assert_eq!(result.tables[0].inserted, 1);
}

#[test]
fn test_empty_insert() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_table(&engine, "test", "empty", "id: int4");

	let mut builder = engine.bulk_insert(&identity);
	builder.table("test.empty").done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables.len(), 1);
	assert_eq!(result.tables[0].inserted, 0);

	let frames = query_table(&engine, "test.empty");
	assert_eq!(row_count(&frames), 0);
}

#[test]
fn test_single_row_insert() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_table(&engine, "test", "single", "id: int4, data: utf8");

	let mut builder = engine.bulk_insert(&identity);
	builder.table("test.single").row(params! { id: 1, data: "only one" }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.tables[0].inserted, 1);

	// Verify actual values
	let frames = query_table(&engine, "test.single");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows[0].get::<i32>("id").unwrap(), Some(1));
	assert_eq!(rows[0].get::<String>("data").unwrap(), Some("only one".to_string()));
}

#[test]
fn test_result_structure() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "ns1");
	create_namespace(&engine, "ns2");
	create_table(&engine, "ns1", "t1", "a: int4");
	create_table(&engine, "ns2", "t2", "b: int4");
	create_ringbuffer(&engine, "ns1", "rb1", 10, "c: int4");

	let mut builder = engine.bulk_insert(&identity);
	builder.table("ns1.t1").row(params! { a: 1 }).row(params! { a: 2 }).done();
	builder.table("ns2.t2").row(params! { b: 3 }).done();
	builder.ringbuffer("ns1.rb1").row(params! { c: 4 }).row(params! { c: 5 }).row(params! { c: 6 }).done();
	let result = builder.execute().unwrap();

	// Verify tables result
	assert_eq!(result.tables.len(), 2);

	assert_eq!(result.tables[0].namespace, "ns1");
	assert_eq!(result.tables[0].table, "t1");
	assert_eq!(result.tables[0].inserted, 2);

	assert_eq!(result.tables[1].namespace, "ns2");
	assert_eq!(result.tables[1].table, "t2");
	assert_eq!(result.tables[1].inserted, 1);

	// Verify ringbuffers result
	assert_eq!(result.ringbuffers.len(), 1);
	assert_eq!(result.ringbuffers[0].namespace, "ns1");
	assert_eq!(result.ringbuffers[0].ringbuffer, "rb1");
	assert_eq!(result.ringbuffers[0].inserted, 3);
}
