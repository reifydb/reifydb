// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Ringbuffer overflow tests for the bulk_insert module.
//!
//! Tests verify that ring buffers correctly handle capacity limits
//! and circular overflow behavior.

use reifydb_engine::test_utils::create_test_engine;
use reifydb_type::params;

use crate::{create_namespace, create_ringbuffer, query_ringbuffer, row_count, test_identity};

#[test]
fn test_ringbuffer_below_capacity() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_ringbuffer(&engine, "test", "events", 10, "id: int4"); // capacity 10

	// Insert fewer rows than capacity
	let mut builder = engine.bulk_insert(identity);
	builder.ringbuffer("test.events").row(params! { id: 1 }).row(params! { id: 2 }).row(params! { id: 3 }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.ringbuffers[0].inserted, 3);

	let frames = query_ringbuffer(&engine, "test::events");
	assert_eq!(row_count(&frames), 3);

	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	values.sort();
	assert_eq!(values, vec![1, 2, 3]);
}

#[test]
fn test_ringbuffer_at_capacity() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_ringbuffer(&engine, "test", "events", 5, "id: int4"); // capacity 5

	// Insert exactly capacity rows
	let rows: Vec<_> = (1..=5).map(|n| params! { id: n }).collect();
	let mut builder = engine.bulk_insert(identity);
	builder.ringbuffer("test.events").rows(rows).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.ringbuffers[0].inserted, 5);

	let frames = query_ringbuffer(&engine, "test::events");
	assert_eq!(row_count(&frames), 5);

	// Verify all values are present
	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	values.sort();
	assert_eq!(values, vec![1, 2, 3, 4, 5]);
}

#[test]
fn test_ringbuffer_overflow_single() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_ringbuffer(&engine, "test", "events", 3, "id: int4"); // capacity 3

	// First: fill to capacity
	let rows: Vec<_> = (1..=3).map(|n| params! { id: n }).collect();
	let mut builder = engine.bulk_insert(identity);
	builder.ringbuffer("test.events").rows(rows).done();
	builder.execute().unwrap();

	// Second: add one more (should overflow, removing oldest)
	let mut builder = engine.bulk_insert(identity);
	builder.ringbuffer("test.events").row(params! { id: 4 }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.ringbuffers[0].inserted, 1);

	// Should still have 3 rows, but oldest (1) should be removed
	let frames = query_ringbuffer(&engine, "test::events");
	assert_eq!(row_count(&frames), 3);

	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	values.sort();
	// Oldest entry (1) should be gone, 2, 3, 4 should remain
	assert_eq!(values, vec![2, 3, 4]);
}

#[test]
fn test_ringbuffer_overflow_batch() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_ringbuffer(&engine, "test", "events", 5, "id: int4"); // capacity 5

	// Insert more than capacity in one batch
	let rows: Vec<_> = (1..=8).map(|n| params! { id: n }).collect();
	let mut builder = engine.bulk_insert(identity);
	builder.ringbuffer("test.events").rows(rows).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.ringbuffers[0].inserted, 8);

	// Should have exactly capacity rows
	let frames = query_ringbuffer(&engine, "test::events");
	assert_eq!(row_count(&frames), 5);

	// Only the most recent 5 entries should remain (4, 5, 6, 7, 8)
	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	values.sort();
	assert_eq!(values, vec![4, 5, 6, 7, 8]);
}

#[test]
fn test_ringbuffer_circular_overwrite() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_ringbuffer(&engine, "test", "circular", 3, "val: int4"); // capacity 3

	// Insert multiple batches, cycling through the buffer
	for batch in 0..3 {
		let start = batch * 3 + 1;
		let rows: Vec<_> = (start..start + 3).map(|n| params! { val: n }).collect();
		let mut builder = engine.bulk_insert(identity);
		builder.ringbuffer("test.circular").rows(rows).done();
		builder.execute().unwrap();
	}

	// After 3 batches of 3 each (9 total), only last 3 should remain
	let frames = query_ringbuffer(&engine, "test::circular");
	assert_eq!(row_count(&frames), 3);

	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i32>("val").unwrap().unwrap()).collect();
	values.sort();
	// Last batch was 7, 8, 9
	assert_eq!(values, vec![7, 8, 9]);
}

#[test]
fn test_ringbuffer_incremental_fill_and_overflow() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_ringbuffer(&engine, "test", "incr", 4, "n: int4"); // capacity 4

	// Insert one at a time
	for n in 1..=6 {
		let mut builder = engine.bulk_insert(identity);
		builder.ringbuffer("test.incr").row(params! { n: n }).done();
		builder.execute().unwrap();
	}

	// After inserting 6 into capacity 4, should have 3, 4, 5, 6
	let frames = query_ringbuffer(&engine, "test::incr");
	assert_eq!(row_count(&frames), 4);

	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i32>("n").unwrap().unwrap()).collect();
	values.sort();
	assert_eq!(values, vec![3, 4, 5, 6]);
}
