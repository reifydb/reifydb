// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;

#[test]
fn test_ringbuffer_below_capacity() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::events { id: int4 } WITH { capacity: 10 }");

	// Insert fewer rows than capacity
	let mut builder = t.bulk_insert(identity);
	builder.ringbuffer("test::events").row(params! { id: 1 }).row(params! { id: 2 }).row(params! { id: 3 }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.ringbuffers[0].inserted, 3);

	let frames = t.query("FROM test::events");
	assert_eq!(TestEngine::row_count(&frames), 3);

	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	values.sort();
	assert_eq!(values, vec![1, 2, 3]);
}

#[test]
fn test_ringbuffer_at_capacity() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::events { id: int4 } WITH { capacity: 5 }");

	// Insert exactly capacity rows
	let rows: Vec<_> = (1..=5).map(|n| params! { id: n }).collect();
	let mut builder = t.bulk_insert(identity);
	builder.ringbuffer("test::events").rows(rows).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.ringbuffers[0].inserted, 5);

	let frames = t.query("FROM test::events");
	assert_eq!(TestEngine::row_count(&frames), 5);

	// Verify all values are present
	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	values.sort();
	assert_eq!(values, vec![1, 2, 3, 4, 5]);
}

#[test]
fn test_ringbuffer_overflow_single() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::events { id: int4 } WITH { capacity: 3 }");

	// First: fill to capacity
	let rows: Vec<_> = (1..=3).map(|n| params! { id: n }).collect();
	let mut builder = t.bulk_insert(identity);
	builder.ringbuffer("test::events").rows(rows).done();
	builder.execute().unwrap();

	// Second: add one more (should overflow, removing oldest)
	let mut builder = t.bulk_insert(identity);
	builder.ringbuffer("test::events").row(params! { id: 4 }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.ringbuffers[0].inserted, 1);

	// Should still have 3 rows, but oldest (1) should be removed
	let frames = t.query("FROM test::events");
	assert_eq!(TestEngine::row_count(&frames), 3);

	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	values.sort();
	// Oldest entry (1) should be gone, 2, 3, 4 should remain
	assert_eq!(values, vec![2, 3, 4]);
}

#[test]
fn test_ringbuffer_overflow_batch() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::events { id: int4 } WITH { capacity: 5 }");

	// Insert more than capacity in one batch
	let rows: Vec<_> = (1..=8).map(|n| params! { id: n }).collect();
	let mut builder = t.bulk_insert(identity);
	builder.ringbuffer("test::events").rows(rows).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.ringbuffers[0].inserted, 8);

	// Should have exactly capacity rows
	let frames = t.query("FROM test::events");
	assert_eq!(TestEngine::row_count(&frames), 5);

	// Only the most recent 5 entries should remain (4, 5, 6, 7, 8)
	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	values.sort();
	assert_eq!(values, vec![4, 5, 6, 7, 8]);
}

#[test]
fn test_ringbuffer_circular_overwrite() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::circular { val: int4 } WITH { capacity: 3 }");

	// Insert multiple batches, cycling through the buffer
	for batch in 0..3 {
		let start = batch * 3 + 1;
		let rows: Vec<_> = (start..start + 3).map(|n| params! { val: n }).collect();
		let mut builder = t.bulk_insert(identity);
		builder.ringbuffer("test::circular").rows(rows).done();
		builder.execute().unwrap();
	}

	// After 3 batches of 3 each (9 total), only last 3 should remain
	let frames = t.query("FROM test::circular");
	assert_eq!(TestEngine::row_count(&frames), 3);

	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i32>("val").unwrap().unwrap()).collect();
	values.sort();
	// Last batch was 7, 8, 9
	assert_eq!(values, vec![7, 8, 9]);
}

#[test]
fn test_ringbuffer_incremental_fill_and_overflow() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::incr { n: int4 } WITH { capacity: 4 }");

	// Insert one at a time
	for n in 1..=6 {
		let mut builder = t.bulk_insert(identity);
		builder.ringbuffer("test::incr").row(params! { n: n }).done();
		builder.execute().unwrap();
	}

	// After inserting 6 into capacity 4, should have 3, 4, 5, 6
	let frames = t.query("FROM test::incr");
	assert_eq!(TestEngine::row_count(&frames), 4);

	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i32>("n").unwrap().unwrap()).collect();
	values.sort();
	assert_eq!(values, vec![3, 4, 5, 6]);
}
