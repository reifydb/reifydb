// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_engine::test_harness::TestEngine;
use reifydb_value::params;

#[test]
fn test_ringbuffer_below_capacity() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::events { id: int4 } WITH { capacity: 10 }");

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

	// Exactly at capacity must not evict; an off-by-one wrap would drop row 1 here.
	let rows: Vec<_> = (1..=5).map(|n| params! { id: n }).collect();
	let mut builder = t.bulk_insert(identity);
	builder.ringbuffer("test::events").rows(rows).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.ringbuffers[0].inserted, 5);

	let frames = t.query("FROM test::events");
	assert_eq!(TestEngine::row_count(&frames), 5);

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

	let rows: Vec<_> = (1..=3).map(|n| params! { id: n }).collect();
	let mut builder = t.bulk_insert(identity);
	builder.ringbuffer("test::events").rows(rows).done();
	builder.execute().unwrap();

	// Overflow must be reported as inserted, not as a rejected row.
	let mut builder = t.bulk_insert(identity);
	builder.ringbuffer("test::events").row(params! { id: 4 }).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.ringbuffers[0].inserted, 1);

	let frames = t.query("FROM test::events");
	assert_eq!(TestEngine::row_count(&frames), 3);

	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	values.sort();
	assert_eq!(values, vec![2, 3, 4]);
}

#[test]
fn test_ringbuffer_overflow_batch() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::events { id: int4 } WITH { capacity: 5 }");

	// A single batch larger than capacity must wrap inside the batch, not fault or truncate it.
	let rows: Vec<_> = (1..=8).map(|n| params! { id: n }).collect();
	let mut builder = t.bulk_insert(identity);
	builder.ringbuffer("test::events").rows(rows).done();
	let result = builder.execute().unwrap();

	assert_eq!(result.ringbuffers[0].inserted, 8);

	let frames = t.query("FROM test::events");
	assert_eq!(TestEngine::row_count(&frames), 5);

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

	// Three full laps of the buffer, so a write cursor that failed to wrap would be visible.
	for batch in 0..3 {
		let start = batch * 3 + 1;
		let rows: Vec<_> = (start..start + 3).map(|n| params! { val: n }).collect();
		let mut builder = t.bulk_insert(identity);
		builder.ringbuffer("test::circular").rows(rows).done();
		builder.execute().unwrap();
	}

	let frames = t.query("FROM test::circular");
	assert_eq!(TestEngine::row_count(&frames), 3);

	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i32>("val").unwrap().unwrap()).collect();
	values.sort();
	assert_eq!(values, vec![7, 8, 9]);
}

#[test]
fn test_ringbuffer_incremental_fill_and_overflow() {
	let t = TestEngine::new();
	let identity = TestEngine::identity();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::incr { n: int4 } WITH { capacity: 4 }");

	// One row per transaction, so eviction must survive across commits and not just within a batch.
	for n in 1..=6 {
		let mut builder = t.bulk_insert(identity);
		builder.ringbuffer("test::incr").row(params! { n: n }).done();
		builder.execute().unwrap();
	}

	let frames = t.query("FROM test::incr");
	assert_eq!(TestEngine::row_count(&frames), 4);

	let mut values: Vec<_> = frames[0].rows().map(|r| r.get::<i32>("n").unwrap().unwrap()).collect();
	values.sort();
	assert_eq!(values, vec![3, 4, 5, 6]);
}
