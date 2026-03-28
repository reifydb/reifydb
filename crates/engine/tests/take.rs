// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;

#[test]
fn test_table_update_take() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, val: int4 }");
	t.command(
		"INSERT test::t [{ id: 1, val: 10 }, { id: 2, val: 10 }, { id: 3, val: 10 }, { id: 4, val: 10 }, { id: 5, val: 10 }]",
	);

	let frames = t.command("UPDATE test::t { val: 99 } FILTER { id > 0 } TAKE 2 RETURNING { id }");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 2);
	let mut returned_ids: Vec<i32> = rows.iter().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	returned_ids.sort();
	assert_eq!(returned_ids, vec![4, 5]);

	let frames = t.query("FROM test::t");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 5);
	for row in &rows {
		let id = row.get::<i32>("id").unwrap().unwrap();
		let val = row.get::<i32>("val").unwrap().unwrap();
		if returned_ids.contains(&id) {
			assert_eq!(val, 99, "row id={id} should be updated");
		} else {
			assert_eq!(val, 10, "row id={id} should not be updated");
		}
	}
}

#[test]
fn test_table_delete_take() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, val: int4 }");
	t.command(
		"INSERT test::t [{ id: 1, val: 10 }, { id: 2, val: 10 }, { id: 3, val: 10 }, { id: 4, val: 10 }, { id: 5, val: 10 }]",
	);

	let frames = t.command("DELETE test::t FILTER { id > 0 } TAKE 2 RETURNING { id }");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 2);
	let mut deleted_ids: Vec<i32> = rows.iter().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	deleted_ids.sort();
	assert_eq!(deleted_ids, vec![4, 5]);

	let frames = t.query("FROM test::t");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 3);
	let mut remaining_ids: Vec<i32> = rows.iter().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	remaining_ids.sort();
	assert_eq!(remaining_ids, vec![1, 2, 3]);
}

#[test]
fn test_table_delete_take_zero() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, val: int4 }");
	t.command("INSERT test::t [{ id: 1, val: 10 }, { id: 2, val: 10 }, { id: 3, val: 10 }]");

	let frames = t.command("DELETE test::t FILTER { id > 0 } TAKE 0 RETURNING { id }");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 0);

	let frames = t.query("FROM test::t");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 3);
	let mut remaining_ids: Vec<i32> = rows.iter().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	remaining_ids.sort();
	assert_eq!(remaining_ids, vec![1, 2, 3]);
}

#[test]
fn test_table_update_take_with_returning() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, val: int4 }");
	t.command(
		"INSERT test::t [{ id: 1, val: 10 }, { id: 2, val: 10 }, { id: 3, val: 10 }, { id: 4, val: 10 }, { id: 5, val: 10 }]",
	);

	let frames = t.command("UPDATE test::t { val: 99 } FILTER { id > 0 } TAKE 2 RETURNING { id }");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 2);
	let mut returned_ids: Vec<i32> = rows.iter().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	returned_ids.sort();
	assert_eq!(returned_ids, vec![4, 5]);

	let frames = t.query("FROM test::t");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 5);
	for row in &rows {
		let id = row.get::<i32>("id").unwrap().unwrap();
		let val = row.get::<i32>("val").unwrap().unwrap();
		if returned_ids.contains(&id) {
			assert_eq!(val, 99, "row id={id} should be updated");
		} else {
			assert_eq!(val, 10, "row id={id} should not be updated");
		}
	}
}

#[test]
fn test_ringbuffer_update_take() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::rb { id: int4, val: int4 } WITH { capacity: 100 }");
	t.command(
		"INSERT test::rb [{ id: 1, val: 10 }, { id: 2, val: 10 }, { id: 3, val: 10 }, { id: 4, val: 10 }, { id: 5, val: 10 }]",
	);

	let frames = t.command("UPDATE test::rb { val: 99 } FILTER { id > 0 } TAKE 2 RETURNING { id }");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 2);
	let mut returned_ids: Vec<i32> = rows.iter().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	returned_ids.sort();
	assert_eq!(returned_ids, vec![1, 2]);

	let frames = t.query("FROM test::rb");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 5);
	for row in &rows {
		let id = row.get::<i32>("id").unwrap().unwrap();
		let val = row.get::<i32>("val").unwrap().unwrap();
		if returned_ids.contains(&id) {
			assert_eq!(val, 99, "row id={id} should be updated");
		} else {
			assert_eq!(val, 10, "row id={id} should not be updated");
		}
	}
}

#[test]
fn test_ringbuffer_delete_take() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::rb { id: int4, val: int4 } WITH { capacity: 100 }");
	t.command(
		"INSERT test::rb [{ id: 1, val: 10 }, { id: 2, val: 10 }, { id: 3, val: 10 }, { id: 4, val: 10 }, { id: 5, val: 10 }]",
	);

	let frames = t.command("DELETE test::rb FILTER { id > 0 } TAKE 2 RETURNING { id }");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 2);
	let mut deleted_ids: Vec<i32> = rows.iter().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	deleted_ids.sort();
	assert_eq!(deleted_ids, vec![1, 2]);

	let frames = t.query("FROM test::rb");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 3);
	let mut remaining_ids: Vec<i32> = rows.iter().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	remaining_ids.sort();
	assert_eq!(remaining_ids, vec![3, 4, 5]);
}

#[test]
fn test_ringbuffer_delete_take_zero() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::rb { id: int4, val: int4 } WITH { capacity: 100 }");
	t.command("INSERT test::rb [{ id: 1, val: 10 }, { id: 2, val: 10 }, { id: 3, val: 10 }]");

	let frames = t.command("DELETE test::rb FILTER { id > 0 } TAKE 0 RETURNING { id }");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 0);

	let frames = t.query("FROM test::rb");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 3);
	let mut remaining_ids: Vec<i32> = rows.iter().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	remaining_ids.sort();
	assert_eq!(remaining_ids, vec![1, 2, 3]);
}

#[test]
fn test_ringbuffer_update_take_with_returning() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::rb { id: int4, val: int4 } WITH { capacity: 100 }");
	t.command(
		"INSERT test::rb [{ id: 1, val: 10 }, { id: 2, val: 10 }, { id: 3, val: 10 }, { id: 4, val: 10 }, { id: 5, val: 10 }]",
	);

	let frames = t.command("UPDATE test::rb { val: 99 } FILTER { id > 0 } TAKE 2 RETURNING { id }");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 2);
	let mut returned_ids: Vec<i32> = rows.iter().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	returned_ids.sort();
	assert_eq!(returned_ids, vec![1, 2]);

	let frames = t.query("FROM test::rb");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 5);
	for row in &rows {
		let id = row.get::<i32>("id").unwrap().unwrap();
		let val = row.get::<i32>("val").unwrap().unwrap();
		if returned_ids.contains(&id) {
			assert_eq!(val, 99, "row id={id} should be updated");
		} else {
			assert_eq!(val, 10, "row id={id} should not be updated");
		}
	}
}

#[test]
fn test_series_update_take() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE SERIES test::s { ts: int8, val: int8 } WITH { key: ts }");
	t.command(
		"INSERT test::s [{ ts: 1000, val: 10 }, { ts: 2000, val: 10 }, { ts: 3000, val: 10 }, { ts: 4000, val: 10 }, { ts: 5000, val: 10 }]",
	);

	let frames = t.command("UPDATE test::s { val: 99 } FILTER { ts > 0 } TAKE 2 RETURNING { ts }");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 2);
	let mut returned_ts: Vec<i64> = rows.iter().map(|r| r.get::<i64>("ts").unwrap().unwrap()).collect();
	returned_ts.sort();
	assert_eq!(returned_ts, vec![4000, 5000]);

	let frames = t.query("FROM test::s");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 5);
	for row in &rows {
		let ts = row.get::<i64>("ts").unwrap().unwrap();
		let val = row.get::<i64>("val").unwrap().unwrap();
		if returned_ts.contains(&ts) {
			assert_eq!(val, 99, "row ts={ts} should be updated");
		} else {
			assert_eq!(val, 10, "row ts={ts} should not be updated");
		}
	}
}

#[test]
fn test_series_delete_take() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE SERIES test::s { ts: int8, val: int8 } WITH { key: ts }");
	t.command(
		"INSERT test::s [{ ts: 1000, val: 10 }, { ts: 2000, val: 10 }, { ts: 3000, val: 10 }, { ts: 4000, val: 10 }, { ts: 5000, val: 10 }]",
	);

	let frames = t.command("DELETE test::s FILTER { ts > 0 } TAKE 2 RETURNING { ts }");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 2);
	let mut deleted_ts: Vec<i64> = rows.iter().map(|r| r.get::<i64>("ts").unwrap().unwrap()).collect();
	deleted_ts.sort();
	assert_eq!(deleted_ts, vec![4000, 5000]);

	let frames = t.query("FROM test::s");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 3);
	let mut remaining_ts: Vec<i64> = rows.iter().map(|r| r.get::<i64>("ts").unwrap().unwrap()).collect();
	remaining_ts.sort();
	assert_eq!(remaining_ts, vec![1000, 2000, 3000]);
}

#[test]
fn test_series_delete_take_zero() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE SERIES test::s { ts: int8, val: int8 } WITH { key: ts }");
	t.command("INSERT test::s [{ ts: 1000, val: 10 }, { ts: 2000, val: 10 }, { ts: 3000, val: 10 }]");

	let frames = t.command("DELETE test::s FILTER { ts > 0 } TAKE 0 RETURNING { ts }");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 0);

	let frames = t.query("FROM test::s");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 3);
	let mut remaining_ts: Vec<i64> = rows.iter().map(|r| r.get::<i64>("ts").unwrap().unwrap()).collect();
	remaining_ts.sort();
	assert_eq!(remaining_ts, vec![1000, 2000, 3000]);
}

#[test]
fn test_series_update_take_with_returning() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE SERIES test::s { ts: int8, val: int8 } WITH { key: ts }");
	t.command(
		"INSERT test::s [{ ts: 1000, val: 10 }, { ts: 2000, val: 10 }, { ts: 3000, val: 10 }, { ts: 4000, val: 10 }, { ts: 5000, val: 10 }]",
	);

	let frames = t.command("UPDATE test::s { val: 99 } FILTER { ts > 0 } TAKE 2 RETURNING { ts }");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 2);
	let mut returned_ts: Vec<i64> = rows.iter().map(|r| r.get::<i64>("ts").unwrap().unwrap()).collect();
	returned_ts.sort();
	assert_eq!(returned_ts, vec![4000, 5000]);

	let frames = t.query("FROM test::s");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 5);
	for row in &rows {
		let ts = row.get::<i64>("ts").unwrap().unwrap();
		let val = row.get::<i64>("val").unwrap().unwrap();
		if returned_ts.contains(&ts) {
			assert_eq!(val, 99, "row ts={ts} should be updated");
		} else {
			assert_eq!(val, 10, "row ts={ts} should not be updated");
		}
	}
}
