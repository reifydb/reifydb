// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::{engine::StandardEngine, test_utils::create_test_engine};
use reifydb_type::value::{frame::frame::Frame, identity::IdentityId};

fn root() -> IdentityId {
	IdentityId::root()
}

fn admin(engine: &StandardEngine, rql: &str) -> Vec<Frame> {
	engine.admin_as(root(), rql, Default::default()).unwrap()
}

fn command(engine: &StandardEngine, rql: &str) -> Vec<Frame> {
	engine.command_as(root(), rql, Default::default()).unwrap()
}

fn query(engine: &StandardEngine, rql: &str) -> Vec<Frame> {
	engine.query_as(root(), rql, Default::default()).unwrap()
}

#[test]
fn test_table_insert_returning() {
	let engine = create_test_engine();
	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE TABLE test::t { id: int4, name: utf8 }");

	let frames = command(
		&engine,
		r#"INSERT test::t [{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }] RETURNING { id, name }"#,
	);
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 2);
	assert_eq!(rows[0].get::<i32>("id").unwrap().unwrap(), 1);
	assert_eq!(rows[0].get::<String>("name").unwrap().unwrap(), "Alice");
	assert_eq!(rows[1].get::<i32>("id").unwrap().unwrap(), 2);
	assert_eq!(rows[1].get::<String>("name").unwrap().unwrap(), "Bob");
}

#[test]
fn test_table_update_returning() {
	let engine = create_test_engine();
	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE TABLE test::t { id: int4, name: utf8 }");
	command(&engine, r#"INSERT test::t [{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }]"#);

	let frames =
		command(&engine, r#"UPDATE test::t { name: "Updated" } FILTER { id == 1 } RETURNING { id, name }"#);
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i32>("id").unwrap().unwrap(), 1);
	assert_eq!(rows[0].get::<String>("name").unwrap().unwrap(), "Updated");
}

#[test]
fn test_table_delete_returning() {
	let engine = create_test_engine();
	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE TABLE test::t { id: int4, name: utf8 }");
	command(&engine, r#"INSERT test::t [{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }]"#);

	let frames = command(&engine, r#"DELETE test::t FILTER { id == 1 } RETURNING { id, name }"#);
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i32>("id").unwrap().unwrap(), 1);
	assert_eq!(rows[0].get::<String>("name").unwrap().unwrap(), "Alice");

	// Verify row is actually deleted
	let frames = query(&engine, "FROM test::t");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i32>("id").unwrap().unwrap(), 2);
}

#[test]
fn test_ringbuffer_insert_returning() {
	let engine = create_test_engine();
	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE RINGBUFFER test::rb { a: int4, b: utf8 } WITH { capacity: 10 }");

	let frames = command(&engine, r#"INSERT test::rb [{ a: 1, b: "x" }] RETURNING { a, b }"#);
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i32>("a").unwrap().unwrap(), 1);
	assert_eq!(rows[0].get::<String>("b").unwrap().unwrap(), "x");
}

#[test]
fn test_ringbuffer_update_returning() {
	let engine = create_test_engine();
	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE RINGBUFFER test::rb { a: int4, b: utf8 } WITH { capacity: 10 }");
	command(&engine, r#"INSERT test::rb [{ a: 1, b: "x" }]"#);

	let frames = command(&engine, r#"UPDATE test::rb { b: "y" } FILTER { a == 1 } RETURNING { a, b }"#);
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i32>("a").unwrap().unwrap(), 1);
	assert_eq!(rows[0].get::<String>("b").unwrap().unwrap(), "y");
}

#[test]
fn test_ringbuffer_delete_returning() {
	let engine = create_test_engine();
	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE RINGBUFFER test::rb { a: int4, b: utf8 } WITH { capacity: 10 }");
	command(&engine, r#"INSERT test::rb [{ a: 1, b: "x" }]"#);

	let frames = command(&engine, r#"DELETE test::rb FILTER { a == 1 } RETURNING { a, b }"#);
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i32>("a").unwrap().unwrap(), 1);
	assert_eq!(rows[0].get::<String>("b").unwrap().unwrap(), "x");
}

#[test]
fn test_table_insert_returning_computed() {
	let engine = create_test_engine();
	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE TABLE test::t { price: int4, qty: int4 }");

	let frames = command(&engine, "INSERT test::t [{ price: 10, qty: 3 }] RETURNING { total: price * qty }");
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i64>("total").unwrap().unwrap(), 30);
}

#[test]
fn test_table_update_returning_empty() {
	let engine = create_test_engine();
	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE TABLE test::t { id: int4, name: utf8 }");
	command(&engine, r#"INSERT test::t [{ id: 1, name: "Alice" }]"#);

	let frames = command(&engine, r#"UPDATE test::t { name: "X" } FILTER { id == 999 } RETURNING { id }"#);
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 0);
}

#[test]
fn test_table_insert_without_returning() {
	let engine = create_test_engine();
	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE TABLE test::t { id: int4, name: utf8 }");

	let frames = command(&engine, r#"INSERT test::t [{ id: 1, name: "Alice" }]"#);
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<String>("namespace").unwrap().unwrap(), "test");
	assert_eq!(rows[0].get::<String>("table").unwrap().unwrap(), "t");
	assert_eq!(rows[0].get::<u64>("inserted").unwrap().unwrap(), 1);
}

#[test]
fn test_series_insert_returning() {
	let engine = create_test_engine();
	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE SERIES test::s { ts: int8, val: int8 } WITH { key: ts }");

	let frames = command(&engine, "INSERT test::s [{ ts: 1000, val: 42 }] RETURNING { ts, val }");
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i64>("ts").unwrap().unwrap(), 1000);
	assert_eq!(rows[0].get::<i64>("val").unwrap().unwrap(), 42);
}

#[test]
fn test_series_update_returning() {
	let engine = create_test_engine();
	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE SERIES test::s { ts: int8, val: int8 } WITH { key: ts }");
	command(&engine, "INSERT test::s [{ ts: 1000, val: 42 }]");

	let frames = command(&engine, "UPDATE test::s { val: 99 } FILTER { ts == 1000 } RETURNING { ts, val }");
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i64>("ts").unwrap().unwrap(), 1000);
	assert_eq!(rows[0].get::<i64>("val").unwrap().unwrap(), 99);
}

#[test]
fn test_series_delete_returning() {
	let engine = create_test_engine();
	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE SERIES test::s { ts: int8, val: int8 } WITH { key: ts }");
	command(&engine, "INSERT test::s [{ ts: 1000, val: 42 }]");

	let frames = command(&engine, "DELETE test::s FILTER { ts == 1000 } RETURNING { ts, val }");
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i64>("ts").unwrap().unwrap(), 1000);
	assert_eq!(rows[0].get::<i64>("val").unwrap().unwrap(), 42);
}

#[test]
fn test_dictionary_insert_returning() {
	let engine = create_test_engine();
	admin(&engine, "CREATE NAMESPACE test");
	admin(&engine, "CREATE DICTIONARY test::d FOR Utf8 AS Uint8");

	let frames = command(&engine, r#"INSERT test::d [{ value: "hello" }] RETURNING { id, value }"#);
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<String>("value").unwrap().unwrap(), "hello");
	assert!(rows[0].get::<u64>("id").unwrap().unwrap() > 0);
}
