// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::datetime::DateTime;

#[test]
fn test_table_insert_returning() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, name: utf8 }");

	let frames = t
		.command(r#"INSERT test::t [{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }] RETURNING { id, name }"#);
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
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, name: utf8 }");
	t.command(r#"INSERT test::t [{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }]"#);

	let frames = t.command(r#"UPDATE test::t { name: "Updated" } FILTER { id == 1 } RETURNING { id, name }"#);
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i32>("id").unwrap().unwrap(), 1);
	assert_eq!(rows[0].get::<String>("name").unwrap().unwrap(), "Updated");
}

#[test]
fn test_table_delete_returning() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, name: utf8 }");
	t.command(r#"INSERT test::t [{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }]"#);

	let frames = t.command(r#"DELETE test::t FILTER { id == 1 } RETURNING { id, name }"#);
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i32>("id").unwrap().unwrap(), 1);
	assert_eq!(rows[0].get::<String>("name").unwrap().unwrap(), "Alice");

	// RETURNING must describe a delete that really happened, not just echo the matched row.
	let frames = t.query("FROM test::t");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i32>("id").unwrap().unwrap(), 2);
}

#[test]
fn test_ringbuffer_insert_returning() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::rb { a: int4, b: utf8 } WITH { capacity: 10 }");

	let frames = t.command(r#"INSERT test::rb [{ a: 1, b: "x" }] RETURNING { a, b }"#);
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i32>("a").unwrap().unwrap(), 1);
	assert_eq!(rows[0].get::<String>("b").unwrap().unwrap(), "x");
}

#[test]
fn test_ringbuffer_update_returning() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::rb { a: int4, b: utf8 } WITH { capacity: 10 }");
	t.command(r#"INSERT test::rb [{ a: 1, b: "x" }]"#);

	let frames = t.command(r#"UPDATE test::rb { b: "y" } FILTER { a == 1 } RETURNING { a, b }"#);
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i32>("a").unwrap().unwrap(), 1);
	assert_eq!(rows[0].get::<String>("b").unwrap().unwrap(), "y");
}

#[test]
fn test_ringbuffer_delete_returning() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE RINGBUFFER test::rb { a: int4, b: utf8 } WITH { capacity: 10 }");
	t.command(r#"INSERT test::rb [{ a: 1, b: "x" }]"#);

	let frames = t.command(r#"DELETE test::rb FILTER { a == 1 } RETURNING { a, b }"#);
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i32>("a").unwrap().unwrap(), 1);
	assert_eq!(rows[0].get::<String>("b").unwrap().unwrap(), "x");
}

#[test]
fn test_table_insert_returning_computed() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { price: int4, qty: int4 }");

	let frames = t.command("INSERT test::t [{ price: 10, qty: 3 }] RETURNING { total: price * qty }");
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i64>("total").unwrap().unwrap(), 30);
}

#[test]
fn test_table_update_returning_empty() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, name: utf8 }");
	t.command(r#"INSERT test::t [{ id: 1, name: "Alice" }]"#);

	let frames = t.command(r#"UPDATE test::t { name: "X" } FILTER { id == 999 } RETURNING { id }"#);
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 0);
}

#[test]
fn test_table_insert_without_returning() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, name: utf8 }");

	let frames = t.command(r#"INSERT test::t [{ id: 1, name: "Alice" }]"#);
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<String>("namespace").unwrap().unwrap(), "test");
	assert_eq!(rows[0].get::<String>("table").unwrap().unwrap(), "t");
	assert_eq!(rows[0].get::<u64>("inserted").unwrap().unwrap(), 1);
}

#[test]
fn test_series_insert_returning() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE SERIES test::s { ts: int8, val: int8 } WITH { key: ts }");

	let frames = t.command("INSERT test::s [{ ts: 1000, val: 42 }] RETURNING { ts, val }");
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i64>("ts").unwrap().unwrap(), 1000);
	assert_eq!(rows[0].get::<i64>("val").unwrap().unwrap(), 42);
}

#[test]
fn test_series_update_returning() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE SERIES test::s { ts: int8, val: int8 } WITH { key: ts }");
	t.command("INSERT test::s [{ ts: 1000, val: 42 }]");

	let frames = t.command("UPDATE test::s { val: 99 } FILTER { ts == 1000 } RETURNING { ts, val }");
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i64>("ts").unwrap().unwrap(), 1000);
	assert_eq!(rows[0].get::<i64>("val").unwrap().unwrap(), 99);
}

#[test]
fn test_series_delete_returning() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE SERIES test::s { ts: int8, val: int8 } WITH { key: ts }");
	t.command("INSERT test::s [{ ts: 1000, val: 42 }]");

	let frames = t.command("DELETE test::s FILTER { ts == 1000 } RETURNING { ts, val }");
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<i64>("ts").unwrap().unwrap(), 1000);
	assert_eq!(rows[0].get::<i64>("val").unwrap().unwrap(), 42);
}

#[test]
fn test_dictionary_insert_returning() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE DICTIONARY test::d FOR Utf8 AS Uint8");

	let frames = t.command(r#"INSERT test::d [{ value: "hello" }] RETURNING { id, value }"#);
	let frame = &frames[0];

	let rows: Vec<_> = frame.rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<String>("value").unwrap().unwrap(), "hello");
	assert!(rows[0].get::<u64>("id").unwrap().unwrap() > 0);
}

#[test]
fn test_table_update_returning_decodes_dictionary_column() {
	// RETURNING must yield the dictionary's value, never the internal entry id.
	// Insert decodes it; update must not diverge, or the same clause means two
	// different things depending on which statement produced the row.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE DICTIONARY test::syms FOR utf8 AS uint2");
	t.admin("CREATE TABLE test::t { id: int4, sym: utf8 with { dictionary: test::syms } }");
	t.command(r#"INSERT test::t [{ id: 1, sym: "alpha" }]"#);

	let frames = t.command(r#"UPDATE test::t { sym: "beta" } FILTER { id == 1 } RETURNING { id, sym }"#);
	let rows: Vec<_> = frames[0].rows().collect();

	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<String>("sym").unwrap().unwrap(), "beta");
}

#[test]
fn test_table_delete_returning_decodes_dictionary_column() {
	// Same contract on the delete path: the removed row is reported in user values,
	// so a caller can act on it without a second lookup against the dictionary.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE DICTIONARY test::syms FOR utf8 AS uint2");
	t.admin("CREATE TABLE test::t { id: int4, sym: utf8 with { dictionary: test::syms } }");
	t.command(r#"INSERT test::t [{ id: 1, sym: "alpha" }]"#);

	let frames = t.command(r#"DELETE test::t FILTER { id == 1 } RETURNING { id, sym }"#);
	let rows: Vec<_> = frames[0].rows().collect();

	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<String>("sym").unwrap().unwrap(), "alpha");
}

#[test]
fn test_ringbuffer_update_returning_decodes_dictionary_column() {
	// RETURNING must not mean different things per object kind: a ringbuffer update
	// reports the dictionary value, exactly as a table update does.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE DICTIONARY test::syms FOR utf8 AS uint2");
	t.admin(
		"CREATE RINGBUFFER test::rb { a: int4, sym: utf8 with { dictionary: test::syms } } WITH { capacity: 10 }",
	);
	t.command(r#"INSERT test::rb [{ a: 1, sym: "alpha" }]"#);

	let frames = t.command(r#"UPDATE test::rb { sym: "beta" } FILTER { a == 1 } RETURNING { a, sym }"#);
	let rows: Vec<_> = frames[0].rows().collect();

	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<String>("sym").unwrap().unwrap(), "beta");
}

#[test]
fn test_ringbuffer_delete_returning_decodes_dictionary_column() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE DICTIONARY test::syms FOR utf8 AS uint2");
	t.admin(
		"CREATE RINGBUFFER test::rb { a: int4, sym: utf8 with { dictionary: test::syms } } WITH { capacity: 10 }",
	);
	t.command(r#"INSERT test::rb [{ a: 1, sym: "alpha" }]"#);

	let frames = t.command(r#"DELETE test::rb FILTER { a == 1 } RETURNING { a, sym }"#);
	let rows: Vec<_> = frames[0].rows().collect();

	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<String>("sym").unwrap().unwrap(), "alpha");
}

#[test]
fn test_series_update_returning_decodes_dictionary_column() {
	// Parity with the table and ringbuffer paths: the dictionary value, not the entry id.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE DICTIONARY test::syms FOR utf8 AS uint2");
	t.admin("CREATE SERIES test::s { ts: int8, sym: utf8 with { dictionary: test::syms } } WITH { key: ts }");
	t.command(r#"INSERT test::s [{ ts: 1000, sym: "alpha" }]"#);

	let frames = t.command(r#"UPDATE test::s { sym: "beta" } FILTER { ts == 1000 } RETURNING { ts, sym }"#);
	let rows: Vec<_> = frames[0].rows().collect();

	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<String>("sym").unwrap().unwrap(), "beta");

	// The update must route the value through the dictionary, not write raw utf8 into
	// the DictionaryId slot - otherwise the write itself is unrepresentable and aborts.
	let frames = t.query("FROM test::s");
	let stored: Vec<_> = frames[0].rows().collect();
	assert_eq!(stored[0].get::<String>("sym").unwrap().unwrap(), "beta");
}

#[test]
fn test_series_update_returning_reports_the_stored_row() {
	// The series update path evaluates RETURNING against the input pipeline columns
	// instead of the row it wrote, so anything the write stamps on the stored row -
	// here the update timestamp - never reaches the caller. RETURNING must describe
	// the row that is now in the series, exactly as the table path does.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE SERIES test::s { ts: int8, val: int8 } WITH { key: ts }");
	t.command("INSERT test::s [{ ts: 1000, val: 42 }]");

	t.mock_clock().advance_millis(5_000);

	let frames = t.command("UPDATE test::s { val: 99 } FILTER { ts == 1000 } RETURNING { ts, #updated_at }");
	let returned: Vec<_> = frames[0].rows().collect();
	assert_eq!(returned.len(), 1);
	let returned_updated_at = returned[0].get::<DateTime>("updated_at").unwrap().unwrap();

	let frames = t.query("FROM test::s | map { ts, #updated_at }");
	let stored: Vec<_> = frames[0].rows().collect();
	let stored_updated_at = stored[0].get::<DateTime>("updated_at").unwrap().unwrap();

	assert_eq!(returned_updated_at, stored_updated_at);
}

#[test]
fn test_table_update_returning_reports_the_stored_row() {
	// Parity guard for the series test above: the table path decodes the stored row,
	// and must keep doing so.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, val: int8 }");
	t.command("INSERT test::t [{ id: 1, val: 42 }]");

	t.mock_clock().advance_millis(5_000);

	let frames = t.command("UPDATE test::t { val: 99 } FILTER { id == 1 } RETURNING { id, #updated_at }");
	let returned: Vec<_> = frames[0].rows().collect();
	let returned_updated_at = returned[0].get::<DateTime>("updated_at").unwrap().unwrap();

	let frames = t.query("FROM test::t | map { id, #updated_at }");
	let stored: Vec<_> = frames[0].rows().collect();
	let stored_updated_at = stored[0].get::<DateTime>("updated_at").unwrap().unwrap();

	assert_eq!(returned_updated_at, stored_updated_at);
}

#[test]
fn test_series_delete_filtered_returning_decodes_dictionary_column() {
	// The filtered delete evaluates RETURNING against the input pipeline columns.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE DICTIONARY test::syms FOR utf8 AS uint2");
	t.admin("CREATE SERIES test::s { ts: int8, sym: utf8 with { dictionary: test::syms } } WITH { key: ts }");
	t.command(r#"INSERT test::s [{ ts: 1000, sym: "alpha" }]"#);

	let frames = t.command(r#"DELETE test::s FILTER { ts == 1000 } RETURNING { ts, sym }"#);
	let rows: Vec<_> = frames[0].rows().collect();

	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<String>("sym").unwrap().unwrap(), "alpha");
}

#[test]
fn test_queue_insert_returning_decodes_dictionary_column() {
	// Queues are insert-only, so this is the whole RETURNING surface for the kind.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE DICTIONARY test::syms FOR utf8 AS uint2");
	t.admin("CREATE QUEUE test::q { id: int4, sym: utf8 with { dictionary: test::syms } } WITH { fifo: {} }");

	let frames = t.command(r#"INSERT test::q [{ id: 1, sym: "alpha" }] RETURNING { id, sym }"#);
	let rows: Vec<_> = frames[0].rows().collect();

	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<String>("sym").unwrap().unwrap(), "alpha");
}

#[test]
fn test_table_insert_returning_decodes_dictionary_column() {
	// Guards the one path that always had the dictionary decode, so the update and
	// delete fixes cannot drift away from it again.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE DICTIONARY test::syms FOR utf8 AS uint2");
	t.admin("CREATE TABLE test::t { id: int4, sym: utf8 with { dictionary: test::syms } }");

	let frames = t.command(r#"INSERT test::t [{ id: 1, sym: "alpha" }] RETURNING { id, sym }"#);
	let rows: Vec<_> = frames[0].rows().collect();

	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<String>("sym").unwrap().unwrap(), "alpha");
}
