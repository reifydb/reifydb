// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM integration tests
//!
//! Run with: wasm-pack test --headless --firefox

use reifydb_webassembly::WasmDB;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
fn test_create_db() {
	// Test that we can create a WASM database
	let db = WasmDB::new();
	assert!(db.is_ok(), "Should be able to create WasmDB");
}

#[wasm_bindgen_test]
fn test_simple_query() {
	let db = WasmDB::new().expect("Failed to create db");

	// Test a simple inline query
	let result = db.query(r#"FROM [{ x: 1, y: 2 }]"#);

	assert!(result.is_ok(), "Simple query should succeed");
}

#[wasm_bindgen_test]
fn test_query_with_filter() {
	let db = WasmDB::new().expect("Failed to create db");

	// Test filtering
	let result = db.query(r#"
        FROM [
            { name: "Alice", age: 30 },
            { name: "Bob", age: 25 },
            { name: "Carol", age: 35 }
        ]
        FILTER age > 25
    "#);

	assert!(result.is_ok(), "Filter query should succeed");
}

#[wasm_bindgen_test]
fn test_create_table() {
	let db = WasmDB::new().expect("Failed to create db");

	// Create namespace
	let result = db.admin("CREATE NAMESPACE test");
	assert!(result.is_ok(), "CREATE NAMESPACE should succeed");

	// Create table
	let result = db.command(
		r#"
        CREATE TABLE test.users {
            id: int4,
            name: utf8
        }
    "#,
	);
	assert!(result.is_ok(), "CREATE TABLE should succeed");
}

#[wasm_bindgen_test]
fn test_insert_and_query() {
	let db = WasmDB::new().expect("Failed to create db");

	// Create namespace and table
	db.admin("CREATE NAMESPACE test").expect("CREATE NAMESPACE failed");
	db.command(
		r#"
        CREATE TABLE test.users {
            id: int4,
            name: utf8
        }
    "#,
	)
	.expect("CREATE TABLE failed");

	// Insert data
	let result = db.command(
		r#"
        FROM [
            { id: 1, name: "Alice" },
            { id: 2, name: "Bob" }
        ]
        INSERT test.users
    "#,
	);
	assert!(result.is_ok(), "INSERT should succeed");

	// Query data back
	let result = db.query("FROM test.users");
	assert!(result.is_ok(), "Query after insert should succeed");
}

#[wasm_bindgen_test]
fn test_invalid_query() {
	let db = WasmDB::new().expect("Failed to create db");

	// Test that invalid queries return errors
	let result = db.query("INVALID QUERY SYNTAX");

	assert!(result.is_err(), "Invalid query should return error");
}

#[wasm_bindgen_test]
fn test_multiple_queries() {
	let db = WasmDB::new().expect("Failed to create db");

	// Test that we can run multiple queries on same db
	db.query(r#"FROM [{ x: 1 }]"#).expect("First query failed");
	db.query(r#"FROM [{ y: 2 }]"#).expect("Second query failed");
	db.query(r#"FROM [{ z: 3 }]"#).expect("Third query failed");
}
