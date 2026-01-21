// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM integration tests
//!
//! Run with: wasm-pack test --headless --chrome

use wasm_bindgen_test::*;
use reifydb_engine_wasm::WasmEngine;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
fn test_create_engine() {
    // Test that we can create a WASM engine
    let engine = WasmEngine::new();
    assert!(engine.is_ok(), "Should be able to create WasmEngine");
}

#[wasm_bindgen_test]
fn test_simple_query() {
    let engine = WasmEngine::new().expect("Failed to create engine");

    // Test a simple inline query
    let result = engine.query(r#"FROM [{ x: 1, y: 2 }]"#);

    assert!(result.is_ok(), "Simple query should succeed");
}

#[wasm_bindgen_test]
fn test_query_with_filter() {
    let engine = WasmEngine::new().expect("Failed to create engine");

    // Test filtering
    let result = engine.query(r#"
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
    let engine = WasmEngine::new().expect("Failed to create engine");

    // Create namespace
    let result = engine.command("CREATE NAMESPACE test");
    assert!(result.is_ok(), "CREATE NAMESPACE should succeed");

    // Create table
    let result = engine.command(r#"
        CREATE TABLE test.users {
            id: int4,
            name: utf8
        }
    "#);
    assert!(result.is_ok(), "CREATE TABLE should succeed");
}

#[wasm_bindgen_test]
fn test_insert_and_query() {
    let engine = WasmEngine::new().expect("Failed to create engine");

    // Create namespace and table
    engine.command("CREATE NAMESPACE test").expect("CREATE NAMESPACE failed");
    engine.command(r#"
        CREATE TABLE test.users {
            id: int4,
            name: utf8
        }
    "#).expect("CREATE TABLE failed");

    // Insert data
    let result = engine.command(r#"
        FROM [
            { id: 1, name: "Alice" },
            { id: 2, name: "Bob" }
        ]
        INSERT test.users
    "#);
    assert!(result.is_ok(), "INSERT should succeed");

    // Query data back
    let result = engine.query("FROM test.users");
    assert!(result.is_ok(), "Query after insert should succeed");
}

#[wasm_bindgen_test]
fn test_aggregation() {
    let engine = WasmEngine::new().expect("Failed to create engine");

    // Test aggregation functions
    let result = engine.query(r#"
        FROM [
            { value: 10 },
            { value: 20 },
            { value: 30 }
        ]
        AGGREGATE {
            total: count(),
            sum: sum(value),
            avg: avg(value)
        }
    "#);

    assert!(result.is_ok(), "Aggregation query should succeed");
}

#[wasm_bindgen_test]
fn test_invalid_query() {
    let engine = WasmEngine::new().expect("Failed to create engine");

    // Test that invalid queries return errors
    let result = engine.query("INVALID QUERY SYNTAX");

    assert!(result.is_err(), "Invalid query should return error");
}

#[wasm_bindgen_test]
fn test_multiple_queries() {
    let engine = WasmEngine::new().expect("Failed to create engine");

    // Test that we can run multiple queries on same engine
    engine.query(r#"FROM [{ x: 1 }]"#).expect("First query failed");
    engine.query(r#"FROM [{ y: 2 }]"#).expect("Second query failed");
    engine.query(r#"FROM [{ z: 3 }]"#).expect("Third query failed");
}
