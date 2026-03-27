// SPDX-License-Identifier: Apache-2.0
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
        CREATE TABLE test::users {
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
        CREATE TABLE test::users {
            id: int4,
            name: utf8
        }
    "#,
	)
	.expect("CREATE TABLE failed");

	// Insert data
	let result = db.command(
		r#"
        INSERT test::users [
            { id: 1, name: "Alice" },
            { id: 2, name: "Bob" }
        ]
    "#,
	);
	assert!(result.is_ok(), "INSERT should succeed");

	// Query data back
	let result = db.query("FROM test::users");
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

#[wasm_bindgen_test]
fn test_create_transactional_view_with_table_source() {
	let db = WasmDB::new().expect("Failed to create db");

	db.admin("CREATE NAMESPACE ns").expect("CREATE NAMESPACE failed");
	db.admin(r#"CREATE TABLE ns::t { id: int4, name: utf8 }"#).expect("CREATE TABLE failed");
	db.admin(r#"CREATE TRANSACTIONAL VIEW ns::v { id: int4, name: utf8 } AS { FROM ns::t }"#)
		.expect("CREATE TRANSACTIONAL VIEW failed");

	db.command(r#"INSERT ns::t [{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }]"#).expect("INSERT failed");

	let result = db.query_text("FROM ns::v").expect("Query on transactional view should succeed");
	assert!(result.contains("Alice"), "View should contain Alice, got: {}", result);
	assert!(result.contains("Bob"), "View should contain Bob, got: {}", result);
}

#[wasm_bindgen_test]
fn test_create_view_with_inline_data_returns_error() {
	let db = WasmDB::new().expect("Failed to create db");

	db.admin("CREATE NAMESPACE ns2").expect("CREATE NAMESPACE failed");

	let result = db.admin(r#"CREATE TRANSACTIONAL VIEW ns2::v { id: int4 } AS { FROM [{ id: 1 }] }"#);

	assert!(result.is_err(), "Creating a view with only inline data should return an error, not panic");
}

#[wasm_bindgen_test]
fn test_login_with_password() {
	let db = WasmDB::new().expect("Failed to create db");

	db.admin("CREATE USER alice").expect("CREATE USER failed");
	db.admin("CREATE AUTHENTICATION FOR alice { method: password; password: 'alice-pass' }")
		.expect("CREATE AUTHENTICATION failed");

	let result = db.login_with_password("alice", "alice-pass");
	assert!(result.is_ok(), "Login with correct password should succeed");

	let login = result.expect("login failed");
	assert!(!login.token().is_empty(), "Token should not be empty");
	assert!(!login.identity().is_empty(), "Identity should not be empty");
}

#[wasm_bindgen_test]
fn test_login_with_wrong_password() {
	let db = WasmDB::new().expect("Failed to create db");

	db.admin("CREATE USER alice").expect("CREATE USER failed");
	db.admin("CREATE AUTHENTICATION FOR alice { method: password; password: 'alice-pass' }")
		.expect("CREATE AUTHENTICATION failed");

	let result = db.login_with_password("alice", "wrong-password");
	assert!(result.is_err(), "Login with wrong password should fail");
}

#[wasm_bindgen_test]
fn test_login_with_token() {
	let db = WasmDB::new().expect("Failed to create db");

	db.admin("CREATE USER bob").expect("CREATE USER failed");
	db.admin("CREATE AUTHENTICATION FOR bob { method: token; token: 'bob-secret-token' }")
		.expect("CREATE AUTHENTICATION failed");

	let result = db.login_with_token("bob-secret-token");
	assert!(result.is_ok(), "Token login should succeed");

	let login = result.expect("login failed");
	assert!(!login.token().is_empty(), "Token should not be empty");
	assert!(!login.identity().is_empty(), "Identity should not be empty");
}

#[wasm_bindgen_test]
fn test_logout() {
	let db = WasmDB::new().expect("Failed to create db");

	db.admin("CREATE USER alice").expect("CREATE USER failed");
	db.admin("CREATE AUTHENTICATION FOR alice { method: password; password: 'alice-pass' }")
		.expect("CREATE AUTHENTICATION failed");

	db.login_with_password("alice", "alice-pass").expect("Login failed");

	let result = db.logout();
	assert!(result.is_ok(), "Logout should succeed");
}

#[wasm_bindgen_test]
fn test_logout_without_login() {
	let db = WasmDB::new().expect("Failed to create db");

	let result = db.logout();
	assert!(result.is_ok(), "Logout without login should succeed (no-op)");
}
