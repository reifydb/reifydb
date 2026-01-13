// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

//! # RQLv2 + VM Command Execution Demo
//!
//! This demo shows the new RQLv2/VM-based command execution path:
//! - DDL: CREATE NAMESPACE, CREATE TABLE (via VM)
//! - DML: INSERT (via legacy path - VM DML pipeline integration pending)
//! - Query: FROM, MAP (via VM)
//!
//! The new implementation compiles RQL to bytecode and executes it via the VM,
//! replacing the legacy direct execution path.

use reifydb::{Identity, Params, embedded};

fn main() {
	println!("=== RQLv2 + VM Command Execution Demo ===\n");

	// 1. Create and start an in-memory database
	println!(">>> Creating in-memory database...");
	let mut db = embedded::memory().build().unwrap();
	db.start().unwrap();
	println!("Database started!\n");

	// Get the engine for direct access to the new methods
	let engine = db.engine().clone();
	let identity = Identity::root();

	// ============================================================
	// DDL Operations via new VM-based execution
	// ============================================================

	// 2. Create a namespace using the new VM-based execution
	println!(">>> CREATE NAMESPACE demo (via VM)...");
	let result = engine.command_new_as(&identity, "CREATE NAMESPACE demo", Params::None);
	match &result {
		Ok(_) => println!("Namespace created successfully!"),
		Err(e) => println!("Error: {}", e),
	}
	println!();

	// 3. Create a table using the new VM-based execution
	println!(">>> CREATE TABLE demo.users (via VM)...");
	let result = engine.command_new_as(
		&identity,
		r#"CREATE TABLE demo.users {
			id: int4,
			name: utf8,
			email: utf8
		}"#,
		Params::None,
	);
	match &result {
		Ok(_) => println!("Table created successfully!"),
		Err(e) => println!("Error: {}", e),
	}
	println!();

	// ============================================================
	// DML Operations - using legacy path for now
	// (VM DML pipeline integration is still in progress)
	// ============================================================

	// 4. Insert data using the legacy path
	println!(">>> INSERT INTO demo.users (via legacy command path)...");
	let result = db.command_as_root(
		r#"FROM [{
			id: 1,
			name: "Alice",
			email: "alice@example.com"
		}, {
			id: 2,
			name: "Bob",
			email: "bob@example.com"
		}, {
			id: 3,
			name: "Charlie",
			email: "charlie@example.com"
		}] INSERT demo.users"#,
		Params::None,
	);
	match &result {
		Ok(_) => println!("Data inserted successfully!"),
		Err(e) => println!("Error: {}", e),
	}
	println!();

	// ============================================================
	// Query Operations via new VM-based execution
	// ============================================================

	// 5. Query the data using the new VM-based execution
	println!(">>> SELECT * FROM demo.users (via VM)...");
	let result = engine.query_new_as(&identity, "FROM demo.users", Params::None);
	match &result {
		Ok(frames) => {
			println!("Query executed successfully!");
			for frame in frames {
				println!("{}", frame);
			}
		}
		Err(e) => println!("Error: {}", e),
	}
	println!();

	let result =
		engine.query_new_as(&identity, "FROM demo.users | MAP { name, email }", Params::None);
	match &result {
		Ok(frames) => {
			println!("Query executed successfully!");
			for frame in frames {
				println!("{}", frame);
			}
		}
		Err(e) => println!("Error: {}", e),
	}
	println!();

	let result = engine.query_new_as(
		&identity,
		r#"FROM demo.users | MAP { name, greeting: "Hello, " + name }"#,
		Params::None,
	);
	match &result {
		Ok(frames) => {
			println!("Query executed successfully!");
			for frame in frames {
				println!("{}", frame);
			}
		}
		Err(e) => println!("Error: {}", e),
	}
	println!();

	// ============================================================
	// Comparison: Legacy vs VM execution
	// ============================================================

	println!(">>> Comparing legacy vs VM execution...");
	println!("\nLegacy query (FROM demo.users):");
	match db.query_as_root("FROM demo.users", Params::None) {
		Ok(frames) => {
			for frame in frames {
				println!("{}", frame);
			}
		}
		Err(e) => println!("Error: {}", e),
	}

	println!("\nVM query (same query):");
	let result = engine.query_new_as(&identity, "FROM demo.users", Params::None);
	match &result {
		Ok(frames) => {
			for frame in frames {
				println!("{}", frame);
			}
		}
		Err(e) => println!("Error: {}", e),
	}

	// Clean shutdown
	println!("\n>>> Stopping database...");
	db.stop().unwrap();
	println!("Database stopped!");

	println!("\n=== Demo complete! ===");
	println!("\nSummary:");
	println!("  - DDL (CREATE NAMESPACE, CREATE TABLE): Working via VM");
	println!("  - DML (INSERT): Using legacy path (VM pipeline integration pending)");
	println!("  - Queries (FROM, MAP): Working via VM");
	println!("  - Note: FILTER type coercion needs additional work");
}
