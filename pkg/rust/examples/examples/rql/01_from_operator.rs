// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! # FROM Operator Example
//!
//! Demonstrates the FROM operator in ReifyDB's RQL:
//! - Loading data from inline arrays
//! - Querying data from tables
//! - The foundation for all query pipelines
//!
//! Run with: `make rql-from` or `cargo run --bin rql-from`

use reifydb::{Params, embedded};
use reifydb_examples::log_query;
use tracing::info;

fn main() {
	// Create and start an in-memory database
	let mut db = embedded::memory().build().unwrap();
	db.start().unwrap();

	// Example 1: FROM with inline data (single encoded)
	info!("Example 1: FROM with single inline encoded");
	log_query(r#"from [{ name: "Alice", age: 30 }]"#);
	for frame in db.query_as_root(r#"from [{ name: "Alice", age: 30 }]"#, Params::None).unwrap() {
		info!("{}", frame);
		// Output:
		// +--------+-------+
		// |  name  |  age  |
		// +--------+-------+
		// | Alice  |  30   |
		// +--------+-------+
	}

	// Example 2: FROM with inline data (multiple rows)
	info!("\nExample 2: FROM with multiple inline rows");
	log_query(
		r#"from [
  { name: "Bob", age: 25 },
  { name: "Carol", age: 35 },
  { name: "Dave", age: 28 }
]"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from [
				{ name: "Bob", age: 25 },
				{ name: "Carol", age: 35 },
				{ name: "Dave", age: 28 }
			]
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
		// Output:
		// +--------+-------+
		// |  name  |  age  |
		// +--------+-------+
		// |  Bob   |  25   |
		// | Carol  |  35   |
		// |  Dave  |  28   |
		// +--------+-------+
	}

	// Example 3: FROM with different data types
	info!("\nExample 3: FROM with various data types");
	log_query(r#"from [{ id: 1, active: true, price: 19.99, description: "Product A" }]"#);
	for frame in db
		.query_as_root(
			r#"
			from [{
				id: 1,
				active: true,
				price: 19.99,
				description: "Product A"
			}]
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
		// Output:
		// +------+----------+---------+---------------+
		// |  id  |  active  |  price  |  description  |
		// +------+----------+---------+---------------+
		// |  1   |   true   |  19.99  |   Product A   |
		// +------+----------+---------+---------------+
	}

	// Example 4: FROM with tables (after creating a table)
	info!("\nExample 4: FROM with tables");

	// First create a namespace and table
	info!("Creating namespace and table...");
	db.admin_as_root(r#"create namespace demo"#, Params::None).unwrap();

	db.admin_as_root(
		r#"
		create table demo::users {
			id: int4,
			username: utf8,
			email: utf8,
			is_active: bool
		}
		"#,
		Params::None,
	)
	.unwrap();

	// Insert some data
	info!("Inserting sample data...");
	db.command_as_root(
		r#"
		INSERT demo::users [
			{ id: 1, username: "alice", email: "alice@example.com", is_active: true },
			{ id: 2, username: "bob", email: "bob@example.com", is_active: true },
			{ id: 3, username: "charlie", email: "charlie@example.com", is_active: false }
		]
		"#,
		Params::None,
	)
	.unwrap();

	// Now query from the table
	log_query(r#"from demo::users"#);
	for frame in db.query_as_root(r#"from demo::users"#, Params::None).unwrap() {
		info!("{}", frame);
		// Output:
		// +------+------------+----------------------+-------------+
		// |  id  |  username  |        email         |  is_active  |
		// +------+------------+----------------------+-------------+
		// |  3   |  charlie   | charlie@example.com  |    false    |
		// |  2   |    bob     |   bob@example.com    |    true     |
		// |  1   |   alice    |  alice@example.com   |    true     |
		// +------+------------+----------------------+-------------+
	}

	// Example 5: FROM with empty array
	info!("\nExample 5: FROM with empty array");
	log_query(r#"from []"#);
	for frame in db.query_as_root(r#"from []"#, Params::None).unwrap() {
		info!("{}", frame);
		// Output: (empty result set)
	}
}
