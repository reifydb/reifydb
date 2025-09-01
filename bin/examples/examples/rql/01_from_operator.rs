//! # FROM Operator Example
//!
//! Demonstrates the FROM operator in ReifyDB's RQL:
//! - Loading data from inline arrays
//! - Querying data from tables
//! - The foundation for all query pipelines
//!
//! Run with: `make rql-from` or `cargo run --bin rql-from`

use reifydb::{embedded, log_info, Params, Session};
use reifydb_examples::log_query;

fn main() {
	// Create and start an in-memory database
	let mut db = embedded::memory_optimistic().build().unwrap();
	db.start().unwrap();

	// Example 1: FROM with inline data (single row)
	log_info!("Example 1: FROM with single inline row");
	log_query(r#"from [{ name: "Alice", age: 30 }]"#);
	for frame in db
		.query_as_root(
			r#"from [{ name: "Alice", age: 30 }]"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Output:
		// +--------+-------+
		// |  name  |  age  |
		// +--------+-------+
		// | Alice  |  30   |
		// +--------+-------+
	}

	// Example 2: FROM with inline data (multiple rows)
	log_info!("\nExample 2: FROM with multiple inline rows");
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
		log_info!("{}", frame);
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
	log_info!("\nExample 3: FROM with various data types");
	log_query(
		r#"from [{ id: 1, active: true, price: 19.99, description: "Product A" }]"#,
	);
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
		log_info!("{}", frame);
		// Output:
		// +------+----------+---------+---------------+
		// |  id  |  active  |  price  |  description  |
		// +------+----------+---------+---------------+
		// |  1   |   true   |  19.99  |   Product A   |
		// +------+----------+---------+---------------+
	}

	// Example 4: FROM with tables (after creating a table)
	log_info!("\nExample 4: FROM with tables");

	// First create a schema and table
	log_info!("Creating schema and table...");
	db.command_as_root(r#"create schema demo"#, Params::None).unwrap();

	db.command_as_root(
		r#"
		create table demo.users {
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
	log_info!("Inserting sample data...");
	db.command_as_root(
		r#"
		from [
			{ id: 1, username: "alice", email: "alice@example.com", is_active: true },
			{ id: 2, username: "bob", email: "bob@example.com", is_active: true },
			{ id: 3, username: "charlie", email: "charlie@example.com", is_active: false }
		]
		insert demo.users
		"#,
		Params::None,
	)
	.unwrap();

	// Now query from the table
	log_query(r#"from demo.users"#);
	for frame in
		db.query_as_root(r#"from demo.users"#, Params::None).unwrap()
	{
		log_info!("{}", frame);
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
	log_info!("\nExample 5: FROM with empty array");
	log_query(r#"from []"#);
	for frame in db.query_as_root(r#"from []"#, Params::None).unwrap() {
		log_info!("{}", frame);
		// Output: (empty result set)
	}
}
