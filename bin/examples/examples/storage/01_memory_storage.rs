//! # Memory Storage Example
//!
//! Demonstrates using ReifyDB with in-memory storage:
//! - Creating an in-memory database
//! - Basic CRUD operations
//! - Data is volatile (lost when program exits)
//!
//! Run with: `cargo run --bin storage-memory`

use reifydb::{sync, Params, SessionSync};
use reifydb::log_info;
use reifydb_examples::log_query;

fn main() {
	log_info!("=== Memory Storage Example ===\n");
	
	// Create an in-memory database
	log_info!("Creating in-memory database...");
	let mut db = sync::memory_optimistic().build().unwrap();
	db.start().unwrap();
	log_info!("✓ Database created and started\n");
	
	// Create a schema
	log_info!("Creating schema 'app'...");
	let result = db.command_as_root("create schema app", Params::None).unwrap();
	for frame in result {
		log_info!("{}", frame);
	}
	log_info!("✓ Schema created\n");
	
	// Create a table
	log_info!("Creating table 'users'...");
	let result = db.command_as_root(
		r#"
		create table app.users {
			id: int4,
			name: utf8,
			email: utf8,
			active: bool
		}
		"#,
		Params::None,
	).unwrap();
	for frame in result {
		log_info!("{}", frame);
	}
	log_info!("✓ Table created\n");
	
	// Insert data
	log_info!("Inserting data...");
	log_query(
		r#"from [
  { id: 1, name: "Alice", email: "alice@example.com", active: true },
  { id: 2, name: "Bob", email: "bob@example.com", active: false },
  { id: 3, name: "Charlie", email: "charlie@example.com", active: true }
]
insert app.users"#
	);
	
	let result = db.command_as_root(
		r#"
		from [
			{ id: 1, name: "Alice", email: "alice@example.com", active: true },
			{ id: 2, name: "Bob", email: "bob@example.com", active: false },
			{ id: 3, name: "Charlie", email: "charlie@example.com", active: true }
		]
		insert app.users
		"#,
		Params::None,
	).unwrap();
	for frame in result {
		log_info!("{}", frame);
	}
	log_info!("✓ Data inserted\n");
	
	// Query all data
	log_info!("Querying all users:");
	log_query("from app.users");
	
	for frame in db
		.query_as_root("from app.users", Params::None)
		.unwrap()
	{
		log_info!("{}", frame);
	}
	
	// Query with filter
	log_info!("\nQuerying active users:");
	log_query("from app.users filter active == true");
	
	for frame in db
		.query_as_root("from app.users filter active == true", Params::None)
		.unwrap()
	{
		log_info!("{}", frame);
	}
	
	// Add more data
	log_info!("\nAdding another user:");
	log_query(
		r#"from [{ id: 4, name: "Diana", email: "diana@example.com", active: true }]
insert app.users"#
	);
	
	let result = db.command_as_root(
		r#"
		from [{ id: 4, name: "Diana", email: "diana@example.com", active: true }]
		insert app.users
		"#,
		Params::None,
	).unwrap();
	for frame in result {
		log_info!("{}", frame);
	}
	log_info!("✓ User added\n");
	
	// Query updated data
	log_info!("Querying all users after update:");
	log_query("from app.users sort id");
	
	for frame in db
		.query_as_root("from app.users sort id", Params::None)
		.unwrap()
	{
		log_info!("{}", frame);
	}
}