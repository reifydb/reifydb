//! # Memory Storage Example
//!
//! Demonstrates using ReifyDB with in-memory storage:
//! - Creating an in-memory database
//! - Basic CRUD operations
//! - Data is volatile (lost when program exits)
//!
//! Run with: `cargo run --bin storage-memory`

use reifydb::{Params, embedded};
use reifydb_examples::log_query;
use tracing::info;

#[tokio::main]
async fn main() {
	info!("=== Memory Storage Example ===\n");

	// Create an in-memory database
	info!("Creating in-memory database...");
	let mut db = embedded::memory().build().unwrap();
	db.start().await.unwrap();
	info!("✓ Database created and started\n");

	// Create a namespace
	info!("Creating namespace 'app'...");
	let result = db.command_as_root("create namespace app", Params::None).await.unwrap();
	for frame in result {
		info!("{}", frame);
	}
	info!("✓ Namespace created\n");

	// Create a table
	info!("Creating table 'users'...");
	let result = db
		.command_as_root(
			r#"
		create table app.users {
			id: int4,
			name: utf8,
			email: utf8,
			active: bool
		}
		"#,
			Params::None,
		)
		.await
		.unwrap();
	for frame in result {
		info!("{}", frame);
	}
	info!("✓ Table created\n");

	// Insert data
	info!("Inserting data...");
	log_query(
		r#"from [
  { id: 1, name: "Alice", email: "alice@example.com", active: true },
  { id: 2, name: "Bob", email: "bob@example.com", active: false },
  { id: 3, name: "Charlie", email: "charlie@example.com", active: true }
]
insert app.users"#,
	);

	let result = db
		.command_as_root(
			r#"
		from [
			{ id: 1, name: "Alice", email: "alice@example.com", active: true },
			{ id: 2, name: "Bob", email: "bob@example.com", active: false },
			{ id: 3, name: "Charlie", email: "charlie@example.com", active: true }
		]
		insert app.users
		"#,
			Params::None,
		)
		.await
		.unwrap();
	for frame in result {
		info!("{}", frame);
	}
	info!("✓ Data inserted\n");

	// Query all data
	info!("Querying all users:");
	log_query("from app.users");

	for frame in db.query_as_root("from app.users", Params::None).await.unwrap() {
		info!("{}", frame);
	}

	// Query with filter
	info!("\nQuerying active users:");
	log_query("from app.users filter active == true");

	for frame in db.query_as_root("from app.users filter active == true", Params::None).await.unwrap() {
		info!("{}", frame);
	}

	// Add more data
	info!("\nAdding another user:");
	log_query(
		r#"from [{ id: 4, name: "Diana", email: "diana@example.com", active: true }]
insert app.users"#,
	);

	let result = db
		.command_as_root(
			r#"
		from [{ id: 4, name: "Diana", email: "diana@example.com", active: true }]
		insert app.users
		"#,
			Params::None,
		)
		.await
		.unwrap();
	for frame in result {
		info!("{}", frame);
	}
	info!("✓ User added\n");

	// Query updated data
	info!("Querying all users after update:");
	log_query("from app.users sort id");

	for frame in db.query_as_root("from app.users sort id", Params::None).await.unwrap() {
		info!("{}", frame);
	}
}
