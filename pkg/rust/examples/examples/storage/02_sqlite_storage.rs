//! # SQLite Storage Example
//!
//! Demonstrates using ReifyDB with SQLite persistent storage:
//! - Creating a database with file persistence
//! - Data survives program restarts
//! - Database file management
//!
//! Run with: `cargo run --bin storage-sqlite`

use std::{fs, path::Path};

use reifydb::{Params, Session, SqliteConfig, embedded};
use reifydb_examples::log_query;
use tracing::info;

fn main() {
	let db_path = "/tmp/reifydb-example/crates";

	// Clean up any existing database
	if Path::new(db_path).exists() {
		info!("Removing existing database file...");
		fs::remove_dir_all(db_path).unwrap_or_else(|_| ());
		info!("✓ Cleanup complete\n");
	}

	// Part 1: Create database and add data
	info!("Part 1: Creating persistent database");
	info!("Database path: {}\n", db_path);

	{
		// Create database (will be saved to disk)
		info!("Creating SQLite database...");
		let mut db = embedded::sqlite(SqliteConfig::new(db_path)).build().unwrap();
		db.start().unwrap();
		info!("✓ Database created and started\n");

		// Create namespace
		info!("Creating namespace 'store'...");
		let result = db.command_as_root("create namespace store", Params::None).unwrap();
		for frame in result {
			info!("{}", frame);
		}
		info!("✓ Namespace created\n");

		// Create table
		info!("Creating table 'products'...");
		let result = db
			.command_as_root(
				r#"
			create table store.products {
				id: int4,
				name: utf8,
				price: float8,
				in_stock: bool
			}
			"#,
				Params::None,
			)
			.unwrap();
		for frame in result {
			info!("{}", frame);
		}
		info!("✓ Table created\n");

		// Insert data
		info!("Inserting products...");
		log_query(
			r#"from [
  { id: 1, name: "Laptop", price: 999.99, in_stock: true },
  { id: 2, name: "Mouse", price: 29.99, in_stock: true },
  { id: 3, name: "Keyboard", price: 79.99, in_stock: false }
]
insert store.products"#,
		);

		let result = db
			.command_as_root(
				r#"
			from [
				{ id: 1, name: "Laptop", price: 999.99, in_stock: true },
				{ id: 2, name: "Mouse", price: 29.99, in_stock: true },
				{ id: 3, name: "Keyboard", price: 79.99, in_stock: false }
			]
			insert store.products
			"#,
				Params::None,
			)
			.unwrap();
		for frame in result {
			info!("{}", frame);
		}
		info!("✓ Products inserted\n");

		// Query data
		info!("Querying products:");
		log_query("from store.products");

		for frame in db.query_as_root("from store.products", Params::None).unwrap() {
			info!("{}", frame);
		}

		info!("\n✓ Database connection closing. Data saved to disk.");
	} // Database connection closed here

	// Part 2: Reopen database and verify persistence
	info!("\n========================================");
	info!("Program could restart here");
	info!("========================================\n");
	info!("Part 2: Reopening database from disk");

	{
		// Open existing database
		info!("Opening existing database at: {}", db_path);
		let mut db = embedded::sqlite(SqliteConfig::new(db_path)).build().unwrap();
		db.start().unwrap();
		info!("✓ Database reopened successfully\n");

		// Verify data persisted
		info!("Verifying data persistence:");
		log_query("from store.products");

		for frame in db.query_as_root("from store.products", Params::None).unwrap() {
			info!("{}", frame);
		}
		info!("✓ Data persisted correctly\n");

		// Add more data
		info!("Adding new product:");
		log_query(
			r#"from [{ id: 4, name: "Monitor", price: 299.99, in_stock: true }]
insert store.products"#,
		);

		let result = db
			.command_as_root(
				r#"
			from [{ id: 4, name: "Monitor", price: 299.99, in_stock: true }]
			insert store.products
			"#,
				Params::None,
			)
			.unwrap();
		for frame in result {
			info!("{}", frame);
		}
		info!("✓ New product added\n");

		// Query updated data
		info!("All products after update:");
		log_query("from store.products sort id");

		for frame in db.query_as_root("from store.products sort id", Params::None).unwrap() {
			info!("{}", frame);
		}

		// Query with filter
		info!("\nQuerying in-stock products:");
		log_query("from store.products filter in_stock == true");

		for frame in db.query_as_root("from store.products filter in_stock == true", Params::None).unwrap() {
			info!("{}", frame);
		}

		info!("\n✓ Database operations complete");
	}

	// Check database file
	if let Ok(metadata) = fs::metadata(db_path) {
		println!("\n=== Database File Info ===");
		println!("Path: {}", db_path);
		println!("Size: {} bytes", metadata.len());
	}

	// Cleanup
	info!("\n=== Cleanup ===");
	info!("Removing database files...");
	fs::remove_dir_all(db_path).unwrap_or_else(|_| ());
	info!("✓ Cleanup complete");
}
