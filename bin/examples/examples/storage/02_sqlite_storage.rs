//! # SQLite Storage Example
//!
//! Demonstrates using ReifyDB with SQLite persistent storage:
//! - Creating a database with file persistence
//! - Data survives program restarts
//! - Database file management
//!
//! Run with: `cargo run --bin storage-sqlite`

use std::{fs, path::Path};

use reifydb::{embedded, log_info, Params, Session, SqliteConfig};
use reifydb_examples::log_query;

fn main() {
	let db_path = "/tmp/reifydb-example/db";

	// Clean up any existing database
	if Path::new(db_path).exists() {
		log_info!("Removing existing database file...");
		fs::remove_dir_all(db_path).unwrap_or_else(|_| ());
		log_info!("✓ Cleanup complete\n");
	}

	// Part 1: Create database and add data
	log_info!("Part 1: Creating persistent database");
	log_info!("Database path: {}\n", db_path);

	{
		// Create database (will be saved to disk)
		log_info!("Creating SQLite database...");
		let mut db = embedded::sqlite_serializable(SqliteConfig::new(
			db_path,
		))
		.build()
		.unwrap();
		db.start().unwrap();
		log_info!("✓ Database created and started\n");

		// Create namespace
		log_info!("Creating namespace 'store'...");
		let result = db
			.command_as_root("create namespace store", Params::None)
			.unwrap();
		for frame in result {
			log_info!("{}", frame);
		}
		log_info!("✓ Namespace created\n");

		// Create table
		log_info!("Creating table 'products'...");
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
			log_info!("{}", frame);
		}
		log_info!("✓ Table created\n");

		// Insert data
		log_info!("Inserting products...");
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
			log_info!("{}", frame);
		}
		log_info!("✓ Products inserted\n");

		// Query data
		log_info!("Querying products:");
		log_query("from store.products");

		for frame in db
			.query_as_root("from store.products", Params::None)
			.unwrap()
		{
			log_info!("{}", frame);
		}

		log_info!(
			"\n✓ Database connection closing. Data saved to disk."
		);
	} // Database connection closed here

	// Part 2: Reopen database and verify persistence
	log_info!("\n========================================");
	log_info!("Program could restart here");
	log_info!("========================================\n");
	log_info!("Part 2: Reopening database from disk");

	{
		// Open existing database
		log_info!("Opening existing database at: {}", db_path);
		let mut db = embedded::sqlite_serializable(SqliteConfig::new(
			db_path,
		))
		.build()
		.unwrap();
		db.start().unwrap();
		log_info!("✓ Database reopened successfully\n");

		// Verify data persisted
		log_info!("Verifying data persistence:");
		log_query("from store.products");

		for frame in db
			.query_as_root("from store.products", Params::None)
			.unwrap()
		{
			log_info!("{}", frame);
		}
		log_info!("✓ Data persisted correctly\n");

		// Add more data
		log_info!("Adding new product:");
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
			log_info!("{}", frame);
		}
		log_info!("✓ New product added\n");

		// Query updated data
		log_info!("All products after update:");
		log_query("from store.products sort id");

		for frame in db
			.query_as_root(
				"from store.products sort id",
				Params::None,
			)
			.unwrap()
		{
			log_info!("{}", frame);
		}

		// Query with filter
		log_info!("\nQuerying in-stock products:");
		log_query("from store.products filter in_stock == true");

		for frame in db
			.query_as_root(
				"from store.products filter in_stock == true",
				Params::None,
			)
			.unwrap()
		{
			log_info!("{}", frame);
		}

		log_info!("\n✓ Database operations complete");
	}

	// Check database file
	if let Ok(metadata) = fs::metadata(db_path) {
		println!("\n=== Database File Info ===");
		println!("Path: {}", db_path);
		println!("Size: {} bytes", metadata.len());
	}

	// Cleanup
	log_info!("\n=== Cleanup ===");
	log_info!("Removing database files...");
	fs::remove_dir_all(db_path).unwrap_or_else(|_| ());
	log_info!("✓ Cleanup complete");
}
