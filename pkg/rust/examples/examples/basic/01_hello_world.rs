// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! # Hello World Example
//!
//! Demonstrates the fundamental ReifyDB operations:
//! - Starting a synchronous in-memory database
//! - Running commands (write operations)
//! - Executing queries (read operations)
//! - Creating and using sessions for isolated operations
//!
//! Run with: `make hello-world` or `cargo run --bin hello-world`

use reifydb::{Identity, Params, Session, embedded};
use reifydb_examples::log_query;
use tracing::info;

fn main() {
	// Step 1: Create and start an in-memory database
	// The embedded::memory() builder creates a database that:
	// - Stores all data in memory (no persistence)
	// - Operates asynchronously
	let mut db = embedded::memory().build().unwrap();

	// Start the database engine - this initializes internal structures
	// and makes the database ready to accept commands and queries
	db.start().unwrap();

	// Step 2: Execute a COMMAND (write operation) as root user
	// Commands can modify the database state and return results
	// The MAP command creates a result set with computed values
	log_query("MAP { 42 as answer }");
	for frame in db.command_as_root("MAP { 42 as answer}", Params::None).unwrap() {
		info!("{}", frame);
	}

	// Step 3: Execute a QUERY (read operation) as root user
	// Queries are read-only operations that cannot modify database state
	// They're useful for retrieving and computing data without side effects
	log_query("Map { 40 + 2 as another_answer }");
	for frame in db.query_as_root("Map { 40 + 2 as another_answer}", Params::None).unwrap() {
		info!("{}", frame);
	}

	// Step 4: Create a SESSION for isolated operations
	// Sessions provide:
	// - Isolated execution context
	// - User-specific permissions and state
	info!("Creating a session for isolated operations");
	let session = db.query_session(Identity::root()).unwrap();

	// Execute a query within the session context
	// Sessions can maintain state across multiple operations
	log_query("map { 20 * 2 + 2 as yet_another_answer}");
	for frame in session.query("map { 20 * 2 + 2 as yet_another_answer}", Params::None).unwrap() {
		info!("{}", frame);
	}

	// Clean shutdown - the database is automatically closed when dropped
	// This ensures all resources are properly released
	info!("Shutting down database...");
	drop(db);
}
