//! # Hello World Example
//!
//! Demonstrates the fundamental ReifyDB operations:
//! - Starting a synchronous in-memory database
//! - Running commands (write operations)
//! - Executing queries (read operations)
//! - Creating and using sessions for isolated operations
//!
//! Run with: `make hello-world` or `cargo run --bin hello-world`

use reifydb::{Identity, MemoryDatabaseOptimistic, Params, Session, embedded, log_info};
use reifydb_examples::log_query;

// Type alias for our in-memory optimistic database
// This uses optimistic concurrency control for better performance
pub type DB = MemoryDatabaseOptimistic;

fn main() {
	// Step 1: Create and start a synchronous in-memory database
	// The sync::memory_optimistic() builder creates a database that:
	// - Stores all data in memory (no persistence)
	// - Uses optimistic concurrency control
	// - Operates synchronously (blocking operations)
	let mut db: DB = embedded::memory_optimistic().build().unwrap();

	// Start the database engine - this initializes internal structures
	// and makes the database ready to accept commands and queries
	db.start().unwrap();

	// Step 2: Execute a COMMAND (write operation) as root user
	// Commands can modify the database state and return results
	// The MAP command creates a result set with computed values
	log_query("MAP { 42 as answer }");
	for frame in db.command_as_root("MAP { 42 as answer}", Params::None).unwrap() {
		log_info!("{}", frame);
	}

	// Step 3: Execute a QUERY (read operation) as root user
	// Queries are read-only operations that cannot modify database state
	// They're useful for retrieving and computing data without side effects
	log_query("Map { 40 + 2 as another_answer }");
	for frame in db.query_as_root("Map { 40 + 2 as another_answer}", Params::None).unwrap() {
		log_info!("{}", frame);
	}

	// Step 4: Create a SESSION for isolated operations
	// Sessions provide:
	// - Isolated execution context
	// - User-specific permissions and state
	log_info!("Creating a session for isolated operations");
	let session = db.query_session(Identity::root()).unwrap();

	// Execute a query within the session context
	// Sessions can maintain state across multiple operations
	log_query("map { 20 * 2 + 2 as yet_another_answer}");
	for frame in session.query("map { 20 * 2 + 2 as yet_another_answer}", Params::None).unwrap() {
		log_info!("{}", frame);
	}

	// Clean shutdown - the database is automatically closed when dropped
	// This ensures all resources are properly released
	log_info!("Shutting down database...");
	drop(db);
}
