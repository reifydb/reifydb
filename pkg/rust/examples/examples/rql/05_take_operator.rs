//! # TAKE Operator Example
//!
//! Demonstrates the TAKE operator in ReifyDB's RQL:
//! - Limiting the number of results
//! - Using take with other operators
//! - Pagination patterns
//!
//! Run with: `make rql-take` or `cargo run --bin rql-take`

use reifydb::{Params, Session, embedded, log_info};
use reifydb_examples::log_query;

fn main() {
	// Create and start an in-memory database
	let mut db = embedded::memory_optimistic().build().unwrap();
	db.start().unwrap();

	// Example 1: Basic take operation
	log_info!("Example 1: Take first 3 rows from inline data");
	log_query(
		r#"from [
  { id: 1, value: "first" },
  { id: 2, value: "second" },
  { id: 3, value: "third" },
  { id: 4, value: "fourth" },
  { id: 5, value: "fifth" }
]
take 3"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from [
				{ id: 1, value: "first" },
				{ id: 2, value: "second" },
				{ id: 3, value: "third" },
				{ id: 4, value: "fourth" },
				{ id: 5, value: "fifth" }
			]
			take 3
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Set up table data for more examples
	db.command_as_root("create namespace demo", Params::None).unwrap();
	db.command_as_root(
		r#"
		create table demo.events {
			id: int4,
			event_type: utf8,
			timestamp: int8,
			severity: utf8,
			message: utf8
		}
		"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
		from [
			{ id: 1, event_type: "ERROR", timestamp: 1000, severity: "HIGH", message: "Database connection failed" },
			{ id: 2, event_type: "INFO", timestamp: 1001, severity: "LOW", message: "User logged in" },
			{ id: 3, event_type: "WARNING", timestamp: 1002, severity: "MEDIUM", message: "Memory usage high" },
			{ id: 4, event_type: "ERROR", timestamp: 1003, severity: "HIGH", message: "Request timeout" },
			{ id: 5, event_type: "INFO", timestamp: 1004, severity: "LOW", message: "Cache cleared" },
			{ id: 6, event_type: "ERROR", timestamp: 1005, severity: "CRITICAL", message: "System crash detected" },
			{ id: 7, event_type: "WARNING", timestamp: 1006, severity: "MEDIUM", message: "Disk space low" },
			{ id: 8, event_type: "INFO", timestamp: 1007, severity: "LOW", message: "Backup completed" },
			{ id: 9, event_type: "ERROR", timestamp: 1008, severity: "HIGH", message: "Authentication failed" },
			{ id: 10, event_type: "INFO", timestamp: 1009, severity: "LOW", message: "Service started" }
		]
		insert demo.events
		"#,
		Params::None,
	)
	.unwrap();

	// Example 2: Take from a table
	log_info!("\nExample 2: Take first 5 events from table");
	log_query(r#"from demo.events take 5"#);
	for frame in db
		.query_as_root(
			r#"
			from demo.events
			take 5
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 3: Take with filter
	log_info!("\nExample 3: Filter ERROR events, then take first 2");
	log_query(
		r#"from demo.events
filter event_type == "ERROR"
take 2"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from demo.events
			filter event_type == "ERROR"
			take 2
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 4: Sort then take (top-N pattern)
	log_info!("\nExample 4: Get 3 most recent events (sort by timestamp desc, take 3)");
	log_query(
		r#"from demo.events
sort timestamp desc
take 3"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from demo.events
			sort timestamp desc
			take 3
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 5: Take with projection
	log_info!("\nExample 5: Project specific columns, then take");
	log_query(
		r#"from demo.events
map { event_type, severity, message }
take 4"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from demo.events
			map { event_type, severity, message }
			take 4
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 6: Comptokenize pipeline with take
	log_info!("\nExample 6: Comptokenize pipeline - filter high severity, sort, take top 3");
	log_query(
		r#"from demo.events
filter severity == "HIGH" or severity == "CRITICAL"
sort timestamp desc
map { id, event_type, severity, message }
take 3"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from demo.events
			filter severity == "HIGH" or severity == "CRITICAL"
			sort timestamp desc
			map { id, event_type, severity, message }
			take 3
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 7: Take 1 (getting single row)
	log_info!("\nExample 7: Take single row (take 1)");
	log_query(
		r#"from demo.events
filter severity == "CRITICAL"
take 1"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from demo.events
			filter severity == "CRITICAL"
			take 1
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 8: Take with no matching results
	log_info!("\nExample 8: Take when filter returns no results");
	log_query(
		r#"from demo.events
filter severity == "UNKNOWN"
take 5"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from demo.events
			filter severity == "UNKNOWN"
			take 5
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}
}
