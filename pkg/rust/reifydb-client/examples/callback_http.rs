// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{thread, time::Duration};

use reifydb_client::http::HttpCallbackSession;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	// Connect to ReifyDB HTTP server
	let session = HttpCallbackSession::new(
		"127.0.0.1",
		8090,
		Some("mysecrettoken".to_string()),
	)?;

	// Execute a command to create a table
	session.command(
		"CREATE SCHEMA test; CREATE TABLE test.users { id: INT4, name: UTF8 }",
		None,
		|result| match result {
			Ok(data) => println!(
				"Command executed: {} frames returned",
				data.frames.len()
			),
			Err(e) => println!("Command failed: {}", e),
		},
	)?;
	println!("Command sent");

	// Execute a query
	session.query("MAP { x: 42, y: 'hello' }", None, |result| {
		match result {
			Ok(data) => {
				println!(
					"Query executed: {} frames returned",
					data.frames.len()
				);
				// Print first frame if available
				if let Some(frame) = data.frames.first() {
					println!("First frame:\n{}", frame);
				}
			}
			Err(e) => println!("Query failed: {}", e),
		}
	})?;
	println!("Query sent");

	// Wait for callbacks to complete
	thread::sleep(Duration::from_millis(500));

	Ok(())
}
