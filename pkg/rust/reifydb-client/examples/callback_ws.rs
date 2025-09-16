// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{thread, time::Duration};

use reifydb_client::Client;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	// Connect to ReifyDB server
	let client = Client::ws(("127.0.0.1", 8090))?;

	// Create a callback session with authentication
	let session = client.callback_session(Some("mysecrettoken".to_string()))?;

	// Execute a command to create a table
	let command_id = session.command(
		"CREATE NAMESPACE test; CREATE TABLE test.users { id: INT4, name: UTF8 }",
		None,
		|result| match result {
			Ok(data) => println!("Command executed: {} frames returned", data.frames.len()),
			Err(e) => println!("Command failed: {}", e),
		},
	)?;
	println!("Command sent with ID: {}", command_id);

	// Execute a query
	let query_id = session.query("MAP { x: 42, y: 'hello' }", None, |result| {
		match result {
			Ok(data) => {
				println!("Query executed: {} frames returned", data.frames.len());
				// Print first frame if available
				if let Some(frame) = data.frames.first() {
					println!("First frame:\n{}", frame);
				}
			}
			Err(e) => println!("Query failed: {}", e),
		}
	})?;
	println!("Query sent with ID: {}", query_id);

	// Wait for callbacks to complete
	thread::sleep(Duration::from_millis(500));

	Ok(())
}
