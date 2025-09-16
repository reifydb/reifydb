// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use reifydb_client::Client;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let client = Client::http(("127.0.0.1", 8090))?;

	// Create a blocking session with authentication
	let mut session = client.blocking_session(Some("mysecrettoken".to_string()))?;

	// Execute a command to create a table
	let command_result =
		session.command("CREATE NAMESPACE test; CREATE TABLE test.users { id: INT4, name: UTF8 }", None)?;
	println!("Command executed: {} frames returned", command_result.frames.len());

	// Execute a query
	let query_result = session.query("MAP { x: 42, y: 'hello' }", None)?;

	println!("Query executed: {} frames returned", query_result.frames.len());

	// Print first frame if available
	if let Some(frame) = query_result.frames.first() {
		println!("First frame:\n{}", frame);
	}

	Ok(())
}
