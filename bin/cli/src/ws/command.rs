// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_client::{CommandResult, WsClient};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub async fn execute_command(host: &str, port: u16, token: Option<String>, statements: &str) -> Result<()> {
	// 1. Connect to server
	let mut client = WsClient::connect(&format!("ws://{}:{}", host, port))
		.await
		.map_err(|e| format!("Failed to connect to WebSocket server: {}", e))?;

	// 2. Authenticate if token provided
	if let Some(ref token) = token {
		client.authenticate(token).await.map_err(|e| format!("Failed to authenticate: {}", e))?;
	}

	// 3. Split statements by semicolon
	let stmts: Vec<&str> = statements.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();

	println!("Executing {} command(s)...\n", stmts.len());

	// 4. Execute each statement in order
	for (i, stmt) in stmts.iter().enumerate() {
		println!("=== Command {} ===", i + 1);
		println!("{}\n", stmt);

		// Execute command
		let result = client
			.command(stmt, None)
			.await
			.map_err(|e| format!("Failed to execute command {}: {}", i + 1, e))?;

		// Print frames
		print_command_result(&result);
		println!();
	}

	// 5. Close connection
	client.close().await?;

	Ok(())
}

fn print_command_result(result: &CommandResult) {
	if result.frames.is_empty() {
		println!("Command executed successfully (no results)");
		return;
	}

	for (i, frame) in result.frames.iter().enumerate() {
		if result.frames.len() > 1 {
			println!("--- Frame {} ---", i + 1);
		}
		println!("{}", frame);
	}
}
