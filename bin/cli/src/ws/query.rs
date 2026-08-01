// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_client::{Frame, WireFormat, WsClient};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub async fn execute_query(host: &str, port: u16, token: Option<String>, statements: &str) -> Result<()> {
	let mut client = WsClient::connect(&format!("ws://{}:{}", host, port), WireFormat::Frames)
		.await
		.map_err(|e| format!("Failed to connect to WebSocket server: {}", e))?;

	if let Some(ref token) = token {
		client.authenticate(token).await.map_err(|e| format!("Failed to authenticate: {}", e))?;
	}

	let stmts: Vec<&str> = statements.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();

	println!("Executing {} statement(s)...\n", stmts.len());

	for (i, stmt) in stmts.iter().enumerate() {
		println!("=== Statement {} ===", i + 1);
		println!("{}\n", stmt);

		let result = client
			.query(stmt, None)
			.await
			.map_err(|e| format!("Failed to execute statement {}: {}", i + 1, e))?;

		print_query_result(&result);
		println!();
	}

	client.close().await?;

	Ok(())
}

fn print_query_result(frames: &[Frame]) {
	if frames.is_empty() {
		println!("(no results)");
		return;
	}

	for (i, frame) in frames.iter().enumerate() {
		if frames.len() > 1 {
			println!("--- Frame {} ---", i + 1);
		}
		println!("{}", frame);
	}
}
