// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_client::{Frame, WireFormat, WsClient};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub async fn execute_admin(host: &str, port: u16, token: Option<String>, statements: &str) -> Result<()> {
	let mut client = WsClient::connect(&format!("ws://{}:{}", host, port), WireFormat::Frames)
		.await
		.map_err(|e| format!("Failed to connect to WebSocket server: {}", e))?;

	if let Some(ref token) = token {
		client.authenticate(token).await.map_err(|e| format!("Failed to authenticate: {}", e))?;
	}

	let stmts: Vec<&str> = statements.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();

	println!("Executing {} admin statement(s)...\n", stmts.len());

	for (i, stmt) in stmts.iter().enumerate() {
		println!("=== Admin {} ===", i + 1);
		println!("{}\n", stmt);

		let result = client
			.admin(stmt, None)
			.await
			.map_err(|e| format!("Failed to execute admin statement {}: {}", i + 1, e))?;

		print_admin_result(&result);
		println!();
	}

	client.close().await?;

	Ok(())
}

fn print_admin_result(frames: &[Frame]) {
	if frames.is_empty() {
		println!("Admin statement executed successfully (no results)");
		return;
	}

	for (i, frame) in frames.iter().enumerate() {
		if frames.len() > 1 {
			println!("--- Frame {} ---", i + 1);
		}
		println!("{}", frame);
	}
}
