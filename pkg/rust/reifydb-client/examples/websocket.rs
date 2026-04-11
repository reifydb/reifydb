// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
//! Example demonstrating async WebSocket client usage

use std::{env, error::Error};

use reifydb_client::{Encoding, WsClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
	// Connect to the server
	let mut client = WsClient::connect("ws://localhost:8090", Encoding::Json).await?;

	// Authenticate
	let token = env::var("REIFYDB_TOKEN").unwrap_or_else(|_| "root".to_string());
	client.authenticate(&token).await?;

	println!("Connected to ReifyDB via WebSocket");

	// Execute a query
	let result = client.query("from system.tables", None).await?;

	println!("Query executed: {} frames returned", result.frames.len());

	for frame in result.frames {
		println!("{}", frame);
	}

	// Close gracefully
	client.close().await?;

	Ok(())
}
