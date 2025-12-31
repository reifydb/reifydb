// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
//! Example demonstrating async HTTP client usage

use reifydb_client::HttpClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	// Connect to the server
	let mut client = HttpClient::connect("http://localhost:8080").await?;

	// Authenticate
	let token = std::env::var("REIFYDB_TOKEN").unwrap_or_else(|_| "root".to_string());
	client.authenticate(&token);

	println!("Connected to ReifyDB via HTTP");

	// Execute a query
	let result = client.query("from system.tables", None).await?;

	println!("Query executed: {} frames returned", result.frames.len());

	for frame in result.frames {
		println!("{}", frame);
	}

	Ok(())
}
