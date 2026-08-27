// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{env, error::Error};

use reifydb_client::{WireFormat, WsClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
	let mut client = WsClient::connect("ws://localhost:8090", WireFormat::Frames).await?;

	let token = env::var("REIFYDB_TOKEN").unwrap_or_else(|_| "root".to_string());
	client.authenticate(&token).await?;

	println!("Connected to ReifyDB via WebSocket");

	let result = client.query("from system.tables", None).await?;

	println!("Query executed: {} frames returned", result.len());

	for frame in result {
		println!("{}", frame);
	}

	client.close().await?;

	Ok(())
}
