// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::time::Duration;

use reifydb::{WithSubsystem, server, sub_server_ws::WsConfig};
use reifydb_client::WsClient;
use tokio::time::{sleep, timeout};

fn main() {
	println!("=== WebSocket Subscription Demo ===\n");

	// 1. Create server with WebSocket subsystem enabled (synchronous)
	println!(">>> Creating database with WebSocket server...");
	let mut db = server::memory()
		.with_ws(WsConfig::default().bind_addr("127.0.0.1:8091"))
		.with_flow(|f| f)
		.build()
		.unwrap();

	println!("Database built with {} subsystems", db.subsystem_count());

	// 2. Start the database (this starts the WS server)
	println!(">>> Starting database...");
	db.start().unwrap();
	println!("Database started successfully!");
	println!("WebSocket server listening on ws://127.0.0.1:8091");

	// 3. Create a runtime for the async client code
	let rt = tokio::runtime::Runtime::new().unwrap();
	rt.block_on(async {
		// Give the server a moment to start accepting connections
		sleep(Duration::from_millis(100)).await;

		// 4. Connect WebSocket client
		println!("\n>>> Connecting WebSocket client...");
		let mut client = WsClient::connect("ws://127.0.0.1:8091").await.unwrap();
		println!("Client connected!");

		// 5. Authenticate
		println!(">>> Authenticating with 'root' token...");
		client.authenticate("root").await.unwrap();
		println!("Authenticated successfully!");

		// 6. Create namespace and table via WebSocket
		println!("\n>>> Creating namespace 'demo'...");
		client.command("create namespace demo;", None).await.unwrap();
		println!("Namespace created!");

		println!(">>> Creating table 'demo.events'...");
		client.command(
			r#"create table demo.events {
				id: int4,
				message: utf8,
				timestamp: uint8
			}"#,
			None,
		)
		.await
		.unwrap();
		println!("Table created!");

		// 7. Subscribe to changes on demo.events
		println!("\n>>> Subscribing to 'from demo.events'...");
		let subscription_id = client.subscribe("from demo.events").await.unwrap();
		println!("Subscribed! Subscription ID: {}", subscription_id);

		// 8. Insert some test data
		println!("\n>>> Inserting test data...");
		client.command(
			r#"from [{
				id: 1,
				message: "First event",
				timestamp: 1000
			}, {
				id: 2,
				message: "Second event",
				timestamp: 2000
			}, {
				id: 3,
				message: "Third event",
				timestamp: 3000
			}] insert demo.events"#,
			None,
		)
		.await
		.unwrap();
		println!("Data inserted!");

		// 9. Receive change notifications
		println!("\n>>> Waiting for change notifications...");
		println!("    (The server sends test frames every 2 seconds)");

		// Try to receive a few change notifications
		for i in 1..=3 {
			println!("\n--- Waiting for notification #{} ---", i);
			match timeout(Duration::from_secs(5), client.recv()).await {
				Ok(Some(change)) => {
					println!("Received change notification!");
					println!("  Subscription ID: {}", change.subscription_id);
					println!("  Frame:");
					for col in &change.frame.columns {
						println!("    Column '{}' ({}): {:?}", col.name, col.r#type, col.data);
					}
				}
				Ok(None) => {
					println!("Connection closed");
					break;
				}
				Err(_) => {
					println!("Timeout waiting for notification");
				}
			}
		}

		// 10. Query the table to verify data
		println!("\n>>> Querying demo.events...");
		let result = client.query("from demo.events", None).await.unwrap();
		for frame in result.frames {
			println!("Query result:");
			println!("{}", frame);
		}

		// 11. Unsubscribe
		println!("\n>>> Unsubscribing...");
		client.unsubscribe(&subscription_id).await.unwrap();
		println!("Unsubscribed!");

		// 12. Close client
		println!("\n>>> Closing client...");
		client.close().await.unwrap();
		println!("Client closed!");
	});

	// 13. Stop database
	println!(">>> Stopping database...");
	db.stop().unwrap();
	println!("Database stopped!");

	println!("\n=== Demo complete! ===");
}
