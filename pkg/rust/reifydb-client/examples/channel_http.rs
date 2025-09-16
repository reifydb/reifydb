// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{collections::HashMap, time::Duration};

use reifydb_client::http::{HttpChannelResponse, HttpChannelSession};

fn main() -> Result<(), Box<dyn std::error::Error>> {
	// Connect to ReifyDB HTTP server
	let (session, receiver) = HttpChannelSession::new("127.0.0.1", 8090, Some("mysecrettoken".to_string()))?;

	// Consume authentication response
	if let Ok(msg) = receiver.recv_timeout(Duration::from_millis(100)) {
		if let Ok(HttpChannelResponse::Auth {
			request_id,
		}) = msg.response
		{
			println!("Authenticated with ID: {}", request_id);
		}
	}

	// Send multiple requests asynchronously to demonstrate multiplexing
	println!("Sending multiple requests concurrently...");

	// Send a command to create a table
	let command_id = session.command("MAP{1}", None)?;
	println!("Command sent with ID: {}", command_id);

	// Send multiple queries concurrently
	let query_id1 = session.query("MAP { x: 42, y: 'hello' }", None)?;
	println!("Query 1 sent with ID: {}", query_id1);

	let query_id2 = session.query("MAP { a: 123, b: 'world' }", None)?;
	println!("Query 2 sent with ID: {}", query_id2);

	let query_id3 = session.query("MAP { count: 999, active: true }", None)?;
	println!("Query 3 sent with ID: {}", query_id3);

	// Track which requests we're expecting
	let mut expected_requests = HashMap::new();
	expected_requests.insert(command_id.clone(), "MAP{1}");
	expected_requests.insert(query_id1.clone(), "Query 1 (x: 42, y: 'hello')");
	expected_requests.insert(query_id2.clone(), "Query 2 (a: 123, b: 'world')");
	expected_requests.insert(query_id3.clone(), "Query 3 (count: 999, active: true)");

	println!("\nWaiting for responses (they may arrive out of order)...");

	// Receive responses asynchronously - they may arrive in any order
	let mut received = 0;
	let total_expected = expected_requests.len();

	while received < total_expected {
		match receiver.recv_timeout(Duration::from_secs(2)) {
			Ok(msg) => {
				let request_description =
					expected_requests.get(&msg.request_id).unwrap_or(&"Unknown request");

				match msg.response {
					Ok(HttpChannelResponse::Command {
						request_id,
						result,
					}) => {
						println!(
							"✓ Received response for {}: Command {} executed ({} frames)",
							request_description,
							request_id,
							result.frames.len()
						);
						expected_requests.remove(&request_id);
						received += 1;
					}
					Ok(HttpChannelResponse::Query {
						request_id,
						result,
					}) => {
						println!(
							"✓ Received response for {}: Query {} executed ({} frames)",
							request_description,
							request_id,
							result.frames.len()
						);

						// Print first frame if
						// available
						if let Some(frame) = result.frames.first() {
							println!("{frame}");
						}

						expected_requests.remove(&request_id);
						received += 1;
					}
					Ok(HttpChannelResponse::Auth {
						..
					}) => {
						// Already handled above
					}
					Err(e) => {
						println!(
							"✗ Request {} ({}) failed: {}",
							msg.request_id, request_description, e
						);
						expected_requests.remove(&msg.request_id);
						received += 1;
					}
				}
			}
			Err(_) => {
				println!("Timeout waiting for responses");
				if !expected_requests.is_empty() {
					println!(
						"Still waiting for: {:?}",
						expected_requests.values().collect::<Vec<_>>()
					);
				}
				break;
			}
		}
	}

	if expected_requests.is_empty() {
		println!("\n🎉 All {} requests completed successfully!", total_expected);
		println!("This demonstrates async HTTP multiplexing - requests were sent concurrently");
		println!("and responses were received and processed as they arrived.");
	} else {
		println!("\n⚠️  {} requests did not complete within timeout", expected_requests.len());
	}

	Ok(())
}
