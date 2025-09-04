// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::time::Duration;

use reifydb_client::{ChannelResponse, Client};

fn main() -> Result<(), Box<dyn std::error::Error>> {
	// Connect to ReifyDB server
	let client = Client::connect("127.0.0.1:8090")?;

	// Create a channel session with authentication
	let (session, receiver) =
		client.channel_session(Some("mysecrettoken".to_string()))?;

	// Consume authentication response
	if let Ok(msg) = receiver.recv_timeout(Duration::from_millis(100)) {
		if let Ok(ChannelResponse::Auth {
			request_id,
		}) = msg.response
		{
			println!("Authenticated with ID: {}", request_id);
		}
	}

	// Execute a command to create a table
	let command_id = session.command(
		"CREATE SCHEMA test; CREATE TABLE test.users { id: INT4, name: UTF8 }",
		None,
	)?;
	println!("Command sent with ID: {}", command_id);

	// Execute a query
	let query_id = session.query("MAP { x: 42, y: 'hello' }", None)?;
	println!("Query sent with ID: {}", query_id);

	// Receive responses
	let mut received = 0;
	while received < 2 {
		match receiver.recv_timeout(Duration::from_secs(1)) {
			Ok(msg) => {
				match msg.response {
					Ok(ChannelResponse::Command {
						request_id,
						result,
					}) => {
						println!(
							"Command {} executed: {} frames returned",
							request_id,
							result.frames.len()
						);
						received += 1;
					}
					Ok(ChannelResponse::Query {
						request_id,
						result,
					}) => {
						println!(
							"Query {} executed: {} frames returned",
							request_id,
							result.frames.len()
						);
						// Print first frame if
						// available
						if let Some(frame) =
							result.frames.first()
						{
							println!(
								"First frame:\n{}",
								frame
							);
						}
						received += 1;
					}
					Ok(ChannelResponse::Auth {
						..
					}) => {
						// Already handled above
					}
					Err(e) => {
						println!(
							"Request {} failed: {}",
							msg.request_id, e
						);
						received += 1;
					}
				}
			}
			Err(_) => {
				println!("Timeout waiting for responses");
				break;
			}
		}
	}

	Ok(())
}
