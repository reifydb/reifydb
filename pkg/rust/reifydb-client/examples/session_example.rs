// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{thread, time::Duration};

use reifydb::core::Value;
use reifydb_client::{Client, Params};

fn main() -> Result<(), Box<dyn std::error::Error>> {
	println!("=== ReifyDB Session-Based Client Example ===\n");

	// Create a single client connection
	let client = Client::connect("127.0.0.1:8090")?;
	println!("✓ Connected to ReifyDB server\n");

	// Example 1: Blocking Session
	println!("1. Using Blocking Session:");
	println!("   ------------------------");
	{
		let mut blocking = client.blocking_session(None)?;

		// Execute a simple query
		match blocking.query("MAP { 1 }", None) {
			Ok(result) => {
				println!("   Query successful!");
				println!("   Frames: {}", result.frames.len());
				for frame in &result.frames {
					println!(
						"   - Frame '{}' with {} rows",
						frame.name,
						frame.rows.len()
					);
					for row in &frame.rows {
						println!(
							"     Row: {:?}",
							row.values
						);
					}
				}
			}
			Err(e) => println!("   Query failed: {}", e),
		}

		// Execute with parameters
		let params = Params::Positional(vec![Value::Int4(42)]);
		match blocking.query("MAP { $1 }", Some(params)) {
			Ok(result) => {
				println!(
					"   Parameterized query returned {} rows",
					result.rows_returned
				);
			}
			Err(e) => {
				println!("   Parameterized query failed: {}", e)
			}
		}
	}
	println!();

	// Example 2: Callback Session
	println!("2. Using Callback Session:");
	println!("   ----------------------");
	{
		let callback = client.callback_session(None)?;

		// Send query with callback
		let request_id = callback.query(
			"MAP { 2 }",
			None,
			|result| match result {
				Ok(data) => {
					println!(
						"   Callback: Query completed!"
					);
					println!(
						"   Callback: Got {} frames",
						data.frames.len()
					);
				}
				Err(e) => println!(
					"   Callback: Query failed: {}",
					e
				),
			},
		)?;

		println!("   Sent query with ID: {}", request_id);

		// Give callback time to execute
		thread::sleep(Duration::from_millis(100));
	}
	println!();

	// Example 3: Channel Session
	println!("3. Using Channel Session:");
	println!("   ----------------------");
	{
		let (channel, rx) = client.channel_session(None)?;

		// Send multiple queries
		let id1 = channel.query("MAP { 3 }", None)?;
		let id2 = channel.command("MAP { 4 }", None)?;
		let id3 = channel.query("MAP { 5 }", None)?;

		println!("   Sent 3 requests: {}, {}, {}", id1, id2, id3);

		// Receive responses
		let mut received = 0;
		while received < 3 {
			match rx.recv_timeout(Duration::from_secs(1)) {
				Ok(msg) => {
					received += 1;
					match msg.response {
						Ok(response) => {
							println!("   Received response for {}: {:?}", msg.request_id, response.payload);
						}
						Err(e) => {
							println!(
								"   Error for {}: {}",
								msg.request_id,
								e
							);
						}
					}
				}
				Err(_) => {
					println!(
						"   Timeout waiting for responses"
					);
					break;
				}
			}
		}
	}
	println!();

	// Example 4: Multiple concurrent sessions
	println!("4. Multiple Concurrent Sessions:");
	println!("   -----------------------------");
	{
		let client_clone1 = client.clone();
		let client_clone2 = client.clone();

		// Spawn threads with different session types
		let handle1 = thread::spawn(move || {
			let mut blocking =
				client_clone1.blocking_session(None).unwrap();
			match blocking.query("MAP { 'thread1' }", None) {
				Ok(_) => println!(
					"   Thread 1 (blocking): Success"
				),
				Err(e) => println!(
					"   Thread 1 (blocking): Failed - {}",
					e
				),
			}
		});

		let handle2 = thread::spawn(move || {
			let callback =
				client_clone2.callback_session(None).unwrap();
			callback.query("MAP { 'thread2' }", None, |result| {
				match result {
					Ok(_) => println!(
						"   Thread 2 (callback): Success"
					),
					Err(e) => println!(
						"   Thread 2 (callback): Failed - {}",
						e
					),
				}
			})
			.unwrap();
			thread::sleep(Duration::from_millis(100));
		});

		handle1.join().unwrap();
		handle2.join().unwrap();
	}
	println!();

	// Close the client
	println!("Closing client connection...");
	client.close()?;
	println!("✓ Connection closed successfully!");

	Ok(())
}
