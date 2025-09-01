// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{
	sync::{Arc, Mutex, mpsc},
	thread,
	time::Duration,
};

use super::{
	message::InternalMessage,
	router::{RequestRouter, route_error, route_response},
};
use crate::WebSocketClient;

/// The background worker thread that handles all WebSocket communication
pub(crate) fn worker_thread(
	url: String,
	command_rx: mpsc::Receiver<InternalMessage>,
	request_router: Arc<Mutex<RequestRouter>>,
) {
	// Connect to WebSocket
	let mut ws_client = match WebSocketClient::connect(&url) {
		Ok(client) => client,
		Err(e) => {
			eprintln!("Failed to connect to WebSocket: {}", e);
			return;
		}
	};

	println!("WebSocket worker thread started for {}", url);

	// Poll for commands and responses
	loop {
		// Check for outgoing requests (non-blocking)
		match command_rx.try_recv() {
			Ok(InternalMessage::Request {
				id,
				request,
				route,
			}) => {
				// Send the request via WebSocket
				if let Err(e) = ws_client.send_request(&request)
				{
					// Send error back through the route
					route_error(&id, e.to_string(), route);
				} else {
					// Store the route for when response
					// arrives
					request_router
						.lock()
						.unwrap()
						.add_route(id, route);
				}
			}
			Ok(InternalMessage::Close) => {
				let _ = ws_client.close();
				break;
			}
			Err(mpsc::TryRecvError::Empty) => {
				// No messages, continue
			}
			Err(mpsc::TryRecvError::Disconnected) => {
				// Channel closed, exit
				let _ = ws_client.close();
				break;
			}
		}

		// Check for incoming WebSocket responses (non-blocking)
		match ws_client.receive() {
			Ok(Some(response)) => {
				let mut router = request_router.lock().unwrap();
				if let Some(route) =
					router.remove_route(&response.id)
				{
					// Route the response to the appropriate
					// session
					route_response(response, route);
				}
			}
			Ok(None) => {
				// No response available
			}
			Err(e) => {
				// Handle connection errors
				if !ws_client.is_connected() {
					eprintln!(
						"WebSocket connection lost: {}",
						e
					);
					// Try to reconnect
					match WebSocketClient::connect(&url) {
						Ok(new_client) => {
							ws_client = new_client;
							println!(
								"Reconnected to WebSocket"
							);
						}
						Err(e) => {
							eprintln!(
								"Failed to reconnect: {}",
								e
							);
							thread::sleep(Duration::from_secs(5));
						}
					}
				}
			}
		}

		// Small sleep to prevent busy waiting
		thread::sleep(Duration::from_millis(1));
	}

	println!("WebSocket worker thread stopped");
}
