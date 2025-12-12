// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! WebSocket connection handler.
//!
//! This module handles individual WebSocket connections, including:
//! - WebSocket handshake via tokio-tungstenite
//! - Message parsing and routing
//! - Authentication state management
//! - Query and command execution

use futures_util::{SinkExt, StreamExt};
use reifydb_core::interface::Identity;
use reifydb_sub_server::{
	convert_frames, execute_command, execute_query, extract_identity_from_ws_auth, AppState,
	ExecuteError, Request, RequestPayload,
};
use reifydb_type::Params;
use serde_json::json;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_tungstenite::{accept_async, tungstenite::Message};

/// Handle a single WebSocket connection.
///
/// This function:
/// 1. Completes the WebSocket handshake
/// 2. Manages authentication state per connection
/// 3. Routes messages to appropriate handlers
/// 4. Responds to shutdown signals
///
/// # Arguments
///
/// * `stream` - Raw TCP stream from accept
/// * `state` - Shared application state
/// * `shutdown` - Watch channel for shutdown signal
pub async fn handle_connection(stream: TcpStream, state: AppState, mut shutdown: watch::Receiver<bool>) {
	let peer = stream.peer_addr().ok();

	let ws_stream = match accept_async(stream).await {
		Ok(ws) => ws,
		Err(e) => {
			tracing::warn!("WebSocket handshake failed from {:?}: {}", peer, e);
			return;
		}
	};

	tracing::debug!("WebSocket connection established from {:?}", peer);
	let (mut sender, mut receiver) = ws_stream.split();

	// Connection starts unauthenticated
	let mut identity: Option<Identity> = None;

	loop {
		tokio::select! {
			biased;

			// Check shutdown first
			result = shutdown.changed() => {
				if result.is_err() || *shutdown.borrow() {
					tracing::debug!("WebSocket connection {:?} shutting down", peer);
					let _ = sender.send(Message::Close(None)).await;
					break;
				}
			}

			// Handle incoming messages
			msg = receiver.next() => {
				match msg {
					Some(Ok(Message::Text(text))) => {
						let response = process_message(&text, &state, &mut identity).await;
						if sender.send(Message::Text(response.into())).await.is_err() {
							tracing::debug!("Failed to send response to {:?}", peer);
							break;
						}
					}
					Some(Ok(Message::Ping(data))) => {
						if sender.send(Message::Pong(data)).await.is_err() {
							break;
						}
					}
					Some(Ok(Message::Pong(_))) => {
						// Client responded to our ping, connection is alive
					}
					Some(Ok(Message::Close(frame))) => {
						tracing::debug!("Client {:?} closed connection: {:?}", peer, frame);
						break;
					}
					Some(Ok(Message::Binary(_))) => {
						let err = build_error("0", "UNSUPPORTED", "Binary messages not supported");
						let _ = sender.send(Message::Text(err.into())).await;
					}
					Some(Ok(Message::Frame(_))) => {
						// Raw frame, ignore
					}
					Some(Err(e)) => {
						tracing::warn!("WebSocket error from {:?}: {}", peer, e);
						break;
					}
					None => {
						tracing::debug!("Client {:?} disconnected", peer);
						break;
					}
				}
			}
		}
	}
}

/// Process a single WebSocket message.
///
/// Parses the message and routes to the appropriate handler based on type.
async fn process_message(text: &str, state: &AppState, identity: &mut Option<Identity>) -> String {
	let request: Request = match serde_json::from_str(text) {
		Ok(r) => r,
		Err(e) => {
			return build_error("0", "PARSE_ERROR", &format!("Invalid JSON: {}", e));
		}
	};

	match request.payload {
		RequestPayload::Auth(auth) => {
			match extract_identity_from_ws_auth(auth.token.as_deref()) {
				Ok(id) => {
					*identity = Some(id);
					build_response(&request.id, "Auth", json!({}))
				}
				Err(e) => build_error(&request.id, "AUTH_FAILED", &format!("{:?}", e)),
			}
		}

		RequestPayload::Query(q) => {
			let id = match identity.as_ref() {
				Some(id) => id.clone(),
				None => return build_error(&request.id, "AUTH_REQUIRED", "Authentication required"),
			};

			let params = q.params.unwrap_or(Params::None);
			let query = q.statements.join("; ");
			let timeout = state.query_timeout();

			match execute_query(state.engine_clone(), query, id, params, timeout).await {
				Ok(frames) => {
					let ws_frames = convert_frames(frames);
					build_response(&request.id, "Query", json!({ "frames": ws_frames }))
				}
				Err(e) => error_to_response(&request.id, e),
			}
		}

		RequestPayload::Command(c) => {
			let id = match identity.as_ref() {
				Some(id) => id.clone(),
				None => return build_error(&request.id, "AUTH_REQUIRED", "Authentication required"),
			};

			let params = c.params.unwrap_or(Params::None);
			let timeout = state.query_timeout();

			match execute_command(state.engine_clone(), c.statements, id, params, timeout).await {
				Ok(frames) => {
					let ws_frames = convert_frames(frames);
					build_response(&request.id, "Command", json!({ "frames": ws_frames }))
				}
				Err(e) => error_to_response(&request.id, e),
			}
		}
	}
}

/// Convert an ExecuteError to a JSON response string.
fn error_to_response(id: &str, e: ExecuteError) -> String {
	match e {
		ExecuteError::Timeout => build_error(id, "QUERY_TIMEOUT", "Query execution timed out"),
		ExecuteError::TaskPanic(msg) => build_error(id, "INTERNAL_ERROR", &msg),
		ExecuteError::Engine {
			error,
			statement,
		} => {
			// Get diagnostic and add statement context for proper display
			let mut diagnostic = error.diagnostic();
			diagnostic.with_statement(statement);
			json!({
				"id": id,
				"type": "Err",
				"payload": {
					"diagnostic": diagnostic
				}
			})
			.to_string()
		}
	}
}

/// Build a success response JSON string.
fn build_response(id: &str, msg_type: &str, data: serde_json::Value) -> String {
	json!({
		"id": id,
		"type": msg_type,
		"payload": data
	})
	.to_string()
}

/// Build an error response JSON string.
fn build_error(id: &str, code: &str, message: &str) -> String {
	json!({
		"id": id,
		"type": "Err",
		"payload": {
			"diagnostic": {
				"code": code,
				"statement": null,
				"message": message,
				"column": null,
				"fragment": "None",
				"label": null,
				"help": null,
				"notes": [],
				"cause": null
			}
		}
	})
	.to_string()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_build_response() {
		let response = build_response("123", "query", json!({"result": "ok"}));
		let parsed: serde_json::Value = serde_json::from_str(&response).unwrap();
		assert_eq!(parsed["id"], "123");
		assert_eq!(parsed["type"], "query");
		assert_eq!(parsed["payload"]["result"], "ok");
	}

	#[test]
	fn test_build_error() {
		let response = build_error("456", "AUTH_REQUIRED", "Please authenticate");
		let parsed: serde_json::Value = serde_json::from_str(&response).unwrap();
		assert_eq!(parsed["id"], "456");
		assert_eq!(parsed["type"], "error");
		assert_eq!(parsed["payload"]["code"], "AUTH_REQUIRED");
		assert_eq!(parsed["payload"]["message"], "Please authenticate");
	}
}
