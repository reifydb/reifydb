// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use reifydb_core::interface::catalog::id::SubscriptionId as DbSubscriptionId;
use reifydb_sub_server::{
	auth::extract_identity_from_ws_auth,
	execute::{ExecuteError, execute_admin, execute_command, execute_query},
	response::{convert_frames, resolve_response_json},
	state::AppState,
	subscribe::cleanup_subscription,
};
use reifydb_subscription::poller::SubscriptionPoller;
use reifydb_type::{
	params::Params,
	value::{identity::IdentityId, uuid::Uuid7},
};
use serde_json::from_str;
use tokio::{
	net::TcpStream,
	select,
	sync::{mpsc, watch},
	time::timeout,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::{
	protocol::{Request, RequestPayload},
	response::{CONTENT_TYPE_FRAMES, CONTENT_TYPE_JSON, Response, ServerPush},
	subscription::{
		handler::handle_subscribe,
		registry::{PushMessage, SubscriptionRegistry},
	},
};

/// Handle a single WebSocket connection.
///
/// This function:
/// 1. Completes the WebSocket handshake
/// 2. Manages authentication state per connection
/// 3. Routes messages to appropriate handler
/// 4. Handles subscription push messages
/// 5. Responds to shutdown signals
/// 6. Cleans up subscriptions on disconnect
///
/// # Arguments
///
/// * `stream` - Raw TCP stream from accept
/// * `state` - Shared application state
/// * `registry` - Subscription registry for push notifications
/// * `poller` - Subscription poller for consuming subscription data
/// * `shutdown` - Watch channel for shutdown signal
pub async fn handle_connection(
	stream: TcpStream,
	state: AppState,
	registry: Arc<SubscriptionRegistry>,
	poller: Arc<SubscriptionPoller>,
	mut shutdown: watch::Receiver<bool>,
) {
	let peer = stream.peer_addr().ok();
	let connection_id = Uuid7::generate();

	// Set TCP_NODELAY to disable Nagle's algorithm for lower latency
	if let Err(e) = stream.set_nodelay(true) {
		warn!("Failed to set TCP_NODELAY for {:?}: {}", peer, e);
	}

	// Wrap accept_async with a 30-second timeout to prevent hanging on slow/malicious clients
	let ws_stream = match timeout(Duration::from_secs(30), accept_async(stream)).await {
		Ok(Ok(ws)) => ws,
		Ok(Err(_)) => {
			return;
		}
		Err(_) => {
			return;
		}
	};

	debug!("WebSocket connection {} established from {:?}", connection_id, peer);
	let (mut sender, mut receiver) = ws_stream.split();

	// Channel for receiving push messages from the registry
	let (push_tx, mut push_rx) = mpsc::channel::<PushMessage>(100);

	// Connection starts unauthenticated
	let mut identity: Option<IdentityId> = None;

	loop {
		select! {
			biased;

			// Check shutdown first
			result = shutdown.changed() => {
				if result.is_err() || *shutdown.borrow() {
					debug!("WebSocket connection {:?} shutting down", peer);
					let _ = sender.send(Message::Close(None)).await;
					break;
				}
			}

			// Handle push messages from the subscription registry
			Some(push) = push_rx.recv() => {

				let msg = match push {
					PushMessage::Change { subscription_id, content_type, body } => {
						ServerPush::change(subscription_id.to_string(), content_type, body).to_json()
					}
				};
				if sender.send(Message::Text(msg.into())).await.is_err() {
					debug!("Failed to send push message to {:?}", peer);
					break;
				}
			}

			// Handle incoming messages
			msg = receiver.next() => {
				match msg {
					Some(Ok(Message::Text(text))) => {
						let response = process_message(
							&text,
							&state,
							&mut identity,
							connection_id,
							&registry,
							&poller,
							push_tx.clone(),
						).await;
						if let Some(resp) = response {
							if sender.send(Message::Text(resp.into())).await.is_err() {
								debug!("Failed to send response to {:?}", peer);
								break;
							}
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
						debug!("Client {:?} closed connection: {:?}", peer, frame);
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
						warn!("WebSocket error from {:?}: {}", peer, e);
						break;
					}
					None => {
						debug!("Client {:?} disconnected", peer);
						break;
					}
				}
			}
		}
	}

	// Cleanup all subscriptions for this connection
	let subscription_ids = registry.cleanup_connection(connection_id);

	// Cleanup database subscriptions for each subscription
	for subscription_id in subscription_ids {
		// Unregister from poller first
		poller.unregister(&subscription_id);

		// Delete the subscription and its associated flow from database
		if let Err(e) = cleanup_subscription(&state, subscription_id).await {
			warn!("Failed to cleanup subscription {} from database: {:?}", subscription_id, e);
		}
	}

	debug!("WebSocket connection {} from {:?} cleaned up", connection_id, peer);
}

/// Connection ID type alias for clarity.
type ConnectionId = Uuid7;

/// Process a single WebSocket message.
///
/// Parses the message and routes to the appropriate handler based on type.
/// Returns None if no response should be sent (e.g., for internal errors already logged).
async fn process_message(
	text: &str,
	state: &AppState,
	identity: &mut Option<IdentityId>,
	connection_id: ConnectionId,
	registry: &SubscriptionRegistry,
	poller: &SubscriptionPoller,
	push_tx: mpsc::Sender<PushMessage>,
) -> Option<String> {
	let request: Request = match from_str(text) {
		Ok(r) => r,
		Err(e) => {
			return Some(build_error("0", "PARSE_ERROR", &format!("Invalid JSON: {}", e)));
		}
	};

	match request.payload {
		RequestPayload::Auth(auth) => match extract_identity_from_ws_auth(auth.token.as_deref()) {
			Ok(id) => {
				*identity = Some(id);
				Some(Response::auth(&request.id).to_json())
			}
			Err(e) => Some(build_error(&request.id, "AUTH_FAILED", &format!("{:?}", e))),
		},

		RequestPayload::Admin(a) => {
			let id: IdentityId = match identity.as_ref() {
				Some(id) => *id,
				None => {
					return Some(build_error(
						&request.id,
						"AUTH_REQUIRED",
						"Authentication required",
					));
				}
			};

			let format = a.format.clone();
			let unwrap = a.unwrap.unwrap_or(false);
			let params = match a.params {
				None => Params::None,
				Some(wp) => match wp.into_params() {
					Ok(p) => p,
					Err(e) => return Some(build_error(&request.id, "INVALID_PARAMS", &e)),
				},
			};
			let timeout = state.query_timeout();

			match execute_admin(
				state.actor_system(),
				state.engine_clone(),
				a.statements,
				id,
				params,
				timeout,
			)
			.await
			{
				Ok(frames) => {
					let (content_type, body) =
						build_response_body(frames, format.as_deref(), unwrap);
					Some(Response::admin(&request.id, content_type, body).to_json())
				}
				Err(e) => Some(error_to_response(&request.id, e)),
			}
		}

		RequestPayload::Query(q) => {
			let id: IdentityId = match identity.as_ref() {
				Some(id) => *id,
				None => {
					return Some(build_error(
						&request.id,
						"AUTH_REQUIRED",
						"Authentication required",
					));
				}
			};

			let format = q.format.clone();
			let unwrap = q.unwrap.unwrap_or(false);
			let params = match q.params {
				None => Params::None,
				Some(wp) => match wp.into_params() {
					Ok(p) => p,
					Err(e) => return Some(build_error(&request.id, "INVALID_PARAMS", &e)),
				},
			};
			let query = q.statements.join("; ");
			let timeout = state.query_timeout();

			match execute_query(state.actor_system(), state.engine_clone(), query, id, params, timeout)
				.await
			{
				Ok(frames) => {
					let (content_type, body) =
						build_response_body(frames, format.as_deref(), unwrap);
					Some(Response::query(&request.id, content_type, body).to_json())
				}
				Err(e) => Some(error_to_response(&request.id, e)),
			}
		}

		RequestPayload::Command(c) => {
			let id: IdentityId = match identity.as_ref() {
				Some(id) => *id,
				None => {
					return Some(build_error(
						&request.id,
						"AUTH_REQUIRED",
						"Authentication required",
					));
				}
			};

			let format = c.format.clone();
			let unwrap = c.unwrap.unwrap_or(false);
			let params = match c.params {
				None => Params::None,
				Some(wp) => match wp.into_params() {
					Ok(p) => p,
					Err(e) => return Some(build_error(&request.id, "INVALID_PARAMS", &e)),
				},
			};
			let timeout = state.query_timeout();

			match execute_command(
				state.actor_system(),
				state.engine_clone(),
				c.statements,
				id,
				params,
				timeout,
			)
			.await
			{
				Ok(frames) => {
					let (content_type, body) =
						build_response_body(frames, format.as_deref(), unwrap);
					Some(Response::command(&request.id, content_type, body).to_json())
				}
				Err(e) => Some(error_to_response(&request.id, e)),
			}
		}

		RequestPayload::Subscribe(sub) => {
			handle_subscribe(&request.id, sub, *identity, connection_id, state, registry, poller, push_tx)
				.await
		}

		RequestPayload::Unsubscribe(unsub) => {
			let subscription_id = match unsub.subscription_id.parse::<u64>() {
				Ok(id) => DbSubscriptionId(id),
				Err(_) => {
					return Some(build_error(
						&request.id,
						"INVALID_SUBSCRIPTION_ID",
						"Invalid subscription ID format",
					));
				}
			};

			// Unregister from poller
			poller.unregister(&subscription_id);

			// Unsubscribe from registry
			let removed = registry.unsubscribe(subscription_id);

			if removed {
				// Cleanup the subscription from the database
				if let Err(e) = cleanup_subscription(state, subscription_id).await {
					warn!(
						"Failed to cleanup subscription {} from database: {:?}",
						subscription_id, e
					);
				}

				info!("Connection {} unsubscribed from {}", connection_id, subscription_id);
				Some(Response::unsubscribed(&request.id, subscription_id.to_string()).to_json())
			} else {
				Some(build_error(
					&request.id,
					"SUBSCRIPTION_NOT_FOUND",
					"Subscription not found or already unsubscribed",
				))
			}
		}
	}
}

/// Convert an ExecuteError to a JSON response string.
pub(crate) fn error_to_response(id: &str, e: ExecuteError) -> String {
	match e {
		ExecuteError::Timeout => {
			Response::internal_error(id, "QUERY_TIMEOUT", "Query execution timed out").to_json()
		}
		ExecuteError::Cancelled => {
			Response::internal_error(id, "QUERY_CANCELLED", "Query was cancelled").to_json()
		}
		ExecuteError::Disconnected => {
			error!("Query stream disconnected unexpectedly");
			Response::internal_error(id, "INTERNAL_ERROR", "Internal server error").to_json()
		}
		ExecuteError::Engine {
			diagnostic,
			statement,
		} => {
			// Create a copy of the diagnostic with the statement attached
			let mut diag = (*diagnostic).clone();
			if diag.statement.is_none() && !statement.is_empty() {
				diag.statement = Some(statement);
			}
			Response::error(id, diag).to_json()
		}
	}
}

/// Build an error response JSON string.
pub(crate) fn build_error(id: &str, code: &str, message: &str) -> String {
	Response::internal_error(id, code, message).to_json()
}

/// Build response body with content_type based on format parameter.
///
/// When `format` is `Some("json")`, resolves the body column to raw JSON.
/// Otherwise, converts frames to the standard frame format.
fn build_response_body(
	frames: Vec<reifydb_type::value::frame::frame::Frame>,
	format: Option<&str>,
	unwrap: bool,
) -> (String, serde_json::Value) {
	if format == Some("json") {
		match resolve_response_json(frames, unwrap) {
			Ok(resolved) => {
				// Parse the raw JSON string into a serde_json::Value to embed in the envelope
				let body = serde_json::from_str(&resolved.body)
					.unwrap_or(serde_json::Value::String(resolved.body));
				(CONTENT_TYPE_JSON.to_string(), body)
			}
			Err(e) => {
				// Return error as a JSON string in the body
				(CONTENT_TYPE_JSON.to_string(), serde_json::Value::String(e))
			}
		}
	} else {
		let ws_frames = convert_frames(frames);
		let body = serde_json::json!({ "frames": ws_frames });
		(CONTENT_TYPE_FRAMES.to_string(), body)
	}
}
