// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use reifydb_catalog::{delete_flow_by_name, delete_subscription};
use reifydb_core::{
	error::diagnostic::internal::internal,
	interface::{
		auth::Identity,
		catalog::{
			id::SubscriptionId as DbSubscriptionId,
			subscription::{subscription_flow_name, subscription_flow_namespace},
		},
	},
};
use reifydb_sub_server::{
	auth::extract_identity_from_ws_auth,
	execute::{ExecuteError, execute_command, execute_query},
	response::convert_frames,
	state::AppState,
};
use reifydb_type::{error::Error, params::Params, value::uuid::Uuid7};
use tokio::{
	net::TcpStream,
	select,
	sync::{mpsc, watch},
	time::timeout,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{debug, warn};

use crate::{
	protocol::{Request, RequestPayload},
	subscription::{
		handler::handle_subscribe,
		poller::SubscriptionPoller,
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
	let mut identity: Option<Identity> = None;

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
				use crate::response::ServerPush;

				let msg = match push {
					PushMessage::Change { subscription_id, frame } => {
						ServerPush::change(subscription_id.to_string(), frame).to_json()
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
		if let Err(e) = cleanup_subscription_from_db(&state, subscription_id).await {
			warn!("Failed to cleanup subscription {} from database: {:?}", subscription_id, e);
		}
	}

	debug!("WebSocket connection {} from {:?} cleaned up", connection_id, peer);
}

/// Cleanup a subscription from the database (synchronous).
fn cleanup_subscription_from_db_sync(
	engine: &reifydb_engine::engine::StandardEngine,
	subscription_id: DbSubscriptionId,
) -> reifydb_type::Result<()> {
	let mut txn = engine.begin_command()?;

	// Delete the associated flow (named after the subscription ID)
	let flow_name = subscription_flow_name(subscription_id);
	let namespace_id = subscription_flow_namespace();
	delete_flow_by_name(&mut txn, namespace_id, &flow_name)?;

	// Delete the subscription (metadata, columns, rows)
	delete_subscription(&mut txn, subscription_id)?;

	txn.commit()?;
	Ok(())
}

/// Cleanup a subscription from the database.
async fn cleanup_subscription_from_db(state: &AppState, subscription_id: DbSubscriptionId) -> reifydb_type::Result<()> {
	let engine = state.engine_clone();
	let system = state.actor_system();

	system.compute(move || cleanup_subscription_from_db_sync(&engine, subscription_id))
		.await
		.map_err(|e| Error(internal(format!("Compute pool error: {:?}", e))))?
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
	identity: &mut Option<Identity>,
	connection_id: ConnectionId,
	registry: &SubscriptionRegistry,
	poller: &SubscriptionPoller,
	push_tx: mpsc::Sender<PushMessage>,
) -> Option<String> {
	let request: Request = match serde_json::from_str(text) {
		Ok(r) => r,
		Err(e) => {
			return Some(build_error("0", "PARSE_ERROR", &format!("Invalid JSON: {}", e)));
		}
	};

	match request.payload {
		RequestPayload::Auth(auth) => {
			use crate::response::Response;

			match extract_identity_from_ws_auth(auth.token.as_deref()) {
				Ok(id) => {
					*identity = Some(id);
					Some(Response::auth(&request.id).to_json())
				}
				Err(e) => Some(build_error(&request.id, "AUTH_FAILED", &format!("{:?}", e))),
			}
		}

		RequestPayload::Query(q) => {
			use crate::response::Response;

			let id = match identity.as_ref() {
				Some(id) => id.clone(),
				None => {
					return Some(build_error(
						&request.id,
						"AUTH_REQUIRED",
						"Authentication required",
					));
				}
			};

			let params = q.params.unwrap_or(Params::None);
			let query = q.statements.join("; ");
			let timeout = state.query_timeout();

			match execute_query(state.actor_system(), state.engine_clone(), query, id, params, timeout).await {
				Ok(frames) => {
					let ws_frames = convert_frames(frames);
					Some(Response::query(&request.id, ws_frames).to_json())
				}
				Err(e) => Some(error_to_response(&request.id, e)),
			}
		}

		RequestPayload::Command(c) => {
			use crate::response::Response;

			let id = match identity.as_ref() {
				Some(id) => id.clone(),
				None => {
					return Some(build_error(
						&request.id,
						"AUTH_REQUIRED",
						"Authentication required",
					));
				}
			};

			let params = c.params.unwrap_or(Params::None);
			let timeout = state.query_timeout();

			match execute_command(state.actor_system(), state.engine_clone(), c.statements, id, params, timeout)
				.await
			{
				Ok(frames) => {
					let ws_frames = convert_frames(frames);
					Some(Response::command(&request.id, ws_frames).to_json())
				}
				Err(e) => Some(error_to_response(&request.id, e)),
			}
		}

		RequestPayload::Subscribe(sub) => {
			handle_subscribe(
				&request.id,
				sub,
				identity.clone(),
				connection_id,
				state,
				registry,
				poller,
				push_tx,
			)
			.await
		}

		RequestPayload::Unsubscribe(unsub) => {
			use crate::response::Response;

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
				if let Err(e) = cleanup_subscription_from_db(state, subscription_id).await {
					warn!(
						"Failed to cleanup subscription {} from database: {:?}",
						subscription_id, e
					);
				}

				tracing::info!("Connection {} unsubscribed from {}", connection_id, subscription_id);
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
	use crate::response::Response;

	match e {
		ExecuteError::Timeout => {
			Response::internal_error(id, "QUERY_TIMEOUT", "Query execution timed out").to_json()
		}
		ExecuteError::Cancelled => {
			Response::internal_error(id, "QUERY_CANCELLED", "Query was cancelled").to_json()
		}
		ExecuteError::Disconnected => {
			tracing::error!("Query stream disconnected unexpectedly");
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
	use crate::response::Response;
	Response::internal_error(id, code, message).to_json()
}
