// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use reifydb_core::{
	actors::server::{ServerAuthResponse, ServerLogoutResponse, ServerMessage},
	interface::catalog::id::SubscriptionId,
	value::frame::response::convert_frames,
};
use reifydb_runtime::actor::{mailbox::ActorRef, reply::reply_channel};
use reifydb_sub_server::{
	actor::ServerActor,
	auth::extract_identity_from_ws_auth,
	dispatch::dispatch,
	execute::ExecuteError,
	interceptor::{Operation, Protocol, RequestContext, RequestMetadata},
	response::resolve_response_json,
	state::AppState,
	subscribe::cleanup_subscription,
};
use reifydb_type::{
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId, uuid::Uuid7},
};
use serde_json::{Value as JsonValue, from_str, json};
use tokio::{
	net::TcpStream,
	select,
	sync::{mpsc, watch},
	task::JoinHandle,
	time::timeout,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{debug, error, info, warn};
use uuid::Builder;

use crate::{
	protocol::{Request, RequestPayload},
	response::{CONTENT_TYPE_FRAMES, CONTENT_TYPE_JSON, Response, ServerPush},
	subscription::{
		handler::handle_subscribe,
		registry::{PushMessage, SubscriptionRegistry},
	},
};

/// Handle a single WebSocket connection.
pub async fn handle_connection(
	stream: TcpStream,
	state: AppState,
	registry: Arc<SubscriptionRegistry>,
	mut shutdown: watch::Receiver<bool>,
) {
	let peer = stream.peer_addr().ok();
	let connection_id = {
		let millis = state.clock().now_millis();
		let random_bytes = state.rng().infra_bytes_10();
		Uuid7::from(Builder::from_unix_timestamp_millis(millis, &random_bytes).into_uuid())
	};

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

	// Spawn per-connection actor
	let actor = ServerActor::new(state.engine_clone(), state.auth_service().clone(), state.clock().clone());
	let actor_handle = state.actor_system().spawn(&format!("ws-{}", connection_id), actor);
	let actor_ref = actor_handle.actor_ref().clone();

	// Channel for receiving push messages from the registry
	let (push_tx, mut push_rx) = mpsc::unbounded_channel::<PushMessage>();

	// Connection starts with anonymous identity; Auth message upgrades it
	let mut identity: Option<IdentityId> = Some(IdentityId::anonymous());
	let mut auth_token: Option<String> = None;

	// Track remote subscription proxy tasks (not registered in registry/poller)
	let mut remote_tasks: HashMap<String, JoinHandle<()>> = HashMap::new();

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
				match push {
					PushMessage::Change { subscription_id, content_type, body } => {
						let msg = ServerPush::change(subscription_id.to_string(), content_type, body).to_json();
						if sender.send(Message::Text(msg.into())).await.is_err() {
							debug!("Failed to send push message to {:?}", peer);
							break;
						}
					}
					PushMessage::Closed { .. } => {
						debug!("Remote subscription closed for {:?}, closing connection", peer);
						let _ = sender.send(Message::Close(None)).await;
						break;
					}
				}
			}

			// Handle incoming messages
			msg = receiver.next() => {
				match msg {
					Some(Ok(Message::Text(text))) => {
						let response = process_message(
							&text,
							&mut ConnectionContext {
								state: &state,
								actor_ref: &actor_ref,
								identity: &mut identity,
								auth_token: &mut auth_token,
								connection_id,
								registry: &registry,
								push_tx: push_tx.clone(),
								remote_tasks: &mut remote_tasks,
								shutdown: shutdown.clone(),
							},
						).await;
						if let Some(resp) = response
							&& sender.send(Message::Text(resp.into())).await.is_err() {
								debug!("Failed to send response to {:?}", peer);
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

	// Drop actor handle — actor stops
	drop(actor_handle);

	// Abort all remote proxy tasks
	for (_, handle) in remote_tasks {
		handle.abort();
	}

	// Cleanup all subscriptions for this connection
	let subscription_ids = registry.cleanup_connection(connection_id);

	// Cleanup database subscriptions for each subscription
	for subscription_id in subscription_ids {
		if let Err(e) = cleanup_subscription(&state, subscription_id).await {
			warn!("Failed to cleanup subscription {} from database: {:?}", subscription_id, e);
		}
	}

	debug!("WebSocket connection {} from {:?} cleaned up", connection_id, peer);
}

/// Connection ID type alias for clarity.
type ConnectionId = Uuid7;

/// Groups the shared connection state passed to message processing and subscription handlers.
pub(crate) struct ConnectionContext<'a> {
	pub state: &'a AppState,
	pub actor_ref: &'a ActorRef<ServerMessage>,
	pub identity: &'a mut Option<IdentityId>,
	pub auth_token: &'a mut Option<String>,
	pub connection_id: ConnectionId,
	pub registry: &'a SubscriptionRegistry,
	pub push_tx: mpsc::UnboundedSender<PushMessage>,
	pub remote_tasks: &'a mut HashMap<String, JoinHandle<()>>,
	pub shutdown: watch::Receiver<bool>,
}

/// Process a single WebSocket message.
async fn process_message(text: &str, conn: &mut ConnectionContext<'_>) -> Option<String> {
	let request: Request = match from_str(text) {
		Ok(r) => r,
		Err(e) => {
			return Some(build_error("0", "PARSE_ERROR", &format!("Invalid JSON: {}", e)));
		}
	};

	match request.payload {
		RequestPayload::Auth(auth) => {
			if let Some(method) = auth.method.as_deref() {
				let credentials = auth.credentials.unwrap_or_default();

				// Dispatch auth through actor
				let (reply, receiver) = reply_channel();
				conn.actor_ref
					.send(ServerMessage::Authenticate {
						method: method.to_string(),
						credentials,
						reply,
					})
					.ok()
					.expect("actor mailbox closed");

				match receiver.recv().await {
					Ok(ServerAuthResponse::Authenticated {
						identity: id,
						token,
					}) => {
						*conn.identity = Some(id);
						*conn.auth_token = Some(token.clone());
						Some(Response::auth_authenticated(&request.id, token, id.to_string())
							.to_json())
					}
					Ok(ServerAuthResponse::Challenge {
						challenge_id,
						payload,
					}) => Some(Response::auth_challenge(&request.id, challenge_id, payload).to_json()),
					Ok(ServerAuthResponse::Failed {
						reason,
					}) => Some(build_error(&request.id, "AUTH_FAILED", &reason)),
					Ok(ServerAuthResponse::Error(reason)) => {
						Some(build_error(&request.id, "AUTH_ERROR", &reason))
					}
					Err(_) => Some(build_error(&request.id, "INTERNAL_ERROR", "actor stopped")),
				}
			} else {
				// Token validation flow (existing behavior — stays outside actor)
				match extract_identity_from_ws_auth(conn.state.auth_service(), auth.token.as_deref()) {
					Ok(id) => {
						*conn.identity = Some(id);
						*conn.auth_token = auth.token;
						Some(Response::auth(&request.id).to_json())
					}
					Err(e) => {
						*conn.identity = None;
						Some(build_error(&request.id, "AUTH_FAILED", &format!("{:?}", e)))
					}
				}
			}
		}

		RequestPayload::Admin(_) if !conn.state.admin_enabled() => {
			Some(build_error(&request.id, "NOT_FOUND", "Unknown request type"))
		}

		RequestPayload::Admin(a) => {
			let id: IdentityId = match conn.identity.as_ref() {
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

			execute_via_dispatch(
				conn,
				&request.id,
				id,
				Operation::Admin,
				a.statements,
				params,
				format.as_deref(),
				unwrap,
				|id, content_type, body| Response::admin(id, content_type, body).to_json(),
			)
			.await
		}

		RequestPayload::Query(q) => {
			let id: IdentityId = match conn.identity.as_ref() {
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

			execute_via_dispatch(
				conn,
				&request.id,
				id,
				Operation::Query,
				q.statements,
				params,
				format.as_deref(),
				unwrap,
				|id, content_type, body| Response::query(id, content_type, body).to_json(),
			)
			.await
		}

		RequestPayload::Command(c) => {
			let id: IdentityId = match conn.identity.as_ref() {
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

			execute_via_dispatch(
				conn,
				&request.id,
				id,
				Operation::Command,
				c.statements,
				params,
				format.as_deref(),
				unwrap,
				|id, content_type, body| Response::command(id, content_type, body).to_json(),
			)
			.await
		}

		RequestPayload::Subscribe(sub) => handle_subscribe(&request.id, sub, conn).await,

		RequestPayload::Logout => {
			if let Some(token) = conn.auth_token.as_ref() {
				let (reply, receiver) = reply_channel();
				conn.actor_ref
					.send(ServerMessage::Logout {
						token: token.clone(),
						reply,
					})
					.ok()
					.expect("actor mailbox closed");

				match receiver.recv().await {
					Ok(ServerLogoutResponse::Ok) => {
						*conn.identity = Some(IdentityId::anonymous());
						*conn.auth_token = None;
						Some(Response::logout(&request.id).to_json())
					}
					Ok(ServerLogoutResponse::InvalidToken) => Some(build_error(
						&request.id,
						"LOGOUT_FAILED",
						"Token revocation failed",
					)),
					Ok(ServerLogoutResponse::Error(reason)) => {
						Some(build_error(&request.id, "LOGOUT_FAILED", &reason))
					}
					Err(_) => Some(build_error(&request.id, "INTERNAL_ERROR", "actor stopped")),
				}
			} else {
				Some(build_error(&request.id, "AUTH_REQUIRED", "No active session to logout"))
			}
		}

		RequestPayload::Unsubscribe(unsub) => {
			// Check remote tasks first (not registered in registry/poller)
			if let Some(handle) = conn.remote_tasks.remove(&unsub.subscription_id) {
				handle.abort();
				info!(
					"Connection {} unsubscribed from remote {}",
					conn.connection_id, unsub.subscription_id
				);
				return Some(Response::unsubscribed(&request.id, unsub.subscription_id).to_json());
			}

			let subscription_id = match unsub.subscription_id.parse::<u64>() {
				Ok(id) => SubscriptionId(id),
				Err(_) => {
					return Some(build_error(
						&request.id,
						"INVALID_SUBSCRIPTION_ID",
						"Invalid subscription ID format",
					));
				}
			};

			// Unsubscribe from registry
			let removed = conn.registry.unsubscribe(subscription_id);

			if removed {
				if let Err(e) = cleanup_subscription(conn.state, subscription_id).await {
					warn!(
						"Failed to cleanup subscription {} from database: {:?}",
						subscription_id, e
					);
				}

				info!("Connection {} unsubscribed from {}", conn.connection_id, subscription_id);
				Some(Response::unsubscribed(&request.id, subscription_id.to_string()).to_json())
			} else {
				info!(
					"Connection {} unsubscribe for {} (already removed)",
					conn.connection_id, subscription_id
				);
				Some(Response::unsubscribed(&request.id, subscription_id.to_string()).to_json())
			}
		}
	}
}

/// Execute a query/command/admin via the shared dispatch layer with response formatting.
#[allow(clippy::too_many_arguments)]
async fn execute_via_dispatch(
	conn: &mut ConnectionContext<'_>,
	request_id: &str,
	identity: IdentityId,
	operation: Operation,
	statements: Vec<String>,
	params: Params,
	format: Option<&str>,
	unwrap: bool,
	build_response: impl FnOnce(&str, String, JsonValue) -> String,
) -> Option<String> {
	let metadata = build_ws_metadata(conn.auth_token);
	let ctx = RequestContext {
		identity,
		operation,
		statements,
		params,
		metadata,
	};

	match dispatch(conn.state, ctx).await {
		Ok((frames, _duration)) => {
			let (content_type, body) = build_response_body(frames, format, unwrap);
			Some(build_response(request_id, content_type, body))
		}
		Err(e) => Some(error_to_response(request_id, e)),
	}
}

/// Build `RequestMetadata` for a WebSocket request, injecting the stored auth token if present.
fn build_ws_metadata(auth_token: &Option<String>) -> RequestMetadata {
	let mut metadata = RequestMetadata::new(Protocol::WebSocket);
	if let Some(token) = auth_token {
		metadata.insert("authorization", format!("Bearer {}", token));
	}
	metadata
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
		ExecuteError::Rejected {
			code,
			message,
		} => Response::rejected(id, &code, &message).to_json(),
		ExecuteError::Engine {
			diagnostic,
			statement,
		} => {
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
fn build_response_body(frames: Vec<Frame>, format: Option<&str>, unwrap: bool) -> (String, JsonValue) {
	if format == Some("json") {
		match resolve_response_json(frames, unwrap) {
			Ok(resolved) => {
				let body = from_str(&resolved.body).unwrap_or(JsonValue::String(resolved.body));
				(CONTENT_TYPE_JSON.to_string(), body)
			}
			Err(e) => (CONTENT_TYPE_JSON.to_string(), JsonValue::String(e)),
		}
	} else {
		let ws_frames = convert_frames(&frames);
		let body = json!({ "frames": ws_frames });
		(CONTENT_TYPE_FRAMES.to_string(), body)
	}
}
