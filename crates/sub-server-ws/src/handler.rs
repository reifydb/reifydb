// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use reifydb_core::{
	actors::server::{Operation, ServerAuthResponse, ServerLogoutResponse, ServerMessage},
	interface::catalog::{binding::BindingFormat, id::SubscriptionId},
};
use reifydb_runtime::actor::{mailbox::ActorRef, reply::reply_channel, system::ActorHandle};
use reifydb_sub_server::{
	actor::ServerActor,
	auth::extract_identity_from_ws_auth,
	binding::dispatch_binding,
	dispatch::dispatch,
	execute::ExecuteError,
	format::WireFormat,
	interceptor::{Protocol, RequestContext, RequestMetadata},
	response::{CONTENT_TYPE_FRAMES, CONTENT_TYPE_JSON, encode_frames_rbcf, resolve_response_json},
	state::AppState,
	subscribe::cleanup_subscription,
};
use reifydb_subscription::batch::BatchId;
use reifydb_type::{
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId, uuid::Uuid7},
};
use reifydb_wire_format::json::to::convert_frames;
use serde_json::{Value as JsonValue, from_str, json, to_string as json_to_string};
use tokio::{
	net::TcpStream,
	select,
	sync::{mpsc, watch},
	task::JoinHandle,
	time::timeout,
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
use tracing::{debug, error, info, warn};
use uuid::Builder;

use crate::{
	protocol::{CallRequest, Request, RequestPayload},
	response::{BatchChangeEntry, Response, ResponseMeta, ServerPush},
	subscription::{
		handler::{handle_batch_subscribe, handle_batch_unsubscribe, handle_subscribe},
		registry::{PushMessage, SubscriptionRegistry},
	},
};

enum WsResponse {
	Text(String),
	Binary(Vec<u8>),
}

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum BinaryKind {
	Response = 0x00,
	Change = 0x01,
	BatchChange = 0x02,
}

pub(crate) fn encode_rbcf_envelope(
	kind: BinaryKind,
	id: &str,
	rbcf_bytes: &[u8],
	meta: Option<&ResponseMeta>,
) -> Vec<u8> {
	let id_bytes = id.as_bytes();
	let meta_json = meta.map(|m| json_to_string(m).unwrap()).unwrap_or_default();
	let meta_bytes = meta_json.as_bytes();

	let mut envelope = Vec::with_capacity(1 + 4 + id_bytes.len() + 4 + meta_bytes.len() + rbcf_bytes.len());
	envelope.push(kind as u8);
	envelope.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
	envelope.extend_from_slice(id_bytes);
	envelope.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
	envelope.extend_from_slice(meta_bytes);
	envelope.extend_from_slice(rbcf_bytes);
	envelope
}

pub async fn handle_connection(
	stream: TcpStream,
	state: AppState,
	registry: Arc<SubscriptionRegistry>,
	mut shutdown: watch::Receiver<bool>,
) {
	let peer = stream.peer_addr().ok();
	let connection_id = generate_connection_id(&state);
	configure_stream(&stream, peer);

	let Some(ws_stream) = accept_ws_with_timeout(stream).await else {
		return;
	};

	debug!("WebSocket connection {} established from {:?}", connection_id, peer);
	let (mut sender, mut receiver) = ws_stream.split();

	let (actor_handle, actor_ref) = spawn_connection_actor(&state, connection_id);

	let (push_tx, mut push_rx) = mpsc::unbounded_channel::<PushMessage>();

	let mut identity: Option<IdentityId> = Some(IdentityId::anonymous());
	let mut auth_token: Option<String> = None;

	let mut remote_tasks: HashMap<String, JoinHandle<()>> = HashMap::new();

	let mut batch_remote_tasks: HashMap<BatchId, Vec<JoinHandle<()>>> = HashMap::new();

	loop {
		select! {
			biased;


			result = shutdown.changed() => {
				if result.is_err() || *shutdown.borrow() {
					debug!("WebSocket connection {:?} shutting down", peer);
					let _ = sender.send(Message::Close(None)).await;
					break;
				}
			}


			Some(push) = push_rx.recv() => {
				match push {
					PushMessage::ChangeJson { subscription_id, content_type, body } => {
						let msg = ServerPush::change(subscription_id.to_string(), content_type, body).to_json();
						if sender.send(Message::Text(msg.into())).await.is_err() {
							debug!("Failed to send push message to {:?}", peer);
							break;
						}
					}
					PushMessage::ChangeRbcf { envelope, .. } => {
						if sender.send(Message::Binary(envelope.into())).await.is_err() {
							debug!("Failed to send RBCF push message to {:?}", peer);
							break;
						}
					}
					PushMessage::Closed { .. } => {
						debug!("Remote subscription closed for {:?}, closing connection", peer);
						let _ = sender.send(Message::Close(None)).await;
						break;
					}
					PushMessage::BatchChangeJson { batch_id, entries } => {
						let wire_entries = entries.into_iter().map(|e| BatchChangeEntry {
							subscription_id: e.subscription_id.to_string(),
							content_type: e.content_type,
							body: e.body,
						}).collect();
						let msg = ServerPush::batch_change(batch_id.to_string(), wire_entries).to_json();
						if sender.send(Message::Text(msg.into())).await.is_err() {
							debug!("Failed to send batch push message to {:?}", peer);
							break;
						}
					}
					PushMessage::BatchChangeRbcf { envelope, .. } => {
						if sender.send(Message::Binary(envelope.into())).await.is_err() {
							debug!("Failed to send RBCF batch push to {:?}", peer);
							break;
						}
					}
					PushMessage::BatchMemberClosed { batch_id, subscription_id } => {
						let msg = ServerPush::batch_member_closed(batch_id.to_string(), subscription_id.to_string()).to_json();
						if sender.send(Message::Text(msg.into())).await.is_err() {
							debug!("Failed to send BatchMemberClosed to {:?}", peer);
							break;
						}
					}
					PushMessage::BatchClosed { batch_id } => {
						let msg = ServerPush::batch_closed(batch_id.to_string()).to_json();
						if sender.send(Message::Text(msg.into())).await.is_err() {
							debug!("Failed to send BatchClosed to {:?}", peer);
							break;
						}
					}
				}
			}


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
								batch_remote_tasks: &mut batch_remote_tasks,
								shutdown: shutdown.clone(),
							},
						).await;
						if let Some(resp) = response {
							let msg = match resp {
								WsResponse::Text(text) => Message::Text(text.into()),
								WsResponse::Binary(data) => Message::Binary(data.into()),
							};
							if sender.send(msg).await.is_err() {
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

	drop(actor_handle);
	abort_remote_tasks(remote_tasks, batch_remote_tasks);
	cleanup_connection_subscriptions(&state, &registry, connection_id).await;

	debug!("WebSocket connection {} from {:?} cleaned up", connection_id, peer);
}

#[inline]
fn generate_connection_id(state: &AppState) -> Uuid7 {
	let millis = state.clock().now_millis();
	let random_bytes = state.rng().infra_bytes_10();
	Uuid7::from(Builder::from_unix_timestamp_millis(millis, &random_bytes).into_uuid())
}

#[inline]
fn configure_stream(stream: &TcpStream, peer: Option<SocketAddr>) {
	if let Err(e) = stream.set_nodelay(true) {
		warn!("Failed to set TCP_NODELAY for {:?}: {}", peer, e);
	}
}

#[inline]
async fn accept_ws_with_timeout(stream: TcpStream) -> Option<WebSocketStream<TcpStream>> {
	match timeout(Duration::from_secs(30), accept_async(stream)).await {
		Ok(Ok(ws)) => Some(ws),
		_ => None,
	}
}

#[inline]
fn spawn_connection_actor(
	state: &AppState,
	connection_id: Uuid7,
) -> (ActorHandle<ServerMessage>, ActorRef<ServerMessage>) {
	let actor = ServerActor::new(state.engine_clone(), state.auth_service().clone(), state.clock().clone());
	let actor_handle = state.actor_system().spawn_query(&format!("ws-{}", connection_id), actor);
	let actor_ref = actor_handle.actor_ref().clone();
	(actor_handle, actor_ref)
}

#[inline]
fn abort_remote_tasks(
	remote_tasks: HashMap<String, JoinHandle<()>>,
	batch_remote_tasks: HashMap<BatchId, Vec<JoinHandle<()>>>,
) {
	for (_, handle) in remote_tasks {
		handle.abort();
	}
	for (_, handles) in batch_remote_tasks {
		for handle in handles {
			handle.abort();
		}
	}
}

#[inline]
async fn cleanup_connection_subscriptions(
	state: &AppState,
	registry: &Arc<SubscriptionRegistry>,
	connection_id: Uuid7,
) {
	let subscription_ids = registry.cleanup_connection(connection_id);
	for subscription_id in subscription_ids {
		if let Err(e) = cleanup_subscription(state, subscription_id).await {
			warn!("Failed to cleanup subscription {} from database: {:?}", subscription_id, e);
		}
	}
}

type ConnectionId = Uuid7;

pub(crate) struct ConnectionContext<'a> {
	pub state: &'a AppState,
	pub actor_ref: &'a ActorRef<ServerMessage>,
	pub identity: &'a mut Option<IdentityId>,
	pub auth_token: &'a mut Option<String>,
	pub connection_id: ConnectionId,
	pub registry: &'a Arc<SubscriptionRegistry>,
	pub push_tx: mpsc::UnboundedSender<PushMessage>,
	pub remote_tasks: &'a mut HashMap<String, JoinHandle<()>>,
	pub batch_remote_tasks: &'a mut HashMap<BatchId, Vec<JoinHandle<()>>>,
	pub shutdown: watch::Receiver<bool>,
}

async fn process_message(text: &str, conn: &mut ConnectionContext<'_>) -> Option<WsResponse> {
	let request: Request = match from_str(text) {
		Ok(r) => r,
		Err(e) => {
			return Some(WsResponse::Text(build_error(
				"0",
				"PARSE_ERROR",
				&format!("Invalid JSON: {}", e),
			)));
		}
	};

	match request.payload {
		RequestPayload::Auth(auth) => {
			if let Some(method) = auth.method.as_deref() {
				let credentials = auth.credentials.unwrap_or_default();

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
						Some(WsResponse::Text(
							Response::auth_authenticated(
								&request.id,
								token,
								id.to_string(),
							)
							.to_json(),
						))
					}
					Ok(ServerAuthResponse::Challenge {
						challenge_id,
						payload,
					}) => Some(WsResponse::Text(
						Response::auth_challenge(&request.id, challenge_id, payload).to_json(),
					)),
					Ok(ServerAuthResponse::Failed {
						reason,
					}) => Some(WsResponse::Text(build_error(&request.id, "AUTH_FAILED", &reason))),
					Ok(ServerAuthResponse::Error(reason)) => {
						Some(WsResponse::Text(build_error(&request.id, "AUTH_ERROR", &reason)))
					}
					Err(_) => Some(WsResponse::Text(build_error(
						&request.id,
						"INTERNAL_ERROR",
						"actor stopped",
					))),
				}
			} else {
				match extract_identity_from_ws_auth(conn.state.auth_service(), auth.token.as_deref()) {
					Ok(id) => {
						*conn.identity = Some(id);
						*conn.auth_token = auth.token;
						Some(WsResponse::Text(Response::auth(&request.id).to_json()))
					}
					Err(e) => {
						*conn.identity = None;
						Some(WsResponse::Text(build_error(
							&request.id,
							"AUTH_FAILED",
							&format!("{:?}", e),
						)))
					}
				}
			}
		}

		RequestPayload::Admin(_) if !conn.state.admin_enabled() => {
			Some(WsResponse::Text(build_error(&request.id, "NOT_FOUND", "Unknown request type")))
		}

		RequestPayload::Admin(a) => {
			let id: IdentityId = match conn.identity.as_ref() {
				Some(id) => *id,
				None => {
					return Some(WsResponse::Text(build_error(
						&request.id,
						"AUTH_REQUIRED",
						"Authentication required",
					)));
				}
			};

			let format = a.format;
			let unwrap = a.unwrap.unwrap_or(false);
			let params = match a.params {
				None => Params::None,
				Some(wp) => match wp.into_params() {
					Ok(p) => p,
					Err(e) => {
						return Some(WsResponse::Text(build_error(
							&request.id,
							"INVALID_PARAMS",
							&e,
						)));
					}
				},
			};

			execute_via_dispatch(
				conn,
				&request.id,
				id,
				Operation::Admin,
				a.rql,
				params,
				format,
				unwrap,
				|id, content_type, body, meta| Response::admin(id, content_type, body, meta).to_json(),
			)
			.await
		}

		RequestPayload::Query(q) => {
			let id: IdentityId = match conn.identity.as_ref() {
				Some(id) => *id,
				None => {
					return Some(WsResponse::Text(build_error(
						&request.id,
						"AUTH_REQUIRED",
						"Authentication required",
					)));
				}
			};

			let format = q.format;
			let unwrap = q.unwrap.unwrap_or(false);
			let params = match q.params {
				None => Params::None,
				Some(wp) => match wp.into_params() {
					Ok(p) => p,
					Err(e) => {
						return Some(WsResponse::Text(build_error(
							&request.id,
							"INVALID_PARAMS",
							&e,
						)));
					}
				},
			};

			execute_via_dispatch(
				conn,
				&request.id,
				id,
				Operation::Query,
				q.rql,
				params,
				format,
				unwrap,
				|id, content_type, body, meta| Response::query(id, content_type, body, meta).to_json(),
			)
			.await
		}

		RequestPayload::Command(c) => {
			let id: IdentityId = match conn.identity.as_ref() {
				Some(id) => *id,
				None => {
					return Some(WsResponse::Text(build_error(
						&request.id,
						"AUTH_REQUIRED",
						"Authentication required",
					)));
				}
			};

			let format = c.format;
			let unwrap = c.unwrap.unwrap_or(false);
			let params = match c.params {
				None => Params::None,
				Some(wp) => match wp.into_params() {
					Ok(p) => p,
					Err(e) => {
						return Some(WsResponse::Text(build_error(
							&request.id,
							"INVALID_PARAMS",
							&e,
						)));
					}
				},
			};

			execute_via_dispatch(
				conn,
				&request.id,
				id,
				Operation::Command,
				c.rql,
				params,
				format,
				unwrap,
				|id, content_type, body, meta| {
					Response::command(id, content_type, body, meta).to_json()
				},
			)
			.await
		}

		RequestPayload::Call(co) => {
			let identity: IdentityId = conn.identity.unwrap_or(IdentityId::anonymous());
			match handle_call(&request.id, identity, co, conn).await {
				Ok(resp) => Some(resp),
				Err(msg) => Some(WsResponse::Text(msg)),
			}
		}

		RequestPayload::Subscribe(sub) => handle_subscribe(&request.id, sub, conn).await.map(WsResponse::Text),

		RequestPayload::BatchSubscribe(req) => {
			handle_batch_subscribe(&request.id, req, conn).await.map(WsResponse::Text)
		}

		RequestPayload::BatchUnsubscribe(req) => {
			handle_batch_unsubscribe(&request.id, req, conn).await.map(WsResponse::Text)
		}

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
						Some(WsResponse::Text(Response::logout(&request.id).to_json()))
					}
					Ok(ServerLogoutResponse::InvalidToken) => Some(WsResponse::Text(build_error(
						&request.id,
						"LOGOUT_FAILED",
						"Token revocation failed",
					))),
					Ok(ServerLogoutResponse::Error(reason)) => Some(WsResponse::Text(build_error(
						&request.id,
						"LOGOUT_FAILED",
						&reason,
					))),
					Err(_) => Some(WsResponse::Text(build_error(
						&request.id,
						"INTERNAL_ERROR",
						"actor stopped",
					))),
				}
			} else {
				Some(WsResponse::Text(build_error(
					&request.id,
					"AUTH_REQUIRED",
					"No active session to logout",
				)))
			}
		}

		RequestPayload::Unsubscribe(unsub) => {
			if let Some(handle) = conn.remote_tasks.remove(&unsub.subscription_id) {
				handle.abort();
				info!(
					"Connection {} unsubscribed from remote {}",
					conn.connection_id, unsub.subscription_id
				);
				return Some(WsResponse::Text(
					Response::unsubscribed(&request.id, unsub.subscription_id).to_json(),
				));
			}

			let subscription_id = match unsub.subscription_id.parse::<u64>() {
				Ok(id) => SubscriptionId(id),
				Err(_) => {
					return Some(WsResponse::Text(build_error(
						&request.id,
						"INVALID_SUBSCRIPTION_ID",
						"Invalid subscription ID format",
					)));
				}
			};

			let removed = conn.registry.unsubscribe(subscription_id);

			if removed {
				if let Err(e) = cleanup_subscription(conn.state, subscription_id).await {
					warn!(
						"Failed to cleanup subscription {} from database: {:?}",
						subscription_id, e
					);
				}

				info!("Connection {} unsubscribed from {}", conn.connection_id, subscription_id);
				Some(WsResponse::Text(
					Response::unsubscribed(&request.id, subscription_id.to_string()).to_json(),
				))
			} else {
				info!(
					"Connection {} unsubscribe for {} (already removed)",
					conn.connection_id, subscription_id
				);
				Some(WsResponse::Text(
					Response::unsubscribed(&request.id, subscription_id.to_string()).to_json(),
				))
			}
		}
	}
}

#[allow(clippy::too_many_arguments)]
async fn execute_via_dispatch(
	conn: &mut ConnectionContext<'_>,
	request_id: &str,
	identity: IdentityId,
	operation: Operation,
	rql: String,
	params: Params,
	format: WireFormat,
	unwrap: bool,
	build_response: impl FnOnce(&str, String, JsonValue, ResponseMeta) -> String,
) -> Option<WsResponse> {
	let metadata = build_ws_metadata(conn.auth_token);
	let ctx = RequestContext {
		identity,
		operation,
		rql,
		params,
		metadata,
	};

	match dispatch(conn.state, ctx).await {
		Ok((frames, metrics)) => {
			let meta = ResponseMeta {
				fingerprint: metrics.fingerprint.to_hex(),
				duration: metrics.total.to_string(),
			};

			match format {
				WireFormat::Rbcf => match encode_frames_rbcf(&frames) {
					Ok(rbcf_bytes) => Some(WsResponse::Binary(encode_rbcf_envelope(
						BinaryKind::Response,
						request_id,
						&rbcf_bytes,
						Some(&meta),
					))),
					Err(e) => Some(WsResponse::Text(build_error(
						request_id,
						"ENCODE_ERROR",
						&format!("RBCF encode error: {}", e),
					))),
				},
				WireFormat::Json | WireFormat::Frames => {
					let (content_type, body) = build_response_body(frames, format, unwrap);
					Some(WsResponse::Text(build_response(request_id, content_type, body, meta)))
				}
			}
		}
		Err(e) => Some(WsResponse::Text(error_to_response(request_id, e))),
	}
}

fn build_ws_metadata(auth_token: &Option<String>) -> RequestMetadata {
	let mut metadata = RequestMetadata::new(Protocol::WebSocket);
	if let Some(token) = auth_token {
		metadata.insert("authorization", format!("Bearer {}", token));
	}
	metadata
}

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
			rql,
		} => {
			let mut diag = (*diagnostic).clone();
			if diag.rql.is_none() && !rql.is_empty() {
				diag.rql = Some(rql);
			}
			Response::error(id, diag).to_json()
		}
	}
}

pub(crate) fn build_error(id: &str, code: &str, message: &str) -> String {
	Response::internal_error(id, code, message).to_json()
}

async fn handle_call(
	request_id: &str,
	identity: IdentityId,
	req: CallRequest,
	conn: &mut ConnectionContext<'_>,
) -> Result<WsResponse, String> {
	let binding =
		conn.state.engine().catalog().cache().find_ws_binding_by_name(&req.name).ok_or_else(|| {
			build_error(request_id, "NOT_FOUND", &format!("no WS binding named `{}`", req.name))
		})?;

	let procedure =
		conn.state.engine().catalog().cache().find_procedure(binding.procedure_id).ok_or_else(|| {
			build_error(request_id, "INTERNAL_ERROR", "binding references missing procedure")
		})?;
	let namespace =
		conn.state.engine().catalog().cache().find_namespace(binding.namespace).ok_or_else(|| {
			build_error(request_id, "INTERNAL_ERROR", "binding references missing namespace")
		})?;

	let params = match req.params {
		None => Params::None,
		Some(wp) => wp.into_params().map_err(|e| build_error(request_id, "INVALID_PARAMS", &e))?,
	};
	match &params {
		Params::None => {
			if let Some(p) = procedure.params().first() {
				return Err(build_error(
					request_id,
					"INVALID_PARAMS",
					&format!("missing required parameter `{}`", p.name),
				));
			}
		}
		Params::Named(map) => {
			for k in map.keys() {
				if !procedure.params().iter().any(|p| &p.name == k) {
					return Err(build_error(
						request_id,
						"INVALID_PARAMS",
						&format!("unknown parameter `{}`", k),
					));
				}
			}
			for p in procedure.params() {
				if !map.contains_key(&p.name) {
					return Err(build_error(
						request_id,
						"INVALID_PARAMS",
						&format!("missing required parameter `{}`", p.name),
					));
				}
			}
		}
		Params::Positional(_) => {
			return Err(build_error(request_id, "INVALID_PARAMS", "Call requires named params"));
		}
	}

	let metadata = build_ws_metadata(conn.auth_token);
	let (frames, metrics) =
		dispatch_binding(conn.state, namespace.name(), procedure.name(), params, identity, metadata)
			.await
			.map_err(|e| error_to_response(request_id, e))?;

	let meta = ResponseMeta {
		fingerprint: metrics.fingerprint.to_hex(),
		duration: metrics.total.to_string(),
	};

	match binding.format {
		BindingFormat::Rbcf => match encode_frames_rbcf(&frames) {
			Ok(rbcf) => Ok(WsResponse::Binary(encode_rbcf_envelope(
				BinaryKind::Response,
				request_id,
				&rbcf,
				Some(&meta),
			))),
			Err(e) => Err(build_error(request_id, "ENCODE_ERROR", &format!("RBCF encode error: {}", e))),
		},
		BindingFormat::Json => {
			let (content_type, body) = build_response_body(frames, WireFormat::Json, false);
			Ok(WsResponse::Text(Response::call(request_id, content_type, body, meta).to_json()))
		}
		BindingFormat::Frames => {
			let (content_type, body) = build_response_body(frames, WireFormat::Frames, false);
			Ok(WsResponse::Text(Response::call(request_id, content_type, body, meta).to_json()))
		}
	}
}

fn build_response_body(frames: Vec<Frame>, format: WireFormat, unwrap: bool) -> (String, JsonValue) {
	match format {
		WireFormat::Json => match resolve_response_json(frames, unwrap) {
			Ok(resolved) => {
				let body = from_str(&resolved.body).unwrap_or(JsonValue::String(resolved.body));
				(CONTENT_TYPE_JSON.to_string(), body)
			}
			Err(e) => (CONTENT_TYPE_JSON.to_string(), JsonValue::String(e)),
		},
		WireFormat::Frames => {
			let ws_frames = convert_frames(&frames);
			let body = json!({ "frames": ws_frames });
			(CONTENT_TYPE_FRAMES.to_string(), body)
		}
		WireFormat::Rbcf => unreachable!("Rbcf is handled before build_response_body"),
	}
}
