// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
use std::{collections::HashMap, sync::Arc};

use futures_util::{
	SinkExt, StreamExt,
	stream::{SplitSink, SplitStream},
};
use reifydb_type::{
	error::{Diagnostic, Error},
	params::Params,
	value::frame::frame::Frame,
};
use reifydb_wire_format::decode::decode_frames;
use serde_json::{Value, from_str, to_string};
use tokio::{
	net::TcpStream,
	select, spawn,
	sync::{Mutex, mpsc, oneshot},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config, tungstenite::Message};

use crate::{
	AdminRequest, AdminResult, AuthRequest, BatchChangeEntry, BatchChangePayload, BatchClosedPayload,
	BatchMemberClosedPayload, BatchMemberInfo, BatchSubscribeRequest, BatchUnsubscribeRequest, CallRequest,
	ChangePayload, CommandRequest, CommandResult, LoginResult, QueryRequest, QueryResult, Request, RequestPayload,
	Response, ResponseMeta, ResponsePayload, ServerPush, SubscribeRequest, UnsubscribeRequest, WireFormat,
	params_to_wire,
	session::{parse_admin_response, parse_call_response, parse_command_response, parse_query_response},
	utils::generate_request_id,
};

/// Internal response type that can carry either a JSON Response or decoded RBCF frames.
enum ClientResponse {
	Json(Box<Response>),
	Frames(Vec<Frame>, Option<ResponseMeta>),
}

type PendingRequests = Arc<Mutex<HashMap<String, oneshot::Sender<ClientResponse>>>>;

/// A server-initiated push event delivered out of band from request/response.
#[derive(Debug, Clone)]
pub enum BatchPushEvent {
	Change(BatchChangePayload),
	MemberClosed(BatchMemberClosedPayload),
	Closed(BatchClosedPayload),
}

/// Dispatcher for routing batch push messages to per-batch subscription handles.
type BatchRouters = Arc<Mutex<HashMap<String, mpsc::Sender<BatchPushEvent>>>>;

/// Async WebSocket client for ReifyDB
pub struct WsClient {
	request_tx: mpsc::Sender<(Request, oneshot::Sender<ClientResponse>)>,
	shutdown_tx: Option<mpsc::Sender<()>>,
	is_authenticated: bool,
	/// Channel for receiving server-initiated Change messages.
	change_rx: mpsc::Receiver<ChangePayload>,
	batch_routers: BatchRouters,
	format: WireFormat,
}

impl WsClient {
	/// Create a new WebSocket client connected to the given URL.
	///
	/// # Arguments
	/// * `url` - WebSocket URL of the ReifyDB server (e.g., "ws://localhost:8090")
	/// * `format` - Wire format for responses
	pub async fn connect(url: &str, format: WireFormat) -> Result<Self, Error> {
		if format == WireFormat::Proto {
			return Err(Error(Box::new(Diagnostic {
				code: "INVALID_FORMAT".to_string(),
				message: "WireFormat::Proto is not supported for WsClient".to_string(),
				..Default::default()
			})));
		}

		let url = if !url.starts_with("ws://") && !url.starts_with("wss://") {
			format!("ws://{}", url)
		} else {
			url.to_string()
		};

		let (ws_stream, _) = connect_async_with_config(&url, None, true).await.unwrap(); // FIXME better error handling

		let (write, read) = ws_stream.split();

		// Channel for sending requests
		let (request_tx, request_rx) = mpsc::channel::<(Request, oneshot::Sender<ClientResponse>)>(32);

		// Channel for shutdown signal
		let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);

		// Channel for receiving server-initiated Change messages
		let (change_tx, change_rx) = mpsc::channel::<ChangePayload>(100);

		// Pending requests map
		let pending: PendingRequests = Arc::new(Mutex::new(HashMap::new()));

		// Dispatcher for routing batch push messages to per-batch subscription handles
		let batch_routers: BatchRouters = Arc::new(Mutex::new(HashMap::new()));

		// Spawn the connection management task
		let pending_clone = pending.clone();
		let batch_routers_clone = batch_routers.clone();
		spawn(async move {
			Self::connection_loop(
				write,
				read,
				request_rx,
				shutdown_rx,
				pending_clone,
				change_tx,
				batch_routers_clone,
			)
			.await;
		});

		Ok(Self {
			request_tx,
			shutdown_tx: Some(shutdown_tx),
			is_authenticated: false,
			change_rx,
			batch_routers,
			format,
		})
	}

	/// Connection management loop
	async fn connection_loop(
		mut write: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
		mut read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
		mut request_rx: mpsc::Receiver<(Request, oneshot::Sender<ClientResponse>)>,
		mut shutdown_rx: mpsc::Receiver<()>,
		pending: PendingRequests,
		change_tx: mpsc::Sender<ChangePayload>,
		batch_routers: BatchRouters,
	) {
		loop {
			select! {
				// Handle incoming messages
				Some(msg) = read.next() => {
					match msg {
						Ok(Message::Text(text)) => {
							// First try to parse as Response (has id field)
							if let Ok(response) = from_str::<Response>(&text) {
								let mut pending_guard = pending.lock().await;
								if let Some(tx) = pending_guard.remove(&response.id) {
									let _ = tx.send(ClientResponse::Json(Box::new(response)));
								}
							}
							// Then try to parse as ServerPush (no id field)
							else if let Ok(push) = from_str::<ServerPush>(&text) {
								match push {
									ServerPush::Change(change) => {
										let _ = change_tx.send(change).await;
									}
									ServerPush::BatchChange(batch) => {
										let sender = {
											let routers = batch_routers.lock().await;
											routers.get(&batch.batch_id).cloned()
										};
										if let Some(tx) = sender {
											let _ = tx.send(BatchPushEvent::Change(batch)).await;
										}
									}
									ServerPush::BatchMemberClosed(m) => {
										let sender = {
											let routers = batch_routers.lock().await;
											routers.get(&m.batch_id).cloned()
										};
										if let Some(tx) = sender {
											let _ = tx.send(BatchPushEvent::MemberClosed(m)).await;
										}
									}
									ServerPush::BatchClosed(c) => {
										let batch_id = c.batch_id.clone();
										let sender = {
											let mut routers = batch_routers.lock().await;
											routers.remove(&batch_id)
										};
										if let Some(tx) = sender {
											let _ = tx.send(BatchPushEvent::Closed(c)).await;
										}
									}
								}
							}
						}
						Ok(Message::Binary(data)) => {
							// Binary envelope layouts:
							// kind=0x00: [u8 0x00][u32 id_len][id][u32 meta_len][meta][RBCF payload]  - one-shot response
							// kind=0x01: [u8 0x01][u32 id_len][id][u32 meta_len][meta][RBCF payload]  - subscription change
							// kind=0x02: [u8 0x02][u32 batch_id_len][batch_id][u32 num_entries]
							//            then N * [u32 sub_id_len][sub_id][u32 rbcf_len][rbcf_bytes] - batch change
							if data.is_empty() { continue; }
							let kind = data[0];
							if kind == 0x02 {
								if let Some(payload) = parse_rbcf_batch_envelope(&data) {
									let batch_id = payload.batch_id.clone();
									let sender = {
										let routers = batch_routers.lock().await;
										routers.get(&batch_id).cloned()
									};
									if let Some(tx) = sender {
										let _ = tx.send(BatchPushEvent::Change(payload)).await;
									}
								}
								continue;
							}
							if data.len() < 5 { continue; }
							let id_len = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
							let meta_len_pos = 5 + id_len;
							if data.len() < meta_len_pos + 4 { continue; }
							let id = String::from_utf8_lossy(&data[5..meta_len_pos]).to_string();
							let meta_len = u32::from_le_bytes([
								data[meta_len_pos],
								data[meta_len_pos + 1],
								data[meta_len_pos + 2],
								data[meta_len_pos + 3],
							]) as usize;
							let meta_start = meta_len_pos + 4;
							if data.len() < meta_start + meta_len { continue; }
							let meta = if meta_len > 0 {
								from_str::<ResponseMeta>(
									&String::from_utf8_lossy(&data[meta_start..meta_start + meta_len])
								).ok()
							} else {
								None
							};
							let rbcf_data = &data[meta_start + meta_len..];
							let frames = match decode_frames(rbcf_data) {
								Ok(f) => f,
								Err(_) => continue,
							};
							match kind {
								0x00 => {
									let mut pending_guard = pending.lock().await;
									if let Some(tx) = pending_guard.remove(&id) {
										let _ = tx.send(ClientResponse::Frames(frames, meta));
									}
								}
								0x01 => {
									let _ = change_tx.send(ChangePayload {
										subscription_id: id,
										content_type: "application/vnd.reifydb.rbcf".to_string(),
										body: Value::Null,
										frames: Some(frames),
									}).await;
								}
								_ => {}
							}
						}
						Ok(Message::Ping(data)) => {
							let _ = write.send(Message::Pong(data)).await;
						}
						Ok(Message::Close(_)) => {
							break;
						}
						Err(_) => {
							break;
						}
						_ => {}
					}
				}

				// Handle outgoing requests
				Some((request, response_tx)) = request_rx.recv() => {
					let id = request.id.clone();

					// Register pending request
					{
						let mut pending_guard = pending.lock().await;
						pending_guard.insert(id, response_tx);
					}

					// Send the request
					if let Ok(json) = to_string(&request)
						&& write.send(Message::Text(json.into())).await.is_err() {
							break;
						}
				}

				// Handle shutdown signal
				_ = shutdown_rx.recv() => {
					let _ = write.send(Message::Close(None)).await;
					break;
				}
			}
		}

		// Clean up pending requests on disconnect
		let mut pending_guard = pending.lock().await;
		pending_guard.clear();
	}

	/// Compute the wire-format field for requests.
	///
	/// Maps the client-side `WireFormat` to the server's required `format` string.
	/// `WireFormat::Json` on this client refers to frames-shape JSON (`{frames: [...]}`),
	/// which the server now names `"frames"`.
	fn wire_format(&self) -> Option<String> {
		match self.format {
			WireFormat::Rbcf => Some("rbcf".to_string()),
			WireFormat::Json => Some("frames".to_string()),
			WireFormat::Proto => None,
		}
	}

	/// Authenticate with the server using a bearer token.
	pub async fn authenticate(&mut self, token: &str) -> Result<(), Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Auth(AuthRequest {
				token: Some(token.to_string()),
				method: None,
				credentials: None,
			}),
		};

		let response = self.send_request_json(request).await?;

		match response.payload {
			ResponsePayload::Auth(_) => {
				self.is_authenticated = true;
				Ok(())
			}
			ResponsePayload::Err(err) => Err(Error(Box::new(err.diagnostic))),
			_ => panic!("Unexpected response type for auth"), // FIXME better error handling
		}
	}

	/// Login with identifier and password.
	pub async fn login_with_password(&mut self, identifier: &str, password: &str) -> Result<LoginResult, Error> {
		let mut credentials = HashMap::new();
		credentials.insert("identifier".to_string(), identifier.to_string());
		credentials.insert("password".to_string(), password.to_string());
		self.login("password", credentials).await
	}

	pub async fn login_with_token(&mut self, token: &str) -> Result<LoginResult, Error> {
		let mut credentials = HashMap::new();
		credentials.insert("token".to_string(), token.to_string());
		self.login("token", credentials).await
	}

	pub async fn login(
		&mut self,
		method: &str,
		credentials: HashMap<String, String>,
	) -> Result<LoginResult, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Auth(AuthRequest {
				token: None,
				method: Some(method.to_string()),
				credentials: Some(credentials),
			}),
		};

		let response = self.send_request_json(request).await?;

		match response.payload {
			ResponsePayload::Auth(auth) => {
				if auth.status.as_deref() == Some("authenticated") {
					let token = auth.token.unwrap_or_default();
					let identity = auth.identity.unwrap_or_default();
					self.is_authenticated = true;
					Ok(LoginResult {
						token,
						identity,
					})
				} else {
					panic!("Authentication failed") // FIXME better error handling
				}
			}
			ResponsePayload::Err(err) => Err(Error(Box::new(err.diagnostic))),
			_ => panic!("Unexpected response type for login"), // FIXME better error handling
		}
	}

	/// Logout from the server, revoking the current session token.
	pub async fn logout(&mut self) -> Result<(), Error> {
		if !self.is_authenticated {
			return Ok(());
		}

		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Logout,
		};

		let response = self.send_request_json(request).await?;

		match response.payload {
			ResponsePayload::Logout(_) => {
				self.is_authenticated = false;
				Ok(())
			}
			ResponsePayload::Err(err) => Err(Error(Box::new(err.diagnostic))),
			_ => panic!("Unexpected response type for logout"), // FIXME better error handling
		}
	}

	/// Execute an admin (DDL + DML + Query) statement.
	pub async fn admin(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		Ok(self.admin_with_meta(rql, params).await?.frames)
	}

	/// Execute an admin statement and return frames together with server-reported metadata.
	pub async fn admin_with_meta(&self, rql: &str, params: Option<Params>) -> Result<AdminResult, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Admin(AdminRequest {
				rql: rql.to_string(),
				params: params.and_then(params_to_wire),
				format: self.wire_format(),
			}),
		};

		match self.send_request(request).await? {
			ClientResponse::Frames(frames, meta) => Ok(AdminResult {
				frames,
				meta,
			}),
			ClientResponse::Json(resp) => parse_admin_response(*resp),
		}
	}

	/// Execute a command (write) statement.
	pub async fn command(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		Ok(self.command_with_meta(rql, params).await?.frames)
	}

	/// Execute a command statement and return frames together with server-reported metadata.
	pub async fn command_with_meta(&self, rql: &str, params: Option<Params>) -> Result<CommandResult, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Command(CommandRequest {
				rql: rql.to_string(),
				params: params.and_then(params_to_wire),
				format: self.wire_format(),
			}),
		};

		match self.send_request(request).await? {
			ClientResponse::Frames(frames, meta) => Ok(CommandResult {
				frames,
				meta,
			}),
			ClientResponse::Json(resp) => parse_command_response(*resp),
		}
	}

	/// Execute a query (read) statement.
	pub async fn query(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		Ok(self.query_with_meta(rql, params).await?.frames)
	}

	/// Execute a query statement and return frames together with server-reported metadata.
	pub async fn query_with_meta(&self, rql: &str, params: Option<Params>) -> Result<QueryResult, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Query(QueryRequest {
				rql: rql.to_string(),
				params: params.and_then(params_to_wire),
				format: self.wire_format(),
			}),
		};

		match self.send_request(request).await? {
			ClientResponse::Frames(frames, meta) => Ok(QueryResult {
				frames,
				meta,
			}),
			ClientResponse::Json(resp) => parse_query_response(*resp),
		}
	}

	/// Invoke a WS binding by its globally-unique name.
	pub async fn call(&self, name: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		Ok(self.call_with_meta(name, params).await?.frames)
	}

	/// Invoke a WS binding and return frames together with server-reported metadata.
	pub async fn call_with_meta(&self, name: &str, params: Option<Params>) -> Result<CommandResult, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Call(CallRequest {
				name: name.to_string(),
				params: params.and_then(params_to_wire),
			}),
		};

		match self.send_request(request).await? {
			ClientResponse::Frames(frames, meta) => Ok(CommandResult {
				frames,
				meta,
			}),
			ClientResponse::Json(resp) => parse_call_response(*resp),
		}
	}

	/// Subscribe to real-time changes for a query.
	pub async fn subscribe(&self, rql: &str) -> Result<String, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Subscribe(SubscribeRequest {
				rql: rql.to_string(),
				format: self.wire_format(),
			}),
		};

		let response = self.send_request_json(request).await?;
		match response.payload {
			ResponsePayload::Subscribed(sub) => Ok(sub.subscription_id),
			ResponsePayload::Err(err) => Err(Error(Box::new(err.diagnostic))),
			_ => panic!("Unexpected response type for subscribe"), // FIXME better error handling
		}
	}

	/// Unsubscribe from a subscription.
	pub async fn unsubscribe(&self, subscription_id: &str) -> Result<(), Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Unsubscribe(UnsubscribeRequest {
				subscription_id: subscription_id.to_string(),
			}),
		};

		let response = self.send_request_json(request).await?;
		match response.payload {
			ResponsePayload::Unsubscribed(_) => Ok(()),
			ResponsePayload::Err(err) => Err(Error(Box::new(err.diagnostic))),
			_ => panic!("Unexpected response type for unsubscribe"), // FIXME better error handling
		}
	}

	/// Open a batch subscription over multiple RQL queries. Returns a handle that
	/// receives coalesced per-tick envelopes.
	pub async fn batch_subscribe(&self, queries: &[&str]) -> Result<WsBatchSubscription, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::BatchSubscribe(BatchSubscribeRequest {
				queries: queries.iter().map(|q| q.to_string()).collect(),
				format: self.wire_format(),
			}),
		};

		let response = self.send_request_json(request).await?;
		match response.payload {
			ResponsePayload::BatchSubscribed(ack) => {
				let (push_tx, push_rx) = mpsc::channel::<BatchPushEvent>(100);
				{
					let mut routers = self.batch_routers.lock().await;
					routers.insert(ack.batch_id.clone(), push_tx);
				}
				Ok(WsBatchSubscription {
					batch_id: ack.batch_id,
					members: ack.members,
					push_rx,
				})
			}
			ResponsePayload::Err(err) => Err(Error(Box::new(err.diagnostic))),
			_ => Err(Error(Box::new(Diagnostic {
				code: "UNEXPECTED_RESPONSE".to_string(),
				message: "Unexpected response type for BatchSubscribe".to_string(),
				..Default::default()
			}))),
		}
	}

	/// Unsubscribe a batch; cascade-removes all members server-side.
	pub async fn batch_unsubscribe(&self, batch_id: &str) -> Result<(), Error> {
		{
			let mut routers = self.batch_routers.lock().await;
			routers.remove(batch_id);
		}

		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::BatchUnsubscribe(BatchUnsubscribeRequest {
				batch_id: batch_id.to_string(),
			}),
		};

		let response = self.send_request_json(request).await?;
		match response.payload {
			ResponsePayload::BatchUnsubscribed(_) => Ok(()),
			ResponsePayload::Err(err) => Err(Error(Box::new(err.diagnostic))),
			_ => Err(Error(Box::new(Diagnostic {
				code: "UNEXPECTED_RESPONSE".to_string(),
				message: "Unexpected response type for BatchUnsubscribe".to_string(),
				..Default::default()
			}))),
		}
	}

	/// Receive the next change notification, waiting if necessary.
	pub async fn recv(&mut self) -> Option<ChangePayload> {
		self.change_rx.recv().await
	}

	/// Try to receive a change notification without blocking.
	pub fn try_recv(&mut self) -> Result<ChangePayload, mpsc::error::TryRecvError> {
		self.change_rx.try_recv()
	}

	/// Send a request and wait for the response (may be JSON or binary frames).
	async fn send_request(&self, request: Request) -> Result<ClientResponse, Error> {
		let (tx, rx) = oneshot::channel();

		self.request_tx.send((request, tx)).await.unwrap(); // FIXME better error handling

		Ok(rx.await.unwrap()) // FIXME better error handling
	}

	/// Send a request and expect a JSON response (for auth/subscribe/unsubscribe).
	async fn send_request_json(&self, request: Request) -> Result<Response, Error> {
		match self.send_request(request).await? {
			ClientResponse::Json(resp) => Ok(*resp),
			ClientResponse::Frames(_, _) => panic!("unexpected binary response"), /* FIXME better error
			                                                                       * handling */
		}
	}

	/// Close the WebSocket connection gracefully.
	pub async fn close(mut self) -> Result<(), Error> {
		if let Some(tx) = self.shutdown_tx.take() {
			let _ = tx.send(()).await;
		}
		Ok(())
	}

	/// Check if the client has authenticated.
	pub fn is_authenticated(&self) -> bool {
		self.is_authenticated
	}
}

impl Drop for WsClient {
	fn drop(&mut self) {
		if let Some(tx) = self.shutdown_tx.take() {
			// Best effort shutdown - ignore errors since we're dropping
			let _ = tx.try_send(());
		}
	}
}

/// Handle for a batch subscription over WebSocket. Each `recv()` yields one batch event.
pub struct WsBatchSubscription {
	batch_id: String,
	members: Vec<BatchMemberInfo>,
	push_rx: mpsc::Receiver<BatchPushEvent>,
}

impl WsBatchSubscription {
	pub fn batch_id(&self) -> &str {
		&self.batch_id
	}

	pub fn members(&self) -> &[BatchMemberInfo] {
		&self.members
	}

	/// Receive the next batch push event; returns `None` after the batch closes.
	pub async fn recv(&mut self) -> Option<BatchPushEvent> {
		self.push_rx.recv().await
	}
}

/// Parse an RBCF batch-change envelope (binary frame with kind=0x02).
///
/// Layout: `[u8 0x02][u32 batch_id_len][batch_id][u32 num_entries]` +
/// N * `[u32 sub_id_len][sub_id][u32 rbcf_len][rbcf_bytes]`.
fn parse_rbcf_batch_envelope(data: &[u8]) -> Option<BatchChangePayload> {
	if data.len() < 5 || data[0] != 0x02 {
		return None;
	}
	let batch_id_len = u32::from_le_bytes(data[1..5].try_into().ok()?) as usize;
	let batch_id_end = 5 + batch_id_len;
	if data.len() < batch_id_end + 4 {
		return None;
	}
	let batch_id = String::from_utf8_lossy(&data[5..batch_id_end]).into_owned();
	let num_entries = u32::from_le_bytes(data[batch_id_end..batch_id_end + 4].try_into().ok()?) as usize;
	let mut pos = batch_id_end + 4;
	let mut entries = Vec::with_capacity(num_entries);
	for _ in 0..num_entries {
		if data.len() < pos + 4 {
			return None;
		}
		let sub_id_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
		pos += 4;
		if data.len() < pos + sub_id_len + 4 {
			return None;
		}
		let sub_id = String::from_utf8_lossy(&data[pos..pos + sub_id_len]).into_owned();
		pos += sub_id_len;
		let rbcf_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
		pos += 4;
		if data.len() < pos + rbcf_len {
			return None;
		}
		let rbcf_bytes = &data[pos..pos + rbcf_len];
		pos += rbcf_len;
		let (frames, decode_error) = match decode_frames(rbcf_bytes) {
			Ok(f) => (Some(f), None),
			Err(e) => (None, Some(e.to_string())),
		};
		entries.push(BatchChangeEntry {
			subscription_id: sub_id,
			content_type: "application/vnd.reifydb.rbcf".to_string(),
			body: Value::Null,
			frames,
			decode_error,
		});
	}
	Some(BatchChangePayload {
		batch_id,
		entries,
	})
}
