// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
use std::{
	collections::HashMap,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use futures_util::{
	SinkExt, StreamExt,
	stream::{SplitSink, SplitStream},
};
use reifydb_codec::{frame::decode::decode_frames, json::from::convert_envelope_response};
use reifydb_value::{error::Error, params::Params, value::frame::frame::Frame};
use serde_json::{Value, from_str, to_string};
use tokio::{
	net::TcpStream,
	select, spawn,
	sync::{Mutex, mpsc, oneshot},
	time::{sleep, timeout},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config, tungstenite::Message};

use crate::{
	AdminRequest, AdminResult, AuthRequest, BatchChangeEntry, BatchChangePayload, BatchMemberInfo, BatchPushEvent,
	BatchSubscribeRequest, BatchUnsubscribeRequest, CallRequest, ChangePayload, CommandRequest, CommandResult,
	LoginResult, QueryRequest, QueryResult, ReconnectOptions, Request, RequestPayload, Response, ResponseMeta,
	ResponsePayload, ServerPush, SubscribeRequest, UnsubscribeRequest, WireBatchChangePayload, WireChangePayload,
	WireFormat,
	changes::frames_to_changes,
	client::{BatchSubscription as ClientBatchSubscription, ReifyClient, Subscription as ClientSubscription},
	error::ClientError,
	params_to_wire,
	reconnect::{backoff_millis, fire, millis_to_std},
	session::{parse_admin_response, parse_call_response, parse_command_response, parse_query_response},
	subscription::{BatchItem, SubscriptionConfig, build_subscription_rql},
	utils::generate_request_id,
};

/// Internal response type that can carry either a JSON Response or decoded RBCF frames.
enum ClientResponse {
	Json(Box<Response>),
	Frames(Vec<Frame>, Option<ResponseMeta>),
}

type WsWrite = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type WsRead = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

type PendingRequests = Arc<Mutex<HashMap<String, oneshot::Sender<ClientResponse>>>>;

/// Where a single subscription's changes are delivered.
#[derive(Clone)]
enum SubSink {
	/// A dedicated handle created via `ReifyClient::subscribe`.
	Dedicated(mpsc::Sender<ChangePayload>),
	/// The shared `WsClient::recv` channel used by the inherent `WsClient::subscribe`.
	Shared,
}

/// An active single subscription, keyed by its stable client id. `rql` is the fully built
/// `CREATE SUBSCRIPTION` statement, replayed verbatim to re-establish the subscription after
/// a reconnect; `server_id` is the server's current (per-connection) id for routing.
struct SubEntry {
	rql: String,
	sink: SubSink,
	server_id: Option<String>,
}

/// An active batch subscription, keyed by its stable client id.
struct BatchEntry {
	queries: Vec<String>,
	sender: mpsc::Sender<BatchPushEvent>,
	server_batch_id: Option<String>,
}

/// State shared between the public `WsClient` handle and the background connection task.
/// Cloning shares the underlying maps; only the plain `format` is copied.
#[derive(Clone)]
struct Shared {
	pending: PendingRequests,
	active_subs: Arc<Mutex<HashMap<u64, SubEntry>>>,
	active_batches: Arc<Mutex<HashMap<u64, BatchEntry>>>,
	server_to_client_sub: Arc<Mutex<HashMap<String, u64>>>,
	server_to_client_batch: Arc<Mutex<HashMap<String, u64>>>,
	pending_sub_acks: Arc<Mutex<HashMap<String, u64>>>,
	pending_batch_acks: Arc<Mutex<HashMap<String, u64>>>,
	format: WireFormat,
}

/// Options controlling a WebSocket connection, including automatic reconnection.
#[derive(Clone)]
pub struct WsClientOptions {
	pub format: WireFormat,
	pub reconnect: ReconnectOptions,
}

impl WsClientOptions {
	/// Options for `format` with the default reconnection policy.
	pub fn new(format: WireFormat) -> Self {
		Self {
			format,
			reconnect: ReconnectOptions::default(),
		}
	}
}

enum Flow {
	Continue,
	Closed,
}

enum PumpOutcome {
	Shutdown,
	Lost,
}

/// Async WebSocket client for ReifyDB
pub struct WsClient {
	request_tx: mpsc::Sender<(Request, oneshot::Sender<ClientResponse>)>,
	shutdown_tx: mpsc::Sender<()>,
	is_authenticated: bool,
	/// Channel for receiving server-initiated Change messages routed to the shared sink.
	change_rx: mpsc::UnboundedReceiver<ChangePayload>,
	shared: Shared,
	token: Arc<Mutex<Option<String>>>,
	sub_id_counter: Arc<AtomicU64>,
	batch_id_counter: Arc<AtomicU64>,
	format: WireFormat,
}

impl WsClient {
	/// Create a new WebSocket client connected to the given URL with the default reconnection
	/// policy.
	///
	/// # Arguments
	/// * `url` - WebSocket URL of the ReifyDB server (e.g., "ws://localhost:8090")
	/// * `format` - Wire format for responses
	pub async fn connect(url: &str, format: WireFormat) -> Result<Self, Error> {
		Self::connect_with_options(url, WsClientOptions::new(format)).await
	}

	/// Create a new WebSocket client with explicit options (wire format + reconnection policy).
	pub async fn connect_with_options(url: &str, options: WsClientOptions) -> Result<Self, Error> {
		let url = if !url.starts_with("ws://") && !url.starts_with("wss://") {
			format!("ws://{}", url)
		} else {
			url.to_string()
		};

		let (ws_stream, _) = connect_async_with_config(&url, None, true)
			.await
			.map_err(|e| ClientError::Transport(format!("Failed to connect: {}", e)))?;
		let (write, read) = ws_stream.split();

		let (request_tx, request_rx) = mpsc::channel::<(Request, oneshot::Sender<ClientResponse>)>(32);
		let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);
		let (change_tx, change_rx) = mpsc::unbounded_channel::<ChangePayload>();

		let shared = Shared {
			pending: Arc::new(Mutex::new(HashMap::new())),
			active_subs: Arc::new(Mutex::new(HashMap::new())),
			active_batches: Arc::new(Mutex::new(HashMap::new())),
			server_to_client_sub: Arc::new(Mutex::new(HashMap::new())),
			server_to_client_batch: Arc::new(Mutex::new(HashMap::new())),
			pending_sub_acks: Arc::new(Mutex::new(HashMap::new())),
			pending_batch_acks: Arc::new(Mutex::new(HashMap::new())),
			format: options.format,
		};

		let token = Arc::new(Mutex::new(None));

		let task_shared = shared.clone();
		let task_token = token.clone();
		let task_url = url.clone();
		let reconnect = options.reconnect;
		spawn(async move {
			Self::run_connection(
				task_url,
				reconnect,
				write,
				read,
				request_rx,
				shutdown_rx,
				task_shared,
				task_token,
				change_tx,
			)
			.await;
		});

		Ok(Self {
			request_tx,
			shutdown_tx,
			is_authenticated: false,
			change_rx,
			shared,
			token,
			sub_id_counter: Arc::new(AtomicU64::new(1)),
			batch_id_counter: Arc::new(AtomicU64::new(1)),
			format: options.format,
		})
	}

	/// Drive one connection until it is lost or a graceful shutdown is requested, reconnecting
	/// with exponential backoff in between (per the supplied [`ReconnectOptions`]).
	#[allow(clippy::too_many_arguments)]
	async fn run_connection(
		url: String,
		options: ReconnectOptions,
		mut write: WsWrite,
		mut read: WsRead,
		mut request_rx: mpsc::Receiver<(Request, oneshot::Sender<ClientResponse>)>,
		mut shutdown_rx: mpsc::Receiver<()>,
		shared: Shared,
		token: Arc<Mutex<Option<String>>>,
		change_tx: mpsc::UnboundedSender<ChangePayload>,
	) {
		loop {
			match Self::pump(&mut write, &mut read, &mut request_rx, &mut shutdown_rx, &shared, &change_tx)
				.await
			{
				PumpOutcome::Shutdown => {
					let _ = write.send(Message::Close(None)).await;
					break;
				}
				PumpOutcome::Lost => {
					Self::reject_pending(&shared).await;
					fire(&options.on_disconnect);
					match Self::reconnect(
						&url,
						&options,
						&shared,
						&token,
						&mut request_rx,
						&mut shutdown_rx,
						&change_tx,
					)
					.await
					{
						Some((new_write, new_read)) => {
							write = new_write;
							read = new_read;
							fire(&options.on_reconnect);
						}
						None => break,
					}
				}
			}
		}

		Self::reject_pending(&shared).await;
	}

	/// Run the active-connection select loop. Returns when the socket is lost or shutdown fires.
	async fn pump(
		write: &mut WsWrite,
		read: &mut WsRead,
		request_rx: &mut mpsc::Receiver<(Request, oneshot::Sender<ClientResponse>)>,
		shutdown_rx: &mut mpsc::Receiver<()>,
		shared: &Shared,
		change_tx: &mpsc::UnboundedSender<ChangePayload>,
	) -> PumpOutcome {
		loop {
			select! {
				msg = read.next() => {
					match msg {
						Some(m) => {
							if let Flow::Closed =
								Self::dispatch_message(m, write, shared, change_tx).await
							{
								return PumpOutcome::Lost;
							}
						}
						None => return PumpOutcome::Lost,
					}
				}
				Some((request, response_tx)) = request_rx.recv() => {
					let id = request.id.clone();
					shared.pending.lock().await.insert(id, response_tx);
					if let Ok(json) = to_string(&request)
						&& write.send(Message::Text(json.into())).await.is_err() {
							return PumpOutcome::Lost;
						}
				}
				_ = shutdown_rx.recv() => {
					return PumpOutcome::Shutdown;
				}
			}
		}
	}

	/// Handle a single inbound WebSocket message, routing responses and pushes through `shared`.
	async fn dispatch_message(
		msg: Result<Message, tokio_tungstenite::tungstenite::Error>,
		write: &mut WsWrite,
		shared: &Shared,
		change_tx: &mpsc::UnboundedSender<ChangePayload>,
	) -> Flow {
		match msg {
			Ok(Message::Text(text)) => {
				if let Ok(response) = from_str::<Response>(&text) {
					Self::handle_response(response, shared).await;
				} else if let Ok(push) = from_str::<ServerPush>(&text) {
					Self::handle_push(push, shared, change_tx).await;
				}
				Flow::Continue
			}
			Ok(Message::Binary(data)) => {
				Self::handle_binary(&data, shared, change_tx).await;
				Flow::Continue
			}
			Ok(Message::Ping(data)) => {
				let _ = write.send(Message::Pong(data)).await;
				Flow::Continue
			}
			Ok(Message::Close(_)) => Flow::Closed,
			Err(_) => Flow::Closed,
			_ => Flow::Continue,
		}
	}

	async fn handle_response(response: Response, shared: &Shared) {
		match &response.payload {
			ResponsePayload::Subscribed(ack) => {
				if let Some(cid) = shared.pending_sub_acks.lock().await.remove(&response.id) {
					shared.server_to_client_sub
						.lock()
						.await
						.insert(ack.subscription_id.clone(), cid);
					if let Some(entry) = shared.active_subs.lock().await.get_mut(&cid) {
						entry.server_id = Some(ack.subscription_id.clone());
					}
				}
			}
			ResponsePayload::BatchSubscribed(ack) => {
				if let Some(cid) = shared.pending_batch_acks.lock().await.remove(&response.id) {
					shared.server_to_client_batch.lock().await.insert(ack.batch_id.clone(), cid);
					if let Some(entry) = shared.active_batches.lock().await.get_mut(&cid) {
						entry.server_batch_id = Some(ack.batch_id.clone());
					}
				}
			}
			ResponsePayload::Err(_) => {
				shared.pending_sub_acks.lock().await.remove(&response.id);
				shared.pending_batch_acks.lock().await.remove(&response.id);
			}
			_ => {}
		}
		if let Some(tx) = shared.pending.lock().await.remove(&response.id) {
			let _ = tx.send(ClientResponse::Json(Box::new(response)));
		}
	}

	async fn handle_push(push: ServerPush, shared: &Shared, change_tx: &mpsc::UnboundedSender<ChangePayload>) {
		match push {
			ServerPush::Change(wire) => {
				let server_id = wire.subscription_id.clone();
				let payload = payload_from_json_change(wire);
				Self::route_change(shared, &server_id, payload, change_tx).await;
			}
			ServerPush::BatchChange(wire) => {
				let server_batch_id = wire.batch_id.clone();
				let payload = batch_change_from_json(wire);
				Self::route_batch(shared, &server_batch_id, BatchPushEvent::Change(payload)).await;
			}
			ServerPush::BatchMemberClosed(m) => {
				let server_batch_id = m.batch_id.clone();
				Self::route_batch(shared, &server_batch_id, BatchPushEvent::MemberClosed(m)).await;
			}
			ServerPush::BatchClosed(c) => {
				let server_batch_id = c.batch_id.clone();
				Self::route_batch(shared, &server_batch_id, BatchPushEvent::Closed(c)).await;
				if let Some(cid) = shared.server_to_client_batch.lock().await.remove(&server_batch_id) {
					shared.active_batches.lock().await.remove(&cid);
				}
			}
		}
	}

	async fn handle_binary(data: &[u8], shared: &Shared, change_tx: &mpsc::UnboundedSender<ChangePayload>) {
		if data.is_empty() {
			return;
		}
		let kind = data[0];
		if kind == 0x02 {
			if let Some(payload) = parse_rbcf_batch_envelope(data) {
				let server_batch_id = payload.batch_id.clone();
				Self::route_batch(shared, &server_batch_id, BatchPushEvent::Change(payload)).await;
			}
			return;
		}
		if data.len() < 5 {
			return;
		}
		let id_len = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
		let meta_len_pos = 5 + id_len;
		if data.len() < meta_len_pos + 4 {
			return;
		}
		let id = String::from_utf8_lossy(&data[5..meta_len_pos]).to_string();
		let meta_len = u32::from_le_bytes([
			data[meta_len_pos],
			data[meta_len_pos + 1],
			data[meta_len_pos + 2],
			data[meta_len_pos + 3],
		]) as usize;
		let meta_start = meta_len_pos + 4;
		if data.len() < meta_start + meta_len {
			return;
		}
		let meta = if meta_len > 0 {
			from_str::<ResponseMeta>(&String::from_utf8_lossy(&data[meta_start..meta_start + meta_len]))
				.ok()
		} else {
			None
		};
		let rbcf_data = &data[meta_start + meta_len..];
		let frames = match decode_frames(rbcf_data) {
			Ok(f) => f,
			Err(_) => return,
		};
		match kind {
			0x00 => {
				if let Some(tx) = shared.pending.lock().await.remove(&id) {
					let _ = tx.send(ClientResponse::Frames(frames, meta));
				}
			}
			0x01 => {
				let payload = ChangePayload {
					subscription_id: id.clone(),
					content_type: "application/vnd.reifydb.rbcf".to_string(),
					body: Value::Null,
					changes: frames_to_changes(frames),
				};
				Self::route_change(shared, &id, payload, change_tx).await;
			}
			_ => {}
		}
	}

	/// Deliver a single-subscription change to the handle behind its current server id, stamping
	/// the payload with the stable client id. Unknown ids fall through to the shared sink.
	async fn route_change(
		shared: &Shared,
		server_id: &str,
		mut payload: ChangePayload,
		change_tx: &mpsc::UnboundedSender<ChangePayload>,
	) {
		let client_id = shared.server_to_client_sub.lock().await.get(server_id).copied();
		match client_id {
			Some(cid) => {
				payload.subscription_id = cid.to_string();
				let sink = shared.active_subs.lock().await.get(&cid).map(|e| e.sink.clone());
				match sink {
					Some(SubSink::Dedicated(tx)) => {
						let _ = tx.send(payload).await;
					}
					_ => {
						let _ = change_tx.send(payload);
					}
				}
			}
			None => {
				let _ = change_tx.send(payload);
			}
		}
	}

	/// Deliver a batch event to the handle behind its current server batch id, stamping the
	/// event with the stable client batch id. Unknown ids are dropped.
	async fn route_batch(shared: &Shared, server_batch_id: &str, mut event: BatchPushEvent) {
		let client_id = shared.server_to_client_batch.lock().await.get(server_batch_id).copied();
		if let Some(cid) = client_id {
			stamp_batch_id(&mut event, cid);
			let sender = shared.active_batches.lock().await.get(&cid).map(|e| e.sender.clone());
			if let Some(tx) = sender {
				let _ = tx.send(event).await;
			}
		}
	}

	/// Drop all in-flight request oneshots; awaiting callers observe the closed channel and
	/// surface [`ClientError::ConnectionLost`].
	async fn reject_pending(shared: &Shared) {
		shared.pending.lock().await.clear();
	}

	/// Reconnect with exponential backoff. On success re-authenticates and replays every active
	/// subscription against the same handles. Returns `None` when attempts are exhausted or a
	/// shutdown is requested; while waiting, new requests are rejected so callers never block.
	async fn reconnect(
		url: &str,
		options: &ReconnectOptions,
		shared: &Shared,
		token: &Arc<Mutex<Option<String>>>,
		request_rx: &mut mpsc::Receiver<(Request, oneshot::Sender<ClientResponse>)>,
		shutdown_rx: &mut mpsc::Receiver<()>,
		change_tx: &mpsc::UnboundedSender<ChangePayload>,
	) -> Option<(WsWrite, WsRead)> {
		let mut attempt = 0u32;
		while attempt < options.max_reconnect_attempts {
			attempt += 1;
			let backoff_ms = backoff_millis(options.reconnect_delay_ms, attempt);
			if !Self::wait_backoff(backoff_ms, request_rx, shutdown_rx).await {
				return None;
			}

			let connect_fut = connect_async_with_config(url, None, true);
			let connect_timeout = millis_to_std(options.connect_timeout_ms);
			let stream = match timeout(connect_timeout, connect_fut).await {
				Ok(Ok((stream, _))) => stream,
				_ => continue,
			};
			let (mut write, mut read) = stream.split();

			let token_value = token.lock().await.clone();
			if let Some(t) = token_value
				&& !Self::reauth(&mut write, &mut read, shared, &t, change_tx).await
			{
				continue;
			}

			Self::resubscribe_all(&mut write, shared).await;
			return Some((write, read));
		}
		None
	}

	/// Sleep for `backoff_ms`, rejecting any requests that arrive and aborting on shutdown.
	/// Returns `false` if a shutdown was requested during the wait.
	async fn wait_backoff(
		backoff_ms: u64,
		request_rx: &mut mpsc::Receiver<(Request, oneshot::Sender<ClientResponse>)>,
		shutdown_rx: &mut mpsc::Receiver<()>,
	) -> bool {
		let deadline = sleep(millis_to_std(backoff_ms));
		tokio::pin!(deadline);
		loop {
			select! {
				_ = &mut deadline => return true,
				_ = shutdown_rx.recv() => return false,
				msg = request_rx.recv() => {
					match msg {
						Some((_, response_tx)) => drop(response_tx),
						None => return false,
					}
				}
			}
		}
	}

	/// Re-authenticate over a freshly opened socket, pumping inbound messages until the auth
	/// response resolves. Returns whether authentication succeeded.
	async fn reauth(
		write: &mut WsWrite,
		read: &mut WsRead,
		shared: &Shared,
		token: &str,
		change_tx: &mpsc::UnboundedSender<ChangePayload>,
	) -> bool {
		let id = generate_request_id();
		let (tx, rx) = oneshot::channel();
		shared.pending.lock().await.insert(id.clone(), tx);
		let request = Request {
			id,
			payload: RequestPayload::Auth(AuthRequest {
				token: Some(token.to_string()),
				method: None,
				credentials: None,
			}),
		};
		let json = match to_string(&request) {
			Ok(j) => j,
			Err(_) => return false,
		};
		if write.send(Message::Text(json.into())).await.is_err() {
			return false;
		}

		tokio::pin!(rx);
		loop {
			select! {
				msg = read.next() => {
					match msg {
						Some(m) => {
							if let Flow::Closed =
								Self::dispatch_message(m, write, shared, change_tx).await
							{
								return false;
							}
						}
						None => return false,
					}
				}
				res = &mut rx => {
					return matches!(res, Ok(ClientResponse::Json(resp)) if matches!(resp.payload, ResponsePayload::Auth(_)));
				}
			}
		}
	}

	/// Re-issue every active subscription and batch subscription over a fresh socket, keeping
	/// each stable client id bound to its existing handle. New server ids are wired by the
	/// normal ack handling once the loop resumes.
	async fn resubscribe_all(write: &mut WsWrite, shared: &Shared) {
		shared.server_to_client_sub.lock().await.clear();
		shared.server_to_client_batch.lock().await.clear();

		let subs: Vec<(u64, String)> = {
			let mut guard = shared.active_subs.lock().await;
			guard.iter_mut()
				.map(|(cid, entry)| {
					entry.server_id = None;
					(*cid, entry.rql.clone())
				})
				.collect()
		};
		for (cid, rql) in subs {
			let req_id = generate_request_id();
			shared.pending_sub_acks.lock().await.insert(req_id.clone(), cid);
			let request = Request {
				id: req_id,
				payload: RequestPayload::Subscribe(SubscribeRequest {
					rql,
					format: wire_format_str(shared.format),
				}),
			};
			if let Ok(json) = to_string(&request) {
				let _ = write.send(Message::Text(json.into())).await;
			}
		}

		let batches: Vec<(u64, Vec<String>)> = {
			let mut guard = shared.active_batches.lock().await;
			guard.iter_mut()
				.map(|(cid, entry)| {
					entry.server_batch_id = None;
					(*cid, entry.queries.clone())
				})
				.collect()
		};
		for (cid, queries) in batches {
			let req_id = generate_request_id();
			shared.pending_batch_acks.lock().await.insert(req_id.clone(), cid);
			let request = Request {
				id: req_id,
				payload: RequestPayload::BatchSubscribe(BatchSubscribeRequest {
					queries,
					format: wire_format_str(shared.format),
				}),
			};
			if let Ok(json) = to_string(&request) {
				let _ = write.send(Message::Text(json.into())).await;
			}
		}
	}

	/// Compute the wire-format field for requests.
	fn wire_format(&self) -> Option<String> {
		wire_format_str(self.format)
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
				*self.token.lock().await = Some(token.to_string());
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
					*self.token.lock().await = Some(token.clone());
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
				*self.token.lock().await = None;
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

	/// Register and issue a subscription, returning its stable client id. The changes flow to
	/// `sink`; the entry is retained for transparent replay across reconnects.
	async fn subscribe_inner(&self, built_rql: String, sink: SubSink) -> Result<u64, Error> {
		let client_id = self.sub_id_counter.fetch_add(1, Ordering::Relaxed);
		let req_id = generate_request_id();

		self.shared.active_subs.lock().await.insert(
			client_id,
			SubEntry {
				rql: built_rql.clone(),
				sink,
				server_id: None,
			},
		);
		self.shared.pending_sub_acks.lock().await.insert(req_id.clone(), client_id);

		let request = Request {
			id: req_id.clone(),
			payload: RequestPayload::Subscribe(SubscribeRequest {
				rql: built_rql,
				format: self.wire_format(),
			}),
		};

		let response = match self.send_request_json(request).await {
			Ok(r) => r,
			Err(e) => {
				self.shared.active_subs.lock().await.remove(&client_id);
				self.shared.pending_sub_acks.lock().await.remove(&req_id);
				return Err(e);
			}
		};

		match response.payload {
			ResponsePayload::Subscribed(_) => Ok(client_id),
			ResponsePayload::Err(err) => {
				self.shared.active_subs.lock().await.remove(&client_id);
				Err(Error(Box::new(err.diagnostic)))
			}
			_ => {
				self.shared.active_subs.lock().await.remove(&client_id);
				self.shared.pending_sub_acks.lock().await.remove(&req_id);
				Err(ClientError::UnexpectedResponse(
					"Unexpected response type for Subscribe".to_string(),
				)
				.into())
			}
		}
	}

	/// Subscribe to real-time changes for a query. Changes are delivered via [`WsClient::recv`].
	/// Returns the stable client subscription id, which is preserved across reconnects.
	pub async fn subscribe(&self, rql: &str, config: SubscriptionConfig) -> Result<String, Error> {
		let built = build_subscription_rql(rql, &config);
		let client_id = self.subscribe_inner(built, SubSink::Shared).await?;
		Ok(client_id.to_string())
	}

	/// Unsubscribe from a subscription by its stable client id.
	pub async fn unsubscribe(&self, subscription_id: &str) -> Result<(), Error> {
		let server_id = self.take_sub_server_id(subscription_id).await;
		let target = server_id.unwrap_or_else(|| subscription_id.to_string());

		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Unsubscribe(UnsubscribeRequest {
				subscription_id: target,
			}),
		};

		let response = self.send_request_json(request).await?;
		match response.payload {
			ResponsePayload::Unsubscribed(_) => Ok(()),
			ResponsePayload::Err(err) => Err(Error(Box::new(err.diagnostic))),
			_ => panic!("Unexpected response type for unsubscribe"), // FIXME better error handling
		}
	}

	/// Remove a subscription from the active registry and routing table, returning its current
	/// server id if it was tracked.
	async fn take_sub_server_id(&self, subscription_id: &str) -> Option<String> {
		let client_id = subscription_id.parse::<u64>().ok()?;
		let entry = self.shared.active_subs.lock().await.remove(&client_id)?;
		if let Some(server_id) = &entry.server_id {
			self.shared.server_to_client_sub.lock().await.remove(server_id);
		}
		entry.server_id
	}

	/// Open a batch subscription over multiple RQL queries. Returns a handle that
	/// receives coalesced per-tick envelopes.
	pub async fn batch_subscribe(&self, items: &[BatchItem<'_>]) -> Result<WsBatchSubscription, Error> {
		let client_id = self.batch_id_counter.fetch_add(1, Ordering::Relaxed);
		let req_id = generate_request_id();
		let (push_tx, push_rx) = mpsc::channel::<BatchPushEvent>(100);
		let queries: Vec<String> = items.iter().map(|i| build_subscription_rql(i.rql, &i.config)).collect();

		self.shared.active_batches.lock().await.insert(
			client_id,
			BatchEntry {
				queries: queries.clone(),
				sender: push_tx,
				server_batch_id: None,
			},
		);
		self.shared.pending_batch_acks.lock().await.insert(req_id.clone(), client_id);

		let request = Request {
			id: req_id.clone(),
			payload: RequestPayload::BatchSubscribe(BatchSubscribeRequest {
				queries,
				format: self.wire_format(),
			}),
		};

		let response = match self.send_request_json(request).await {
			Ok(r) => r,
			Err(e) => {
				self.shared.active_batches.lock().await.remove(&client_id);
				self.shared.pending_batch_acks.lock().await.remove(&req_id);
				return Err(e);
			}
		};
		match response.payload {
			ResponsePayload::BatchSubscribed(ack) => Ok(WsBatchSubscription {
				batch_id: client_id.to_string(),
				members: ack.members,
				push_rx,
			}),
			ResponsePayload::Err(err) => {
				self.shared.active_batches.lock().await.remove(&client_id);
				Err(Error(Box::new(err.diagnostic)))
			}
			_ => {
				self.shared.active_batches.lock().await.remove(&client_id);
				self.shared.pending_batch_acks.lock().await.remove(&req_id);
				Err(ClientError::UnexpectedResponse(
					"Unexpected response type for BatchSubscribe".to_string(),
				)
				.into())
			}
		}
	}

	/// Unsubscribe a batch; cascade-removes all members server-side.
	pub async fn batch_unsubscribe(&self, batch_id: &str) -> Result<(), Error> {
		let server_batch_id = self.take_batch_server_id(batch_id).await;
		let target = server_batch_id.unwrap_or_else(|| batch_id.to_string());

		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::BatchUnsubscribe(BatchUnsubscribeRequest {
				batch_id: target,
			}),
		};

		let response = self.send_request_json(request).await?;
		match response.payload {
			ResponsePayload::BatchUnsubscribed(_) => Ok(()),
			ResponsePayload::Err(err) => Err(Error(Box::new(err.diagnostic))),
			_ => Err(ClientError::UnexpectedResponse(
				"Unexpected response type for BatchUnsubscribe".to_string(),
			)
			.into()),
		}
	}

	async fn take_batch_server_id(&self, batch_id: &str) -> Option<String> {
		let client_id = batch_id.parse::<u64>().ok()?;
		let entry = self.shared.active_batches.lock().await.remove(&client_id)?;
		if let Some(server_batch_id) = &entry.server_batch_id {
			self.shared.server_to_client_batch.lock().await.remove(server_batch_id);
		}
		entry.server_batch_id
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

		if self.request_tx.send((request, tx)).await.is_err() {
			return Err(ClientError::ConnectionLost.into());
		}

		match rx.await {
			Ok(response) => Ok(response),
			Err(_) => Err(ClientError::ConnectionLost.into()),
		}
	}

	/// Send a request and expect a JSON response (for auth/subscribe/unsubscribe).
	async fn send_request_json(&self, request: Request) -> Result<Response, Error> {
		match self.send_request(request).await? {
			ClientResponse::Json(resp) => Ok(*resp),
			ClientResponse::Frames(_, _) => panic!("unexpected binary response"), /* FIXME better error
			                                                                       * handling */
		}
	}

	/// Close the WebSocket connection gracefully, disabling reconnection.
	pub async fn close(self) -> Result<(), Error> {
		let _ = self.shutdown_tx.send(()).await;
		Ok(())
	}

	/// Disconnect the client, disabling reconnection. Unlike [`WsClient::close`] this borrows
	/// the client so it can be triggered without consuming the handle (TS `disconnect` parity).
	pub async fn disconnect(&self) {
		let _ = self.shutdown_tx.send(()).await;
	}

	/// Check if the client has authenticated.
	pub fn is_authenticated(&self) -> bool {
		self.is_authenticated
	}
}

impl Drop for WsClient {
	fn drop(&mut self) {
		// Best effort shutdown - ignore errors since we're dropping
		let _ = self.shutdown_tx.try_send(());
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

fn wire_format_str(format: WireFormat) -> Option<String> {
	match format {
		WireFormat::Rbcf => Some("rbcf".to_string()),
		WireFormat::Json => Some("frames".to_string()),
	}
}

fn stamp_batch_id(event: &mut BatchPushEvent, client_id: u64) {
	let id = client_id.to_string();
	match event {
		BatchPushEvent::Change(payload) => payload.batch_id = id,
		BatchPushEvent::MemberClosed(m) => m.batch_id = id,
		BatchPushEvent::Closed(c) => c.batch_id = id,
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
		let (changes, decode_error) = match decode_frames(rbcf_bytes) {
			Ok(frames) => (frames_to_changes(frames), None),
			Err(e) => (Vec::new(), Some(e.to_string())),
		};
		entries.push(BatchChangeEntry {
			subscription_id: sub_id,
			content_type: "application/vnd.reifydb.rbcf".to_string(),
			body: Value::Null,
			changes,
			decode_error,
		});
	}
	Some(BatchChangePayload {
		batch_id,
		entries,
	})
}

fn payload_from_json_change(wire: WireChangePayload) -> ChangePayload {
	let changes = frames_to_changes(convert_envelope_response(wire.body.clone()));
	ChangePayload {
		subscription_id: wire.subscription_id,
		content_type: wire.content_type,
		body: wire.body,
		changes,
	}
}

fn batch_change_from_json(wire: WireBatchChangePayload) -> BatchChangePayload {
	let entries = wire
		.entries
		.into_iter()
		.map(|entry| {
			let changes = frames_to_changes(convert_envelope_response(entry.body.clone()));
			BatchChangeEntry {
				subscription_id: entry.subscription_id,
				content_type: entry.content_type,
				body: entry.body,
				changes,
				decode_error: None,
			}
		})
		.collect();
	BatchChangePayload {
		batch_id: wire.batch_id,
		entries,
	}
}

pub struct WsSubscription {
	subscription_id: String,
	change_rx: mpsc::Receiver<ChangePayload>,
}

#[async_trait::async_trait]
impl ClientSubscription for WsSubscription {
	fn subscription_id(&self) -> &str {
		&self.subscription_id
	}

	async fn recv(&mut self) -> Option<ChangePayload> {
		self.change_rx.recv().await
	}
}

#[async_trait::async_trait]
impl ClientBatchSubscription for WsBatchSubscription {
	fn batch_id(&self) -> &str {
		WsBatchSubscription::batch_id(self)
	}

	fn members(&self) -> &[BatchMemberInfo] {
		WsBatchSubscription::members(self)
	}

	async fn recv(&mut self) -> Option<BatchPushEvent> {
		WsBatchSubscription::recv(self).await
	}
}

#[async_trait::async_trait]
impl ReifyClient for WsClient {
	fn wire_format(&self) -> WireFormat {
		self.format
	}

	fn is_authenticated(&self) -> bool {
		WsClient::is_authenticated(self)
	}

	async fn authenticate(&mut self, token: &str) -> Result<(), Error> {
		WsClient::authenticate(self, token).await
	}

	async fn login_with_password(&mut self, identifier: &str, password: &str) -> Result<LoginResult, Error> {
		WsClient::login_with_password(self, identifier, password).await
	}

	async fn login_with_token(&mut self, token: &str) -> Result<LoginResult, Error> {
		WsClient::login_with_token(self, token).await
	}

	async fn logout(&mut self) -> Result<(), Error> {
		WsClient::logout(self).await
	}

	async fn admin(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		WsClient::admin(self, rql, params).await
	}

	async fn admin_with_meta(&self, rql: &str, params: Option<Params>) -> Result<AdminResult, Error> {
		WsClient::admin_with_meta(self, rql, params).await
	}

	async fn command(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		WsClient::command(self, rql, params).await
	}

	async fn command_with_meta(&self, rql: &str, params: Option<Params>) -> Result<CommandResult, Error> {
		WsClient::command_with_meta(self, rql, params).await
	}

	async fn query(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		WsClient::query(self, rql, params).await
	}

	async fn query_with_meta(&self, rql: &str, params: Option<Params>) -> Result<QueryResult, Error> {
		WsClient::query_with_meta(self, rql, params).await
	}

	async fn call(&self, name: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		WsClient::call(self, name, params).await
	}

	async fn call_with_meta(&self, name: &str, params: Option<Params>) -> Result<CommandResult, Error> {
		WsClient::call_with_meta(self, name, params).await
	}

	async fn subscribe(&self, rql: &str, config: SubscriptionConfig) -> Result<Box<dyn ClientSubscription>, Error> {
		let (change_tx, change_rx) = mpsc::channel::<ChangePayload>(100);
		let built = build_subscription_rql(rql, &config);
		let client_id = self.subscribe_inner(built, SubSink::Dedicated(change_tx)).await?;
		Ok(Box::new(WsSubscription {
			subscription_id: client_id.to_string(),
			change_rx,
		}))
	}

	async fn unsubscribe(&self, subscription_id: &str) -> Result<(), Error> {
		WsClient::unsubscribe(self, subscription_id).await
	}

	async fn batch_subscribe<'a>(
		&self,
		items: &[BatchItem<'a>],
	) -> Result<Box<dyn ClientBatchSubscription>, Error> {
		let sub = WsClient::batch_subscribe(self, items).await?;
		Ok(Box::new(sub))
	}

	async fn batch_unsubscribe(&self, batch_id: &str) -> Result<(), Error> {
		WsClient::batch_unsubscribe(self, batch_id).await
	}
}
