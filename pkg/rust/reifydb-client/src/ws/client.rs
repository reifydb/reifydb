// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
use std::{
	collections::{BTreeMap, HashMap},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use futures_util::{
	SinkExt, StreamExt,
	stream::{SplitSink, SplitStream},
};
use reifydb_codec::{frame::decode::decode_frames, json::from::frames_from_envelope};
use reifydb_value::{error::Error, params::Params, value::frame::frame::Frame};
use serde_json::{Value, from_str, to_string};
use tokio::{
	net::TcpStream,
	pin, select, spawn,
	sync::{Mutex, mpsc, oneshot},
	time::{sleep, timeout},
};
use tokio_tungstenite::{
	MaybeTlsStream, WebSocketStream, connect_async_with_config,
	tungstenite::{Error as TungsteniteError, Message},
};

use crate::{
	AdminRequest, AdminResult, AuthRequest, CallRequest, ChangePayload, CommandRequest, CommandResult, ErrResponse,
	FrameChange, LoginResult, QueryRequest, QueryResult, QueueClaimRequest, ReconnectOptions, Request,
	RequestPayload, Response, ResponseMeta, ResponsePayload, ServerPush, SubscribeRequest, UnsubscribeRequest,
	WireChangePayload, WireFormat, WireSubscribeItem, WsQueueClaimRequest,
	changes::frames_to_changes,
	client::{ReifyClient, Subscription as ClientSubscription},
	error::ClientError,
	params_to_wire,
	reconnect::{backoff_millis, fire, millis_to_std},
	session::{parse_admin_response, parse_call_response, parse_command_response, parse_query_response},
	subscription::{SubscribeItem, SubscriptionConfig},
	subscription_options_to_wire,
	utils::generate_request_id,
};

enum ClientResponse {
	Json(Box<Response>),
	Frames(Vec<Frame>, Option<ResponseMeta>),
}

type WsWrite = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type WsRead = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

type PendingRequests = Arc<Mutex<HashMap<String, oneshot::Sender<ClientResponse>>>>;

struct SubEntry {
	rql: String,
	config: SubscriptionConfig,
	sender: mpsc::Sender<ChangePayload>,
	server_id: Option<String>,
	call_id: u64,
}

#[derive(Clone)]
struct Shared {
	pending: PendingRequests,
	active_subs: Arc<Mutex<HashMap<u64, SubEntry>>>,
	server_to_client_sub: Arc<Mutex<HashMap<String, u64>>>,
	pending_sub_acks: Arc<Mutex<HashMap<String, Vec<u64>>>>,
	format: WireFormat,
}

#[derive(Clone)]
pub struct WsClientOptions {
	pub format: WireFormat,
	pub reconnect: ReconnectOptions,
}

impl WsClientOptions {
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
	Lost {
		received: bool,
	},
}

pub struct WsClient {
	request_tx: mpsc::Sender<(Request, oneshot::Sender<ClientResponse>)>,
	shutdown_tx: mpsc::Sender<()>,
	is_authenticated: bool,
	shared: Shared,
	token: Arc<Mutex<Option<String>>>,
	sub_id_counter: Arc<AtomicU64>,
	call_id_counter: Arc<AtomicU64>,
	format: WireFormat,
}

impl WsClient {
	pub async fn connect(url: &str, format: WireFormat) -> Result<Self, Error> {
		Self::connect_with_options(url, WsClientOptions::new(format)).await
	}

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

		let shared = Shared {
			pending: Arc::new(Mutex::new(HashMap::new())),
			active_subs: Arc::new(Mutex::new(HashMap::new())),
			server_to_client_sub: Arc::new(Mutex::new(HashMap::new())),
			pending_sub_acks: Arc::new(Mutex::new(HashMap::new())),
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
			)
			.await;
		});

		Ok(Self {
			request_tx,
			shutdown_tx,
			is_authenticated: false,
			shared,
			token,
			sub_id_counter: Arc::new(AtomicU64::new(1)),
			call_id_counter: Arc::new(AtomicU64::new(1)),
			format: options.format,
		})
	}

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
	) {
		let mut attempt = 0u32;
		loop {
			match Self::pump(&mut write, &mut read, &mut request_rx, &mut shutdown_rx, &shared).await {
				PumpOutcome::Shutdown => {
					let _ = write.send(Message::Close(None)).await;
					break;
				}
				PumpOutcome::Lost {
					received,
				} => {
					if received {
						attempt = 0;
					}
					Self::reject_pending(&shared).await;
					fire(&options.on_disconnect);
					match Self::reconnect(
						&url,
						&options,
						&shared,
						&token,
						&mut request_rx,
						&mut shutdown_rx,
						&mut attempt,
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
		shared.active_subs.lock().await.clear();
	}

	async fn pump(
		write: &mut WsWrite,
		read: &mut WsRead,
		request_rx: &mut mpsc::Receiver<(Request, oneshot::Sender<ClientResponse>)>,
		shutdown_rx: &mut mpsc::Receiver<()>,
		shared: &Shared,
	) -> PumpOutcome {
		let mut received = false;
		loop {
			select! {
				msg = read.next() => {
					match msg {
						Some(m) => {
							match Self::dispatch_message(m, write, shared).await {
								Flow::Closed => return PumpOutcome::Lost { received },
								Flow::Continue => received = true,
							}
						}
						None => return PumpOutcome::Lost { received },
					}
				}
				Some((request, response_tx)) = request_rx.recv() => {
					let id = request.id.clone();
					shared.pending.lock().await.insert(id, response_tx);
					if let Ok(json) = to_string(&request)
						&& write.send(Message::Text(json.into())).await.is_err() {
							return PumpOutcome::Lost { received };
						}
				}
				_ = shutdown_rx.recv() => {
					return PumpOutcome::Shutdown;
				}
			}
		}
	}

	async fn dispatch_message(
		msg: Result<Message, TungsteniteError>,
		write: &mut WsWrite,
		shared: &Shared,
	) -> Flow {
		match msg {
			Ok(Message::Text(text)) => {
				if let Ok(response) = from_str::<Response>(&text) {
					Self::handle_response(response, shared).await;
				} else if let Ok(push) = from_str::<ServerPush>(&text) {
					Self::handle_push(push, shared).await;
				} else if let Some(id) = request_id_of(&text) {
					Self::fail_pending(
						shared,
						id,
						"server sent a text reply that is neither a response nor a push"
							.to_string(),
					)
					.await;
				}
				Flow::Continue
			}
			Ok(Message::Binary(data)) => {
				Self::handle_binary(&data, shared).await;
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

	async fn fail_pending(shared: &Shared, id: String, reason: String) {
		let Some(tx) = shared.pending.lock().await.remove(&id) else {
			return;
		};
		let error: Error = ClientError::Decode(reason).into();
		let _ = tx.send(ClientResponse::Json(Box::new(Response {
			id,
			payload: ResponsePayload::Err(ErrResponse {
				diagnostic: *error.0,
			}),
		})));
	}

	async fn handle_response(response: Response, shared: &Shared) {
		match &response.payload {
			ResponsePayload::Subscribed(ack) => {
				if let Some(cids) = shared.pending_sub_acks.lock().await.remove(&response.id) {
					let mut server_to_client = shared.server_to_client_sub.lock().await;
					let mut active = shared.active_subs.lock().await;
					for info in &ack.subscriptions {
						let Some(cid) = cids.get(info.index) else {
							continue;
						};
						server_to_client.insert(info.subscription_id.clone(), *cid);
						if let Some(entry) = active.get_mut(cid) {
							entry.server_id = Some(info.subscription_id.clone());
						}
					}
				}
			}
			ResponsePayload::Err(_) => {
				shared.pending_sub_acks.lock().await.remove(&response.id);
			}
			_ => {}
		}
		if let Some(tx) = shared.pending.lock().await.remove(&response.id) {
			let _ = tx.send(ClientResponse::Json(Box::new(response)));
		}
	}

	async fn handle_push(push: ServerPush, shared: &Shared) {
		match push {
			ServerPush::Change(wire) => {
				for payload in change_entries_from_json(wire) {
					let server_id = payload.subscription_id.clone();
					Self::route_change(shared, &server_id, payload).await;
				}
			}
			ServerPush::SubscriptionClosed(closed) => {
				Self::close_subscription(shared, &closed.subscription_id).await
			}
		}
	}

	async fn handle_binary(data: &[u8], shared: &Shared) {
		if data.is_empty() {
			return;
		}
		if data[0] == 0x01 {
			if let Some(payloads) = parse_rbcf_change_envelope(data) {
				for payload in payloads {
					let server_id = payload.subscription_id.clone();
					Self::route_change(shared, &server_id, payload).await;
				}
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
			Self::fail_pending(shared, id, "binary reply truncated inside its meta section".to_string())
				.await;
			return;
		}
		let meta = if meta_len > 0 {
			match from_str::<ResponseMeta>(&String::from_utf8_lossy(
				&data[meta_start..meta_start + meta_len],
			)) {
				Ok(meta) => Some(meta),
				Err(e) => {
					Self::fail_pending(
						shared,
						id,
						format!("failed to parse the reply meta: {}", e),
					)
					.await;
					return;
				}
			}
		} else {
			None
		};
		let rbcf_data = &data[meta_start + meta_len..];
		if data[0] == 0x00
			&& let Some(tx) = shared.pending.lock().await.remove(&id)
		{
			let _ = tx.send(query_response_from_rbcf(id, rbcf_data, meta));
		}
	}

	async fn route_change(shared: &Shared, server_id: &str, mut payload: ChangePayload) {
		let Some(cid) = shared.server_to_client_sub.lock().await.get(server_id).copied() else {
			return;
		};
		payload.subscription_id = cid.to_string();
		let sender = shared.active_subs.lock().await.get(&cid).map(|entry| entry.sender.clone());
		if let Some(sender) = sender {
			let _ = sender.send(payload).await;
		}
	}

	async fn close_subscription(shared: &Shared, server_id: &str) {
		let Some(cid) = shared.server_to_client_sub.lock().await.remove(server_id) else {
			return;
		};
		shared.active_subs.lock().await.remove(&cid);
	}

	async fn reject_pending(shared: &Shared) {
		shared.pending.lock().await.clear();
	}

	#[allow(clippy::too_many_arguments)]
	async fn reconnect(
		url: &str,
		options: &ReconnectOptions,
		shared: &Shared,
		token: &Arc<Mutex<Option<String>>>,
		request_rx: &mut mpsc::Receiver<(Request, oneshot::Sender<ClientResponse>)>,
		shutdown_rx: &mut mpsc::Receiver<()>,
		attempt: &mut u32,
	) -> Option<(WsWrite, WsRead)> {
		while *attempt < options.max_reconnect_attempts {
			*attempt += 1;
			let backoff_ms = backoff_millis(options.reconnect_delay_ms, *attempt);
			if !Self::wait_backoff(backoff_ms, request_rx, shutdown_rx).await {
				return None;
			}

			let opened = timeout(
				millis_to_std(options.connect_timeout_ms),
				Self::open_socket(url, shared, token),
			)
			.await;
			if let Ok(Some(pair)) = opened {
				return Some(pair);
			}
		}
		None
	}

	async fn open_socket(
		url: &str,
		shared: &Shared,
		token: &Arc<Mutex<Option<String>>>,
	) -> Option<(WsWrite, WsRead)> {
		let (stream, _) = connect_async_with_config(url, None, true).await.ok()?;
		let (mut write, mut read) = stream.split();

		let token_value = token.lock().await.clone();
		if let Some(t) = token_value
			&& !Self::reauth(&mut write, &mut read, shared, &t).await
		{
			return None;
		}

		Self::resubscribe_all(&mut write, shared).await;
		Some((write, read))
	}

	async fn wait_backoff(
		backoff_ms: u64,
		request_rx: &mut mpsc::Receiver<(Request, oneshot::Sender<ClientResponse>)>,
		shutdown_rx: &mut mpsc::Receiver<()>,
	) -> bool {
		let deadline = sleep(millis_to_std(backoff_ms));
		pin!(deadline);
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

	async fn reauth(write: &mut WsWrite, read: &mut WsRead, shared: &Shared, token: &str) -> bool {
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

		pin!(rx);
		loop {
			select! {
				msg = read.next() => {
					match msg {
						Some(m) => {
							if let Flow::Closed =
								Self::dispatch_message(m, write, shared).await
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

	async fn resubscribe_all(write: &mut WsWrite, shared: &Shared) {
		shared.server_to_client_sub.lock().await.clear();

		let mut calls: BTreeMap<u64, Vec<(u64, WireSubscribeItem)>> = BTreeMap::new();
		{
			let mut guard = shared.active_subs.lock().await;
			for (cid, entry) in guard.iter_mut() {
				entry.server_id = None;
				calls.entry(entry.call_id).or_default().push((
					*cid,
					WireSubscribeItem {
						rql: entry.rql.clone(),
						options: subscription_options_to_wire(&entry.config),
					},
				));
			}
		}
		for (_, mut items) in calls {
			items.sort_by_key(|(cid, _)| *cid);
			let (cids, subscriptions): (Vec<u64>, Vec<WireSubscribeItem>) = items.into_iter().unzip();
			let req_id = generate_request_id();
			shared.pending_sub_acks.lock().await.insert(req_id.clone(), cids);
			let request = Request {
				id: req_id,
				payload: RequestPayload::Subscribe(SubscribeRequest {
					subscriptions,
					format: Some(shared.format),
				}),
			};
			if let Ok(json) = to_string(&request) {
				let _ = write.send(Message::Text(json.into())).await;
			}
		}
	}

	fn wire_format(&self) -> Option<WireFormat> {
		Some(self.format)
	}

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
			_ => {
				Err(ClientError::UnexpectedResponse("unexpected response type for auth".to_string())
					.into())
			}
		}
	}

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
					Err(ClientError::NotAuthenticated(format!(
						"login was not accepted, server reported status {:?}",
						auth.status
					))
					.into())
				}
			}
			ResponsePayload::Err(err) => Err(Error(Box::new(err.diagnostic))),
			_ => Err(ClientError::UnexpectedResponse("unexpected response type for login".to_string())
				.into()),
		}
	}

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
			_ => Err(ClientError::UnexpectedResponse("unexpected response type for logout".to_string())
				.into()),
		}
	}

	pub async fn admin(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		Ok(self.admin_with_meta(rql, params).await?.frames)
	}

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

	pub async fn command(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		Ok(self.command_with_meta(rql, params).await?.frames)
	}

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

	pub async fn query(&self, rql: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		Ok(self.query_with_meta(rql, params).await?.frames)
	}

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

	pub async fn call(&self, name: &str, params: Option<Params>) -> Result<Vec<Frame>, Error> {
		Ok(self.call_with_meta(name, params).await?.frames)
	}

	pub async fn call_with_meta(&self, name: &str, params: Option<Params>) -> Result<CommandResult, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Call(CallRequest {
				name: name.to_string(),
				params: params.and_then(params_to_wire),
				format: self.wire_format(),
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

	pub async fn queue_claim(&self, request: QueueClaimRequest) -> Result<Vec<Frame>, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::QueueClaim(WsQueueClaimRequest {
				queue: request.queue,
				worker: request.worker,
				max_n: request.max_n,
				lease_ttl: request.lease_ttl,
				wait_for: request.wait_for,
				format: self.wire_format(),
			}),
		};

		match self.send_request(request).await? {
			ClientResponse::Frames(frames, _) => Ok(frames),
			ClientResponse::Json(resp) => parse_call_response(*resp).map(|result| result.frames),
		}
	}

	pub async fn subscribe(&self, items: &[SubscribeItem<'_>]) -> Result<Vec<WsSubscription>, Error> {
		let call_id = self.call_id_counter.fetch_add(1, Ordering::Relaxed);
		let req_id = generate_request_id();
		let mut cids = Vec::with_capacity(items.len());
		let mut receivers = Vec::with_capacity(items.len());
		{
			let mut active = self.shared.active_subs.lock().await;
			for item in items {
				let cid = self.sub_id_counter.fetch_add(1, Ordering::Relaxed);
				let (sender, receiver) = mpsc::channel::<ChangePayload>(100);
				active.insert(
					cid,
					SubEntry {
						rql: item.rql.to_string(),
						config: item.config.clone(),
						sender,
						server_id: None,
						call_id,
					},
				);
				cids.push(cid);
				receivers.push(receiver);
			}
		}
		self.shared.pending_sub_acks.lock().await.insert(req_id.clone(), cids.clone());

		let request = Request {
			id: req_id.clone(),
			payload: RequestPayload::Subscribe(SubscribeRequest {
				subscriptions: subscriptions_to_wire(items),
				format: self.wire_format(),
			}),
		};

		let response = match self.send_request_json(request).await {
			Ok(r) => r,
			Err(e) => {
				self.forget(&cids, &req_id).await;
				return Err(e);
			}
		};

		match response.payload {
			ResponsePayload::Subscribed(ack) if ack.subscriptions.len() == items.len() => Ok(cids
				.into_iter()
				.zip(receivers)
				.map(|(cid, change_rx)| WsSubscription {
					subscription_id: cid.to_string(),
					change_rx,
				})
				.collect()),
			ResponsePayload::Subscribed(ack) => {
				self.forget(&cids, &req_id).await;
				Err(ClientError::UnexpectedResponse(format!(
					"Subscribed acknowledged {} of {} subscriptions",
					ack.subscriptions.len(),
					items.len()
				))
				.into())
			}
			ResponsePayload::Err(err) => {
				self.forget(&cids, &req_id).await;
				Err(Error(Box::new(err.diagnostic)))
			}
			_ => {
				self.forget(&cids, &req_id).await;
				Err(ClientError::UnexpectedResponse(
					"Unexpected response type for Subscribe".to_string(),
				)
				.into())
			}
		}
	}

	async fn forget(&self, cids: &[u64], req_id: &str) {
		self.shared.pending_sub_acks.lock().await.remove(req_id);
		let removed: Vec<SubEntry> = {
			let mut active = self.shared.active_subs.lock().await;
			cids.iter().filter_map(|cid| active.remove(cid)).collect()
		};
		let mut server_to_client = self.shared.server_to_client_sub.lock().await;
		for server_id in removed.iter().filter_map(|entry| entry.server_id.as_ref()) {
			server_to_client.remove(server_id);
		}
	}

	pub async fn unsubscribe(&self, subscription_ids: &[&str]) -> Result<(), Error> {
		let mut targets = Vec::with_capacity(subscription_ids.len());
		for subscription_id in subscription_ids {
			let server_id = self.take_sub_server_id(subscription_id).await;
			targets.push(server_id.unwrap_or_else(|| subscription_id.to_string()));
		}

		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Unsubscribe(UnsubscribeRequest {
				subscription_ids: targets,
			}),
		};

		let response = self.send_request_json(request).await?;
		match response.payload {
			ResponsePayload::Unsubscribed(_) => Ok(()),
			ResponsePayload::Err(err) => Err(Error(Box::new(err.diagnostic))),
			_ => Err(ClientError::UnexpectedResponse(
				"unexpected response type for unsubscribe".to_string(),
			)
			.into()),
		}
	}

	async fn take_sub_server_id(&self, subscription_id: &str) -> Option<String> {
		let client_id = subscription_id.parse::<u64>().ok()?;
		let entry = self.shared.active_subs.lock().await.remove(&client_id)?;
		if let Some(server_id) = &entry.server_id {
			self.shared.server_to_client_sub.lock().await.remove(server_id);
		}
		entry.server_id
	}

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

	async fn send_request_json(&self, request: Request) -> Result<Response, Error> {
		match self.send_request(request).await? {
			ClientResponse::Json(resp) => Ok(*resp),
			ClientResponse::Frames(_, _) => Err(ClientError::UnexpectedResponse(
				"expected a json response, got binary frames".to_string(),
			)
			.into()),
		}
	}

	pub async fn close(self) -> Result<(), Error> {
		let _ = self.shutdown_tx.send(()).await;
		Ok(())
	}

	pub async fn disconnect(&self) {
		let _ = self.shutdown_tx.send(()).await;
	}

	pub fn is_authenticated(&self) -> bool {
		self.is_authenticated
	}
}

impl Drop for WsClient {
	fn drop(&mut self) {
		let _ = self.shutdown_tx.try_send(());
	}
}

fn subscriptions_to_wire(items: &[SubscribeItem<'_>]) -> Vec<WireSubscribeItem> {
	items.iter()
		.map(|item| WireSubscribeItem {
			rql: item.rql.to_string(),
			options: subscription_options_to_wire(&item.config),
		})
		.collect()
}

fn request_id_of(text: &str) -> Option<String> {
	from_str::<Value>(text).ok()?.get("id")?.as_str().map(str::to_string)
}

fn parse_rbcf_change_envelope(data: &[u8]) -> Option<Vec<ChangePayload>> {
	if data.len() < 5 || data[0] != 0x01 {
		return None;
	}
	let num_entries = u32::from_le_bytes(data[1..5].try_into().ok()?) as usize;
	let mut pos = 5;
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
			entries.push(ChangePayload {
				subscription_id: sub_id,
				content_type: "application/vnd.reifydb.rbcf".to_string(),
				body: Value::Null,
				changes: Vec::new(),
				decode_error: Some(format!(
					"change entry truncated: declared {} payload bytes, {} remain",
					rbcf_len,
					data.len() - pos
				)),
			});
			break;
		}
		let (changes, decode_error) = changes_from_rbcf(&data[pos..pos + rbcf_len]);
		pos += rbcf_len;
		entries.push(ChangePayload {
			subscription_id: sub_id,
			content_type: "application/vnd.reifydb.rbcf".to_string(),
			body: Value::Null,
			changes,
			decode_error,
		});
	}
	Some(entries)
}

fn query_response_from_rbcf(id: String, rbcf: &[u8], meta: Option<ResponseMeta>) -> ClientResponse {
	match decode_frames(rbcf) {
		Ok(frames) => ClientResponse::Frames(frames, meta),
		Err(e) => {
			let error: Error = ClientError::Decode(format!("failed to decode RBCF payload: {}", e)).into();
			ClientResponse::Json(Box::new(Response {
				id,
				payload: ResponsePayload::Err(ErrResponse {
					diagnostic: *error.0,
				}),
			}))
		}
	}
}

fn changes_from_rbcf(rbcf: &[u8]) -> (Vec<FrameChange>, Option<String>) {
	match decode_frames(rbcf) {
		Ok(frames) => (frames_to_changes(frames), None),
		Err(e) => (Vec::new(), Some(e.to_string())),
	}
}

fn changes_from_envelope(body: Value) -> (Vec<FrameChange>, Option<String>) {
	match frames_from_envelope(body) {
		Ok(frames) => (frames_to_changes(frames), None),
		Err(e) => (Vec::new(), Some(e.to_string())),
	}
}

fn change_entries_from_json(wire: WireChangePayload) -> Vec<ChangePayload> {
	wire.entries
		.into_iter()
		.map(|entry| {
			let (changes, decode_error) = changes_from_envelope(entry.body.clone());
			ChangePayload {
				subscription_id: entry.subscription_id,
				content_type: entry.content_type,
				body: entry.body,
				changes,
				decode_error,
			}
		})
		.collect()
}

pub struct WsSubscription {
	subscription_id: String,
	change_rx: mpsc::Receiver<ChangePayload>,
}

impl WsSubscription {
	pub fn subscription_id(&self) -> &str {
		&self.subscription_id
	}

	pub async fn recv(&mut self) -> Option<ChangePayload> {
		self.change_rx.recv().await
	}
}

#[async_trait::async_trait]
impl ClientSubscription for WsSubscription {
	fn subscription_id(&self) -> &str {
		WsSubscription::subscription_id(self)
	}

	async fn recv(&mut self) -> Option<ChangePayload> {
		WsSubscription::recv(self).await
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

	async fn subscribe<'a>(&self, items: &[SubscribeItem<'a>]) -> Result<Vec<Box<dyn ClientSubscription>>, Error> {
		Ok(WsClient::subscribe(self, items)
			.await?
			.into_iter()
			.map(|subscription| Box::new(subscription) as Box<dyn ClientSubscription>)
			.collect())
	}

	async fn unsubscribe(&self, subscription_ids: &[&str]) -> Result<(), Error> {
		WsClient::unsubscribe(self, subscription_ids).await
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::frame::{encode::encode_frames, options::EncodeOptions};
	use reifydb_core::value::{batch::batch, column::factory};
	use reifydb_value::value::{Value, diff_type::DiffType, frame::frame::Frame};

	use super::{ClientResponse, changes_from_rbcf, query_response_from_rbcf};
	use crate::{ChangeKind, session::parse_query_response};

	fn update_frame(id: i32) -> Frame {
		Frame::from(batch(vec![factory::int4("id", [id])]).expect("an int4 column forms a batch"))
			.with_op(DiffType::Update)
	}

	fn rbcf(frames: &[Frame]) -> Vec<u8> {
		encode_frames(frames, &EncodeOptions::default()).unwrap()
	}

	fn corrupt_rbcf() -> Vec<u8> {
		let mut bytes = rbcf(&[update_frame(7)]);
		bytes.truncate(bytes.len() / 2);
		bytes
	}

	#[test]
	fn corrupt_change_frame_reaches_the_subscriber_as_a_decode_error() {
		// A change frame that fails to decode must surface its error, never pass as an empty change.
		let (changes, decode_error) = changes_from_rbcf(&corrupt_rbcf());

		assert!(decode_error.is_some(), "a failed decode must carry its error");
		assert!(changes.is_empty());
	}

	#[test]
	fn valid_change_frame_decodes_to_its_changes_with_no_error() {
		// A clean change frame must keep its rows and op, otherwise the error path swallowed a good change.
		let (changes, decode_error) = changes_from_rbcf(&rbcf(&[update_frame(7)]));

		assert_eq!(decode_error, None);
		assert_eq!(changes.len(), 1);
		assert_eq!(changes[0].kind, ChangeKind::Update);
		let id = changes[0].frame.column("id").expect("the id column reads").expect("the id column decodes");
		assert_eq!(id.get_value(0), Value::Int4(7));
	}

	#[test]
	fn corrupt_query_response_completes_the_request_with_a_decode_error() {
		// A response that fails to decode must fail the waiting request, never leave it waiting forever.
		let response = match query_response_from_rbcf("req-1".to_string(), &corrupt_rbcf(), None) {
			ClientResponse::Json(response) => response,
			ClientResponse::Frames(..) => panic!("a failed decode must not complete as frames"),
		};

		assert_eq!(response.id, "req-1");
		match parse_query_response(*response) {
			Err(error) => assert_eq!(error.0.code, "DECODE"),
			Ok(_) => panic!("a failed decode must reach the caller as an error"),
		}
	}

	#[test]
	fn valid_query_response_completes_the_request_with_its_frames() {
		// A clean response must complete as frames, otherwise every binary query would fail.
		let expected = vec![update_frame(7)];

		match query_response_from_rbcf("req-1".to_string(), &rbcf(&expected), None) {
			ClientResponse::Frames(frames, _) => assert_eq!(frames, expected),
			ClientResponse::Json(_) => panic!("a clean decode must complete as frames"),
		}
	}
}

#[cfg(test)]
mod json_change_tests {
	use reifydb_codec::json::to::convert_frames;
	use reifydb_core::value::{batch::batch, column::factory};
	use reifydb_value::value::{diff_type::DiffType, frame::frame::Frame};
	use serde_json::{Value, json};

	use super::change_entries_from_json;
	use crate::{WireChangeEntry, WireChangePayload};

	fn malformed_body() -> Value {
		json!({ "frames": [{ "columns": "not a column list" }] })
	}

	fn valid_body() -> Value {
		let frame = Frame::from(batch(vec![factory::int4("id", [7])]).expect("an int4 column forms a batch"))
			.with_op(DiffType::Update);
		json!({ "frames": convert_frames(&[frame]).expect("the frame converts") })
	}

	fn entry(subscription_id: &str, body: Value) -> WireChangeEntry {
		WireChangeEntry {
			subscription_id: subscription_id.to_string(),
			content_type: "application/vnd.reifydb.frames".to_string(),
			body,
		}
	}

	fn change(entries: Vec<WireChangeEntry>) -> WireChangePayload {
		WireChangePayload {
			entries,
		}
	}

	#[test]
	fn a_malformed_json_change_reaches_the_subscriber_as_a_decode_error() {
		// An empty change is a valid delivery, so a body that fails to decode must carry its error.
		let payloads = change_entries_from_json(change(vec![entry("sub-1", malformed_body())]));

		assert!(payloads[0].decode_error.is_some(), "a failed decode must carry its error");
		assert!(payloads[0].changes.is_empty());
	}

	#[test]
	fn a_valid_json_change_decodes_with_no_error() {
		// A clean change must keep its rows, otherwise the error path swallowed a good change.
		let payloads = change_entries_from_json(change(vec![entry("sub-1", valid_body())]));

		assert_eq!(payloads[0].decode_error, None);
		assert_eq!(payloads[0].changes.len(), 1);
	}

	#[test]
	fn a_malformed_json_entry_carries_its_own_decode_error() {
		// One entry that fails to decode must say so on itself, never on its neighbour or as an empty change.
		let payloads = change_entries_from_json(change(vec![
			entry("sub-1", valid_body()),
			entry("sub-2", malformed_body()),
		]));

		assert_eq!(payloads[0].decode_error, None, "the clean entry must decode on its own");
		assert_eq!(payloads[0].changes.len(), 1);
		assert!(payloads[1].decode_error.is_some(), "a failed decode must carry its error");
		assert!(payloads[1].changes.is_empty());
	}
}
