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
use serde_json::{from_str, to_string};
use tokio::{
	net::TcpStream,
	select, spawn,
	sync::{Mutex, mpsc, oneshot},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config, tungstenite::Message};

use crate::{
	AdminRequest, AdminResult, AuthRequest, ChangePayload, CommandRequest, CommandResult, Encoding, LoginResult,
	QueryRequest, QueryResult, Request, RequestPayload, Response, ResponsePayload, ServerPush, SubscribeRequest,
	UnsubscribeRequest, params_to_wire,
	session::{parse_admin_response, parse_command_response, parse_query_response},
	utils::generate_request_id,
};

/// Internal response type that can carry either a JSON Response or decoded RBCF frames.
enum ClientResponse {
	Json(Response),
	Frames(Vec<Frame>),
}

type PendingRequests = Arc<Mutex<HashMap<String, oneshot::Sender<ClientResponse>>>>;

/// Async WebSocket client for ReifyDB
pub struct WsClient {
	request_tx: mpsc::Sender<(Request, oneshot::Sender<ClientResponse>)>,
	shutdown_tx: Option<mpsc::Sender<()>>,
	is_authenticated: bool,
	/// Channel for receiving server-initiated Change messages.
	change_rx: mpsc::Receiver<ChangePayload>,
	encoding: Encoding,
}

impl WsClient {
	/// Create a new WebSocket client connected to the given URL.
	///
	/// # Arguments
	/// * `url` - WebSocket URL of the ReifyDB server (e.g., "ws://localhost:8090")
	/// * `encoding` - Wire format encoding for responses
	pub async fn connect(url: &str, encoding: Encoding) -> Result<Self, Error> {
		if encoding == Encoding::Proto {
			return Err(Error(Box::new(Diagnostic {
				code: "INVALID_ENCODING".to_string(),
				message: "Encoding::Proto is not supported for WsClient".to_string(),
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

		// Spawn the connection management task
		let pending_clone = pending.clone();
		spawn(async move {
			Self::connection_loop(write, read, request_rx, shutdown_rx, pending_clone, change_tx).await;
		});

		Ok(Self {
			request_tx,
			shutdown_tx: Some(shutdown_tx),
			is_authenticated: false,
			change_rx,
			encoding,
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
									let _ = tx.send(ClientResponse::Json(response));
								}
							}
							// Then try to parse as ServerPush (no id field)
							else if let Ok(push) = from_str::<ServerPush>(&text) {
								match push {
									ServerPush::Change(change) => {
										let _ = change_tx.send(change).await;
									}
								}
							}
						}
						Ok(Message::Binary(data)) => {
							// RBCF binary envelope: [u32 LE id_len][id bytes][RBCF payload]
							if data.len() >= 4 {
								let id_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
								if data.len() >= 4 + id_len {
									let id = String::from_utf8_lossy(&data[4..4 + id_len]).to_string();
									let rbcf_data = &data[4 + id_len..];
									if let Ok(frames) = reifydb_wire_format::decode::decode_frames(rbcf_data) {
										let mut pending_guard = pending.lock().await;
										if let Some(tx) = pending_guard.remove(&id) {
											let _ = tx.send(ClientResponse::Frames(frames));
										}
									}
								}
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

	/// Compute the format field for requests based on encoding.
	fn rbcf_format(&self) -> Option<String> {
		if self.encoding == Encoding::Rbcf {
			Some("rbcf".to_string())
		} else {
			None
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
	pub async fn admin(&self, rql: &str, params: Option<Params>) -> Result<AdminResult, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Admin(AdminRequest {
				statements: vec![rql.to_string()],
				params: params.and_then(params_to_wire),
				format: self.rbcf_format(),
			}),
		};

		match self.send_request(request).await? {
			ClientResponse::Frames(frames) => Ok(AdminResult {
				frames,
			}),
			ClientResponse::Json(resp) => parse_admin_response(resp),
		}
	}

	/// Execute multiple admin statements in a batch.
	pub async fn admin_batch(&self, statements: Vec<&str>, params: Option<Params>) -> Result<AdminResult, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Admin(AdminRequest {
				statements: statements.into_iter().map(String::from).collect(),
				params: params.and_then(params_to_wire),
				format: self.rbcf_format(),
			}),
		};

		match self.send_request(request).await? {
			ClientResponse::Frames(frames) => Ok(AdminResult {
				frames,
			}),
			ClientResponse::Json(resp) => parse_admin_response(resp),
		}
	}

	/// Execute a command (write) statement.
	pub async fn command(&self, rql: &str, params: Option<Params>) -> Result<CommandResult, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Command(CommandRequest {
				statements: vec![rql.to_string()],
				params: params.and_then(params_to_wire),
				format: self.rbcf_format(),
			}),
		};

		match self.send_request(request).await? {
			ClientResponse::Frames(frames) => Ok(CommandResult {
				frames,
			}),
			ClientResponse::Json(resp) => parse_command_response(resp),
		}
	}

	/// Execute a query (read) statement.
	pub async fn query(&self, rql: &str, params: Option<Params>) -> Result<QueryResult, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Query(QueryRequest {
				statements: vec![rql.to_string()],
				params: params.and_then(params_to_wire),
				format: self.rbcf_format(),
			}),
		};

		match self.send_request(request).await? {
			ClientResponse::Frames(frames) => Ok(QueryResult {
				frames,
			}),
			ClientResponse::Json(resp) => parse_query_response(resp),
		}
	}

	/// Execute multiple command statements in a batch.
	pub async fn command_batch(
		&self,
		statements: Vec<&str>,
		params: Option<Params>,
	) -> Result<CommandResult, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Command(CommandRequest {
				statements: statements.into_iter().map(String::from).collect(),
				params: params.and_then(params_to_wire),
				format: self.rbcf_format(),
			}),
		};

		match self.send_request(request).await? {
			ClientResponse::Frames(frames) => Ok(CommandResult {
				frames,
			}),
			ClientResponse::Json(resp) => parse_command_response(resp),
		}
	}

	/// Execute multiple query statements in a batch.
	pub async fn query_batch(&self, statements: Vec<&str>, params: Option<Params>) -> Result<QueryResult, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Query(QueryRequest {
				statements: statements.into_iter().map(String::from).collect(),
				params: params.and_then(params_to_wire),
				format: self.rbcf_format(),
			}),
		};

		match self.send_request(request).await? {
			ClientResponse::Frames(frames) => Ok(QueryResult {
				frames,
			}),
			ClientResponse::Json(resp) => parse_query_response(resp),
		}
	}

	/// Subscribe to real-time changes for a query.
	pub async fn subscribe(&self, query: &str) -> Result<String, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Subscribe(SubscribeRequest {
				query: query.to_string(),
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
			ClientResponse::Json(resp) => Ok(resp),
			ClientResponse::Frames(_) => panic!("unexpected binary response"), /* FIXME better error
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
