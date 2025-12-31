// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
use std::{collections::HashMap, sync::Arc};

use futures_util::{SinkExt, StreamExt};
use reifydb_type::{Error, Params, diagnostic};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
	AuthRequest, CommandRequest, QueryRequest, Request, RequestPayload, Response, ResponsePayload,
	session::{CommandResult, QueryResult, parse_command_response, parse_query_response},
	utils::generate_request_id,
};

type PendingRequests = Arc<Mutex<HashMap<String, oneshot::Sender<Response>>>>;

/// Async WebSocket client for ReifyDB
pub struct WsClient {
	request_tx: mpsc::Sender<(Request, oneshot::Sender<Response>)>,
	shutdown_tx: Option<mpsc::Sender<()>>,
	is_authenticated: bool,
}

impl WsClient {
	/// Create a new WebSocket client connected to the given URL.
	///
	/// # Arguments
	/// * `url` - WebSocket URL of the ReifyDB server (e.g., "ws://localhost:8090")
	///
	/// # Example
	/// ```no_run
	/// use reifydb_client::WsClient;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
	/// 	let client = WsClient::connect("ws://localhost:8090").await?;
	/// 	Ok(())
	/// }
	/// ```
	pub async fn connect(url: &str) -> Result<Self, Error> {
		let url = if !url.starts_with("ws://") && !url.starts_with("wss://") {
			format!("ws://{}", url)
		} else {
			url.to_string()
		};

		let (ws_stream, _) = connect_async(&url)
			.await
			.map_err(|e| Error(diagnostic::internal(format!("Failed to connect to WebSocket: {}", e))))?;

		let (write, read) = ws_stream.split();

		// Channel for sending requests
		let (request_tx, request_rx) = mpsc::channel::<(Request, oneshot::Sender<Response>)>(32);

		// Channel for shutdown signal
		let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);

		// Pending requests map
		let pending: PendingRequests = Arc::new(Mutex::new(HashMap::new()));

		// Spawn the connection management task
		let pending_clone = pending.clone();
		tokio::spawn(async move {
			Self::connection_loop(write, read, request_rx, shutdown_rx, pending_clone).await;
		});

		Ok(Self {
			request_tx,
			shutdown_tx: Some(shutdown_tx),
			is_authenticated: false,
		})
	}

	/// Connection management loop
	async fn connection_loop(
		mut write: futures_util::stream::SplitSink<
			tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
			Message,
		>,
		mut read: futures_util::stream::SplitStream<
			tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
		>,
		mut request_rx: mpsc::Receiver<(Request, oneshot::Sender<Response>)>,
		mut shutdown_rx: mpsc::Receiver<()>,
		pending: PendingRequests,
	) {
		loop {
			tokio::select! {
				// Handle incoming messages
				Some(msg) = read.next() => {
					match msg {
						Ok(Message::Text(text)) => {
							if let Ok(response) = serde_json::from_str::<Response>(&text) {
								let mut pending_guard = pending.lock().await;
								if let Some(tx) = pending_guard.remove(&response.id) {
									let _ = tx.send(response);
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
					if let Ok(json) = serde_json::to_string(&request) {
						if write.send(Message::Text(json.into())).await.is_err() {
							break;
						}
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

	/// Authenticate with the server.
	///
	/// # Arguments
	/// * `token` - Bearer token for authentication
	pub async fn authenticate(&mut self, token: &str) -> Result<(), Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Auth(AuthRequest {
				token: Some(token.to_string()),
			}),
		};

		let response = self.send_request(request).await?;

		match response.payload {
			ResponsePayload::Auth(_) => {
				self.is_authenticated = true;
				Ok(())
			}
			ResponsePayload::Err(err) => Err(Error(err.diagnostic)),
			_ => Err(Error(diagnostic::internal("Unexpected response type for auth"))),
		}
	}

	/// Execute a command (write) statement.
	///
	/// # Arguments
	/// * `rql` - RQL statement to execute
	/// * `params` - Optional parameters for the statement
	pub async fn command(&self, rql: &str, params: Option<Params>) -> Result<CommandResult, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Command(CommandRequest {
				statements: vec![rql.to_string()],
				params,
			}),
		};

		let response = self.send_request(request).await?;
		parse_command_response(response)
	}

	/// Execute a query (read) statement.
	///
	/// # Arguments
	/// * `rql` - RQL query to execute
	/// * `params` - Optional parameters for the query
	pub async fn query(&self, rql: &str, params: Option<Params>) -> Result<QueryResult, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Query(QueryRequest {
				statements: vec![rql.to_string()],
				params,
			}),
		};

		let response = self.send_request(request).await?;
		parse_query_response(response)
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
				params,
			}),
		};

		let response = self.send_request(request).await?;
		parse_command_response(response)
	}

	/// Execute multiple query statements in a batch.
	pub async fn query_batch(&self, statements: Vec<&str>, params: Option<Params>) -> Result<QueryResult, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Query(QueryRequest {
				statements: statements.into_iter().map(String::from).collect(),
				params,
			}),
		};

		let response = self.send_request(request).await?;
		parse_query_response(response)
	}

	/// Send a request and wait for the response.
	async fn send_request(&self, request: Request) -> Result<Response, Error> {
		let (tx, rx) = oneshot::channel();

		self.request_tx
			.send((request, tx))
			.await
			.map_err(|_| Error(diagnostic::internal("Connection closed")))?;

		rx.await.map_err(|_| Error(diagnostic::internal("Response channel closed")))
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
