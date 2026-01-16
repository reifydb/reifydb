// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
use std::{collections::HashMap, sync::Arc};

use futures_util::{SinkExt, StreamExt};
use reifydb_type::{
	error::{Error, diagnostic, diagnostic::internal::internal},
	params::Params,
};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
	AuthRequest, ChangePayload, CommandRequest, QueryRequest, Request, RequestPayload, Response, ResponsePayload,
	ServerPush, SubscribeRequest, UnsubscribeRequest,
	session::{CommandResult, QueryResult, parse_command_response, parse_query_response},
	utils::generate_request_id,
};

type PendingRequests = Arc<Mutex<HashMap<String, oneshot::Sender<Response>>>>;

/// Async WebSocket client for ReifyDB
pub struct WsClient {
	request_tx: mpsc::Sender<(Request, oneshot::Sender<Response>)>,
	shutdown_tx: Option<mpsc::Sender<()>>,
	is_authenticated: bool,
	/// Channel for receiving server-initiated Change messages.
	change_rx: mpsc::Receiver<ChangePayload>,
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

		let (ws_stream, _) = connect_async(&url).await.map_err(|e| {
			Error(diagnostic::internal::internal(format!("Failed to connect to WebSocket: {}", e)))
		})?;

		let (write, read) = ws_stream.split();

		// Channel for sending requests
		let (request_tx, request_rx) = mpsc::channel::<(Request, oneshot::Sender<Response>)>(32);

		// Channel for shutdown signal
		let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);

		// Channel for receiving server-initiated Change messages
		let (change_tx, change_rx) = mpsc::channel::<ChangePayload>(100);

		// Pending requests map
		let pending: PendingRequests = Arc::new(Mutex::new(HashMap::new()));

		// Spawn the connection management task
		let pending_clone = pending.clone();
		tokio::spawn(async move {
			Self::connection_loop(write, read, request_rx, shutdown_rx, pending_clone, change_tx).await;
		});

		Ok(Self {
			request_tx,
			shutdown_tx: Some(shutdown_tx),
			is_authenticated: false,
			change_rx,
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
		change_tx: mpsc::Sender<ChangePayload>,
	) {
		loop {
			tokio::select! {
				// Handle incoming messages
				Some(msg) = read.next() => {
					match msg {
						Ok(Message::Text(text)) => {
							// First try to parse as Response (has id field)
							if let Ok(response) = serde_json::from_str::<Response>(&text) {
								let mut pending_guard = pending.lock().await;
								if let Some(tx) = pending_guard.remove(&response.id) {
									let _ = tx.send(response);
								}
							}
							// Then try to parse as ServerPush (no id field)
							else if let Ok(push) = serde_json::from_str::<ServerPush>(&text) {
								match push {
									ServerPush::Change(change) => {
										let _ = change_tx.send(change).await;
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
			_ => Err(Error(internal("Unexpected response type for auth"))),
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

	/// Subscribe to real-time changes for a query.
	///
	/// Returns the subscription ID. Use `recv()` or `try_recv()` to receive
	/// Change messages from the server.
	///
	/// # Arguments
	/// * `query` - RQL query to subscribe to
	pub async fn subscribe(&self, query: &str) -> Result<String, Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Subscribe(SubscribeRequest {
				query: query.to_string(),
			}),
		};

		let response = self.send_request(request).await?;
		match response.payload {
			ResponsePayload::Subscribed(sub) => Ok(sub.subscription_id),
			ResponsePayload::Err(err) => Err(Error(err.diagnostic)),
			_ => Err(Error(internal("Unexpected response type for subscribe"))),
		}
	}

	/// Unsubscribe from a subscription.
	///
	/// # Arguments
	/// * `subscription_id` - The subscription ID returned from `subscribe()`
	pub async fn unsubscribe(&self, subscription_id: &str) -> Result<(), Error> {
		let id = generate_request_id();
		let request = Request {
			id,
			payload: RequestPayload::Unsubscribe(UnsubscribeRequest {
				subscription_id: subscription_id.to_string(),
			}),
		};

		let response = self.send_request(request).await?;
		match response.payload {
			ResponsePayload::Unsubscribed(_) => Ok(()),
			ResponsePayload::Err(err) => Err(Error(err.diagnostic)),
			_ => Err(Error(internal("Unexpected response type for unsubscribe"))),
		}
	}

	/// Receive the next change notification, waiting if necessary.
	///
	/// Returns `None` if the connection is closed.
	pub async fn recv(&mut self) -> Option<ChangePayload> {
		self.change_rx.recv().await
	}

	/// Try to receive a change notification without blocking.
	///
	/// Returns `Ok(payload)` if a change is available, or an error if the
	/// channel is empty or disconnected.
	pub fn try_recv(&mut self) -> Result<ChangePayload, mpsc::error::TryRecvError> {
		self.change_rx.try_recv()
	}

	/// Send a request and wait for the response.
	async fn send_request(&self, request: Request) -> Result<Response, Error> {
		let (tx, rx) = oneshot::channel();

		self.request_tx.send((request, tx)).await.map_err(|_| Error(internal("Connection closed")))?;

		rx.await.map_err(|_| Error(internal("Response channel closed")))
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
