// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{
	sync::{Arc, mpsc},
	thread,
	time::{Duration, Instant},
};

use reifydb_type::{Error, diagnostic::internal};

use crate::{
	CommandRequest, Params, QueryRequest,
	http::client::HttpClient,
	session::{
		CommandResult, QueryResult, convert_execute_response,
		convert_query_response,
	},
};

/// HTTP Channel response enum for different response types
#[derive(Debug)]
pub enum HttpChannelResponse {
	/// Authentication response
	Auth {
		request_id: String,
	},
	/// Command execution response with frames
	Command {
		request_id: String,
		result: CommandResult,
	},
	/// Query execution response with frames
	Query {
		request_id: String,
		result: QueryResult,
	},
}

/// HTTP Response message for channel sessions
#[derive(Debug)]
pub struct HttpResponseMessage {
	pub request_id: String,
	pub response: Result<HttpChannelResponse, Error>,
	pub timestamp: Instant,
}

/// A channel-based HTTP session for message-passing style communication
pub struct HttpChannelSession {
	client: Arc<HttpClient>,
	token: Option<String>,
	response_tx: mpsc::Sender<HttpResponseMessage>,
	request_counter: Arc<std::sync::atomic::AtomicU64>,
}

impl HttpChannelSession {
	/// Create a new channel HTTP session
	pub fn new(
		host: &str,
		port: u16,
		token: Option<String>,
	) -> Result<(Self, mpsc::Receiver<HttpResponseMessage>), Error> {
		let client = HttpClient::new((host, port)).map_err(|e| {
			Error(internal(format!(
				"Failed to create client: {}",
				e
			)))
		})?;
		Self::from_client(client, token)
	}

	/// Create a new channel HTTP session from an existing client
	pub fn from_client(
		client: HttpClient,
		token: Option<String>,
	) -> Result<(Self, mpsc::Receiver<HttpResponseMessage>), Error> {
		let client = Arc::new(client);

		// Test connection
		if let Err(e) = client.test_connection() {
			return Err(Error(internal(format!(
				"Connection failed: {}",
				e
			))));
		}

		let (tx, rx) = mpsc::channel();

		let session = Self {
			client,
			token: token.clone(),
			response_tx: tx,
			request_counter: Arc::new(
				std::sync::atomic::AtomicU64::new(0),
			),
		};

		// Authenticate if token provided
		if token.is_some() {
			let _ = session.authenticate();
		}

		Ok((session, rx))
	}

	/// Create from URL (e.g., "http://localhost:8080")
	pub fn from_url(
		url: &str,
		token: Option<String>,
	) -> Result<(Self, mpsc::Receiver<HttpResponseMessage>), Error> {
		let client =
			Arc::new(HttpClient::from_url(url).map_err(|e| {
				Error(internal(format!("Invalid URL: {}", e)))
			})?);

		// Test connection
		if let Err(e) = client.test_connection() {
			return Err(Error(internal(format!(
				"Connection failed: {}",
				e
			))));
		}

		let (tx, rx) = mpsc::channel();

		let session = Self {
			client,
			token: token.clone(),
			response_tx: tx,
			request_counter: Arc::new(
				std::sync::atomic::AtomicU64::new(0),
			),
		};

		// Authenticate if token provided
		if token.is_some() {
			let _ = session.authenticate();
		}

		Ok((session, rx))
	}

	/// Set timeout for requests
	pub fn with_timeout(mut self, timeout: Duration) -> Self {
		self.client =
			Arc::new((*self.client).clone().with_timeout(timeout));
		self
	}

	/// Generate a unique request ID
	fn next_request_id(&self) -> String {
		let counter = self
			.request_counter
			.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
		format!("http_req_{}", counter)
	}

	/// Authenticate (response arrives on channel)
	fn authenticate(&self) -> Result<String, Error> {
		if self.token.is_none() {
			return Ok(String::new());
		}

		let id = self.next_request_id();
		let return_id = id.clone();
		let tx = self.response_tx.clone();

		// For HTTP, we'll just simulate authentication
		// In a real implementation, this might send an auth request to
		// /v1/auth
		// Note: HTTP doesn't have persistent authentication like
		// WebSocket, but we simulate it for API consistency
		thread::spawn(move || {
			let message = HttpResponseMessage {
				request_id: id.clone(),
				response: Ok(HttpChannelResponse::Auth {
					request_id: id,
				}),
				timestamp: Instant::now(),
			};

			let _ = tx.send(message);
		});

		Ok(return_id)
	}

	/// Send a command (response arrives on channel)
	pub fn command(
		&self,
		rql: &str,
		params: Option<Params>,
	) -> Result<String, Box<dyn std::error::Error>> {
		let id = self.next_request_id();
		let return_id = id.clone();

		let request = CommandRequest {
			statements: vec![rql.to_string()],
			params,
		};

		let client = self.client.clone();
		let tx = self.response_tx.clone();
		let request_id = id;

		thread::spawn(move || {
			let timestamp = Instant::now();

			let response = match client.send_command(&request) {
				Ok(response) => Ok(HttpChannelResponse::Command {
					request_id: request_id.clone(),
					result: CommandResult {
						frames: convert_execute_response(response),
					},
				}),
				Err(e) => Err(e),
			};

			let message = HttpResponseMessage {
				request_id,
				response,
				timestamp,
			};

			let _ = tx.send(message);
		});

		Ok(return_id)
	}

	/// Send a query (response arrives on channel)
	pub fn query(
		&self,
		rql: &str,
		params: Option<Params>,
	) -> Result<String, Box<dyn std::error::Error>> {
		let id = self.next_request_id();
		let return_id = id.clone();

		let request = QueryRequest {
			statements: vec![rql.to_string()],
			params,
		};

		let client = self.client.clone();
		let tx = self.response_tx.clone();
		let request_id = id;

		thread::spawn(move || {
			let timestamp = Instant::now();

			let response = match client.send_query(&request) {
				Ok(response) => Ok(HttpChannelResponse::Query {
					request_id: request_id.clone(),
					result: QueryResult {
						frames: convert_query_response(response),
					},
				}),
				Err(e) => Err(e),
			};

			let message = HttpResponseMessage {
				request_id,
				response,
				timestamp,
			};

			let _ = tx.send(message);
		});

		Ok(return_id)
	}
}
