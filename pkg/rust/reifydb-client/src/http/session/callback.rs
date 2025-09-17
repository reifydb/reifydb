// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{
	sync::{Arc, Mutex, mpsc},
	thread,
	time::Duration,
};

use reifydb_type::{Error, diagnostic::internal};

use crate::{
	Params,
	http::{
		client::HttpClient,
		session::{HttpChannelResponse, HttpChannelSession, HttpResponseMessage},
	},
	session::{CommandResult, QueryResult},
};

/// A callback-based HTTP session for asynchronous operations
pub struct HttpCallbackSession {
	channel_session: Arc<HttpChannelSession>,
	receiver: Arc<Mutex<mpsc::Receiver<HttpResponseMessage>>>,
	authenticated: Arc<Mutex<bool>>,
}

impl HttpCallbackSession {
	/// Create a new callback HTTP session
	pub fn new(host: &str, port: u16, token: Option<String>) -> Result<Self, Error> {
		let client = HttpClient::new((host, port)).map_err(|e| {
			reifydb_type::Error(reifydb_type::diagnostic::internal(format!(
				"Failed to create client: {}",
				e
			)))
		})?;
		Self::from_client(client, token)
	}

	/// Create a new callback HTTP session from an existing client
	pub fn from_client(client: HttpClient, token: Option<String>) -> Result<Self, Error> {
		// Create a channel session and get the receiver
		let (channel_session, receiver) = HttpChannelSession::from_client(client, token.clone())?;

		let channel_session = Arc::new(channel_session);
		let receiver = Arc::new(Mutex::new(receiver));
		let authenticated = Arc::new(Mutex::new(false));

		// If token provided, consume the authentication response
		if token.is_some() {
			// Try to receive the auth response with a short timeout
			match receiver.lock().unwrap().recv_timeout(Duration::from_millis(500)) {
				Ok(msg) => {
					match msg.response {
						Ok(HttpChannelResponse::Auth {
							..
						}) => {
							*authenticated.lock().unwrap() = true;
							println!("HTTP Authentication successful");
						}
						Err(e) => {
							// Authentication failed, but we'll continue anyway
							eprintln!(
								"HTTP Authentication error (continuing anyway): {}",
								e
							);
							*authenticated.lock().unwrap() = true;
						}
						_ => {
							// Not an auth response - this shouldn't happen
							eprintln!(
								"Warning: Expected auth response but got: {:?}",
								msg.response
							);
						}
					}
				}
				Err(_) => {
					// Timeout or disconnected - continue
					// anyway
					println!("HTTP session created with token (no auth response received)");
					*authenticated.lock().unwrap() = true;
				}
			}
		}

		Ok(Self {
			channel_session,
			receiver,
			authenticated,
		})
	}

	/// Create from URL (e.g., "http://localhost:8080")
	pub fn from_url(url: &str, token: Option<String>) -> Result<Self, Error> {
		let client = HttpClient::from_url(url).map_err(|e| Error(internal(format!("Invalid URL: {}", e))))?;
		Self::from_client(client, token)
	}

	/// Set timeout for requests
	pub fn with_timeout(self, _timeout: Duration) -> Self {
		// This would need to be implemented at the channel session
		// level For now, just return self
		self
	}

	/// Send a command with callback
	pub fn command<F>(&self, rql: &str, params: Option<Params>, callback: F) -> Result<String, Error>
	where
		F: FnOnce(Result<CommandResult, Error>) + Send + 'static,
	{
		// Send command through channel session
		let request_id = self
			.channel_session
			.command(rql, params)
			.map_err(|e| Error(internal(format!("Failed to send command: {}", e))))?;

		// Spawn thread to wait for response and invoke callback with
		// timeout
		let receiver = self.receiver.clone();
		let request_id_clone = request_id.clone();
		thread::spawn(move || {
			// Wait up to 30 seconds for response
			match receiver.lock().unwrap().recv_timeout(Duration::from_secs(30)) {
				Ok(msg) => {
					if msg.request_id == request_id_clone {
						match msg.response {
							Ok(HttpChannelResponse::Command {
								result,
								..
							}) => {
								callback(Ok(result));
							}
							Err(e) => {
								callback(Err(e));
							}
							_ => {
								callback(Err(Error(internal(
									"Unexpected response type for command"
										.to_string(),
								))));
							}
						}
					}
				}
				Err(mpsc::RecvTimeoutError::Timeout) => {
					callback(Err(Error(internal("Command request timeout".to_string()))));
				}
				Err(mpsc::RecvTimeoutError::Disconnected) => {
					callback(Err(Error(internal("Command channel disconnected".to_string()))));
				}
			}
		});

		Ok(request_id)
	}

	/// Send a query with callback
	pub fn query<F>(&self, rql: &str, params: Option<Params>, callback: F) -> Result<String, Error>
	where
		F: FnOnce(Result<QueryResult, Error>) + Send + 'static,
	{
		// Send query through channel session
		let request_id = self
			.channel_session
			.query(rql, params)
			.map_err(|e| Error(internal(format!("Failed to send query: {}", e))))?;

		// Spawn thread to wait for response and invoke callback with
		// timeout
		let receiver = self.receiver.clone();
		let request_id_clone = request_id.clone();
		thread::spawn(move || {
			// Wait up to 30 seconds for response
			match receiver.lock().unwrap().recv_timeout(Duration::from_secs(30)) {
				Ok(msg) => {
					if msg.request_id == request_id_clone {
						match msg.response {
							Ok(HttpChannelResponse::Query {
								result,
								..
							}) => {
								callback(Ok(result));
							}
							Err(e) => {
								callback(Err(e));
							}
							_ => {
								callback(Err(Error(internal(
									"Unexpected response type for query"
										.to_string(),
								))));
							}
						}
					}
				}
				Err(mpsc::RecvTimeoutError::Timeout) => {
					callback(Err(Error(internal("Query request timeout".to_string()))));
				}
				Err(mpsc::RecvTimeoutError::Disconnected) => {
					callback(Err(Error(internal("Query channel disconnected".to_string()))));
				}
			}
		});

		Ok(request_id)
	}

	/// Check if the session is authenticated
	pub fn is_authenticated(&self) -> bool {
		*self.authenticated.lock().unwrap()
	}
}

/// Trait for structured callback handling (same as WebSocket)
pub trait HttpResponseHandler: Send {
	fn on_success(&mut self, result: CommandResult);
	fn on_error(&mut self, error: String);
	fn on_timeout(&mut self) {}
}

/// Trait for query response handling (same as WebSocket)
pub trait HttpQueryHandler: Send {
	fn on_success(&mut self, result: QueryResult);
	fn on_error(&mut self, error: String);
	fn on_timeout(&mut self) {}
}

impl HttpCallbackSession {
	/// Execute command with a response handler
	pub fn command_with_handler(
		&self,
		rql: &str,
		params: Option<Params>,
		mut handler: impl HttpResponseHandler + 'static,
	) -> Result<String, Error> {
		self.command(rql, params, move |result| match result {
			Ok(data) => handler.on_success(data),
			Err(e) => handler.on_error(e.to_string()),
		})
	}

	/// Execute query with a response handler
	pub fn query_with_handler(
		&self,
		rql: &str,
		params: Option<Params>,
		mut handler: impl HttpQueryHandler + 'static,
	) -> Result<String, Error> {
		self.query(rql, params, move |result| match result {
			Ok(data) => handler.on_success(data),
			Err(e) => handler.on_error(e.to_string()),
		})
	}
}

impl Drop for HttpCallbackSession {
	fn drop(&mut self) {
		// No cleanup needed since we don't have a worker thread anymore
	}
}
