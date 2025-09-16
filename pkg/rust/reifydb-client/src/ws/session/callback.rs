// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{
	sync::{mpsc, Arc, Mutex},
	thread,
	time::Duration,
};

use crate::{
	session::{CommandResult, QueryResult},
	ws::{
		client::ClientInner,
		session::{ChannelResponse, ChannelSession, ResponseMessage},
	},
	Params,
};

/// A callback-based session for asynchronous operations
pub struct CallbackSession {
	channel_session: Arc<ChannelSession>,
	receiver: Arc<Mutex<mpsc::Receiver<ResponseMessage>>>,
	authenticated: Arc<Mutex<bool>>,
}

impl CallbackSession {
	/// Create a new callback session
	pub(crate) fn new(
		client: Arc<ClientInner>,
		token: Option<String>,
	) -> Result<Self, reifydb_type::Error> {
		// Create a channel session and get the receiver
		let (channel_session, receiver) =
			ChannelSession::new(client, token.clone())?;

		let channel_session = Arc::new(channel_session);
		let receiver = Arc::new(Mutex::new(receiver));
		let authenticated = Arc::new(Mutex::new(false));

		// If token provided, consume the authentication response
		if token.is_some() {
			// Try to receive the auth response with a short timeout
			match receiver
				.lock()
				.unwrap()
				.recv_timeout(Duration::from_millis(500))
			{
				Ok(msg) => {
					match msg.response {
						Ok(ChannelResponse::Auth {
							..
						}) => {
							*authenticated
								.lock()
								.unwrap() = true;
							println!(
								"WebSocket Authentication successful"
							);
						}
						Err(e) => {
							// Authentication
							// failed, but we'll
							// continue anyway
							eprintln!(
								"WebSocket Authentication error (continuing anyway): {}",
								e
							);
							*authenticated
								.lock()
								.unwrap() = true;
						}
						_ => {
							// Not an auth response
							// - this shouldn't
							// happen
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
					println!(
						"WebSocket session created with token (no auth response received)"
					);
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

	/// Send a command with callback
	pub fn command<F>(
		&self,
		rql: &str,
		params: Option<Params>,
		callback: F,
	) -> Result<String, reifydb_type::Error>
	where
		F: FnOnce(Result<CommandResult, reifydb_type::Error>)
			+ Send
			+ 'static,
	{
		// Send command through channel session
		let request_id = self
			.channel_session
			.command(rql, params)
			.map_err(|e| {
				reifydb_type::Error(reifydb_type::diagnostic::internal(
				format!("Failed to send command: {}", e)
			))
			})?;

		// Spawn thread to wait for response and invoke callback with
		// timeout
		let receiver = self.receiver.clone();
		let request_id_clone = request_id.clone();
		thread::spawn(move || {
			// Wait up to 30 seconds for response
			match receiver
				.lock()
				.unwrap()
				.recv_timeout(Duration::from_secs(30))
			{
				Ok(msg) => {
					if msg.request_id == request_id_clone {
						match msg.response {
							Ok(ChannelResponse::Command { result, .. }) => {
								callback(Ok(result));
							}
							Err(e) => {
								callback(Err(e));
							}
							_ => {
								callback(Err(reifydb_type::Error(reifydb_type::diagnostic::internal(
									"Unexpected response type for command".to_string()
								))));
							}
						}
					}
				}
				Err(mpsc::RecvTimeoutError::Timeout) => {
					callback(Err(reifydb_type::Error(reifydb_type::diagnostic::internal(
						"Command request timeout".to_string()
					))));
				}
				Err(mpsc::RecvTimeoutError::Disconnected) => {
					callback(Err(reifydb_type::Error(reifydb_type::diagnostic::internal(
						"Command channel disconnected".to_string()
					))));
				}
			}
		});

		Ok(request_id)
	}

	/// Send a query with callback
	pub fn query<F>(
		&self,
		rql: &str,
		params: Option<Params>,
		callback: F,
	) -> Result<String, reifydb_type::Error>
	where
		F: FnOnce(Result<QueryResult, reifydb_type::Error>)
			+ Send
			+ 'static,
	{
		// Send query through channel session
		let request_id = self
			.channel_session
			.query(rql, params)
			.map_err(|e| {
				reifydb_type::Error(reifydb_type::diagnostic::internal(
				format!("Failed to send query: {}", e)
			))
			})?;

		// Spawn thread to wait for response and invoke callback with
		// timeout
		let receiver = self.receiver.clone();
		let request_id_clone = request_id.clone();
		thread::spawn(move || {
			// Wait up to 30 seconds for response
			match receiver
				.lock()
				.unwrap()
				.recv_timeout(Duration::from_secs(30))
			{
				Ok(msg) => {
					if msg.request_id == request_id_clone {
						match msg.response {
							Ok(ChannelResponse::Query { result, .. }) => {
								callback(Ok(result));
							}
							Err(e) => {
								callback(Err(e));
							}
							_ => {
								callback(Err(reifydb_type::Error(reifydb_type::diagnostic::internal(
									"Unexpected response type for query".to_string()
								))));
							}
						}
					}
				}
				Err(mpsc::RecvTimeoutError::Timeout) => {
					callback(Err(reifydb_type::Error(reifydb_type::diagnostic::internal(
						"Query request timeout".to_string()
					))));
				}
				Err(mpsc::RecvTimeoutError::Disconnected) => {
					callback(Err(reifydb_type::Error(reifydb_type::diagnostic::internal(
						"Query channel disconnected".to_string()
					))));
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

impl Drop for CallbackSession {
	fn drop(&mut self) {
		// No cleanup needed since we don't have a worker thread anymore
	}
}

/// Trait for structured callback handling
pub trait ResponseHandler: Send {
	fn on_success(&mut self, result: CommandResult);
	fn on_error(&mut self, error: String);
	fn on_timeout(&mut self) {}
}

/// Trait for query response handling
pub trait QueryHandler: Send {
	fn on_success(&mut self, result: QueryResult);
	fn on_error(&mut self, error: String);
	fn on_timeout(&mut self) {}
}

impl CallbackSession {
	/// Execute command with a response handler
	pub fn command_with_handler(
		&self,
		rql: &str,
		params: Option<Params>,
		mut handler: impl ResponseHandler + 'static,
	) -> Result<String, reifydb_type::Error> {
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
		mut handler: impl QueryHandler + 'static,
	) -> Result<String, reifydb_type::Error> {
		self.query(rql, params, move |result| match result {
			Ok(data) => handler.on_success(data),
			Err(e) => handler.on_error(e.to_string()),
		})
	}
}
