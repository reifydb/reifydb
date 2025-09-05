// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{
	sync::{Arc, Mutex, mpsc},
	thread,
};

use crate::{
	Params,
	session::{CommandResult, QueryResult},
	ws::{
		client::ClientInner,
		session::{ChannelResponse, ChannelSession, ResponseMessage},
	},
};

/// A callback-based session for asynchronous operations
pub struct CallbackSession {
	channel_session: Arc<ChannelSession>,
	receiver: Arc<Mutex<mpsc::Receiver<ResponseMessage>>>,
	authenticated: Arc<Mutex<bool>>,
	worker_handle: Option<thread::JoinHandle<()>>,
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

		// Start a worker thread to process responses and invoke
		// callbacks
		let receiver_clone = receiver.clone();
		let auth_flag = authenticated.clone();
		let worker_handle =
			thread::spawn(move || {
				// If token provided, handle authentication
				// response
				if token.is_some() {
					if let Ok(msg) = receiver_clone
						.lock()
						.unwrap()
						.recv()
					{
						match msg.response {
						Ok(ChannelResponse::Auth { .. }) => {
							*auth_flag.lock().unwrap() = true;
							println!("Authentication successful");
						}
						Err(e) => {
							eprintln!("Authentication failed: {}", e);
						}
						_ => {}
					}
					}
				}
			});

		Ok(Self {
			channel_session,
			receiver,
			authenticated,
			worker_handle: Some(worker_handle),
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
		let request_id =
			self.channel_session.command(rql, params).map_err(
				|e| {
					reifydb_type::Error(reifydb_type::diagnostic::internal(
				format!("Failed to send command: {}", e)
			))
				},
			)?;

		// Spawn thread to wait for response and invoke callback
		let receiver = self.receiver.clone();
		let request_id_clone = request_id.clone();
		thread::spawn(move || {
			if let Ok(msg) = receiver.lock().unwrap().recv() {
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
		let request_id =
			self.channel_session.query(rql, params).map_err(
				|e| {
					reifydb_type::Error(reifydb_type::diagnostic::internal(
				format!("Failed to send query: {}", e)
			))
				},
			)?;

		// Spawn thread to wait for response and invoke callback
		let receiver = self.receiver.clone();
		let request_id_clone = request_id.clone();
		thread::spawn(move || {
			if let Ok(msg) = receiver.lock().unwrap().recv() {
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
		// Clean up the worker thread if it exists
		if let Some(handle) = self.worker_handle.take() {
			let _ = handle.join();
		}
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
