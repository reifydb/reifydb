// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{
	sync::{Arc, Mutex},
	thread,
	time::Duration,
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

/// A callback-based HTTP session for asynchronous operations
pub struct HttpCallbackSession {
	client: Arc<HttpClient>,
	token: Option<String>,
	authenticated: Arc<Mutex<bool>>,
}

impl HttpCallbackSession {
	/// Create a new callback HTTP session
	pub fn new(
		host: &str,
		port: u16,
		token: Option<String>,
	) -> Result<Self, Error> {
		let client = HttpClient::new((host, port)).map_err(|e| {
			Error(internal(format!(
				"Failed to create client: {}",
				e
			)))
		})?;
		Self::from_client(client, token)
	}

	/// Create a new callback HTTP session from an existing client
	pub fn from_client(
		client: HttpClient,
		token: Option<String>,
	) -> Result<Self, Error> {
		let client = Arc::new(client);

		// Test connection
		if let Err(e) = client.test_connection() {
			return Err(Error(internal(format!(
				"Connection failed: {}",
				e
			))));
		}

		let session = Self {
			client,
			token: token.clone(),
			authenticated: Arc::new(Mutex::new(false)),
		};

		// Authenticate if token provided
		if token.is_some() {
			let auth_flag = session.authenticated.clone();
			let _ = session.authenticate(
				move |result| match result {
					Ok(_) => {
						*auth_flag.lock().unwrap() =
							true;
						println!(
							"HTTP Authentication successful"
						);
					}
					Err(e) => {
						eprintln!(
							"HTTP Authentication failed: {}",
							e
						);
					}
				},
			);
		}

		Ok(session)
	}

	/// Create from URL (e.g., "http://localhost:8080")
	pub fn from_url(
		url: &str,
		token: Option<String>,
	) -> Result<Self, Error> {
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

		let session = Self {
			client,
			token: token.clone(),
			authenticated: Arc::new(Mutex::new(false)),
		};

		// Authenticate if token provided
		if token.is_some() {
			let auth_flag = session.authenticated.clone();
			let _ = session.authenticate(
				move |result| match result {
					Ok(_) => {
						*auth_flag.lock().unwrap() =
							true;
						println!(
							"HTTP Authentication successful"
						);
					}
					Err(e) => {
						eprintln!(
							"HTTP Authentication failed: {}",
							e
						);
					}
				},
			);
		}

		Ok(session)
	}

	/// Set timeout for requests
	pub fn with_timeout(mut self, timeout: Duration) -> Self {
		self.client =
			Arc::new((*self.client).clone().with_timeout(timeout));
		self
	}

	/// Authenticate with callback
	fn authenticate<F>(&self, callback: F) -> Result<(), Error>
	where
		F: FnOnce(Result<(), String>) + Send + 'static,
	{
		if self.token.is_none() {
			callback(Ok(()));
			return Ok(());
		}

		// For HTTP, we'll just simulate authentication
		// In a real implementation, this might send an auth request to
		// /v1/auth
		thread::spawn(move || {
			// Simulate some async work
			thread::sleep(Duration::from_millis(100));
			callback(Ok(()));
		});

		Ok(())
	}

	/// Send a command with callback
	pub fn command<F>(
		&self,
		rql: &str,
		params: Option<Params>,
		callback: F,
	) -> Result<(), Error>
	where
		F: FnOnce(Result<CommandResult, Error>) + Send + 'static,
	{
		let request = CommandRequest {
			statements: vec![rql.to_string()],
			params,
		};

		let client = self.client.clone();

		thread::spawn(move || {
			let result =
				client.send_command(&request).map(|response| {
					CommandResult {
						frames:
							convert_execute_response(
								response,
							),
					}
				});

			callback(result);
		});

		Ok(())
	}

	/// Send a query with callback
	pub fn query<F>(
		&self,
		rql: &str,
		params: Option<Params>,
		callback: F,
	) -> Result<(), Error>
	where
		F: FnOnce(Result<QueryResult, Error>) + Send + 'static,
	{
		let request = QueryRequest {
			statements: vec![rql.to_string()],
			params,
		};

		let client = self.client.clone();

		thread::spawn(move || {
			let result =
				client.send_query(&request).map(|response| {
					QueryResult {
						frames: convert_query_response(
							response,
						),
					}
				});

			callback(result);
		});

		Ok(())
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
	) -> Result<(), Error> {
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
	) -> Result<(), Error> {
		self.query(rql, params, move |result| match result {
			Ok(data) => handler.on_success(data),
			Err(e) => handler.on_error(e.to_string()),
		})
	}
}
