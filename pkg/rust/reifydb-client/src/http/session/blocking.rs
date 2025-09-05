// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::time::Duration;

use reifydb_type::{Error, diagnostic::internal};

use crate::{
	CommandRequest, Params, QueryRequest,
	http::client::HttpClient,
	session::{
		CommandResult, QueryResult, convert_execute_response,
		convert_query_response,
	},
};

/// A blocking HTTP session that waits for responses synchronously
pub struct HttpBlockingSession {
	client: HttpClient,
	token: Option<String>,
	authenticated: bool,
}

impl HttpBlockingSession {
	/// Create a new blocking HTTP session
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

	/// Create a new blocking HTTP session from an existing client
	pub fn from_client(
		client: HttpClient,
		token: Option<String>,
	) -> Result<Self, Error> {
		// Test connection
		if let Err(e) = client.test_connection() {
			return Err(Error(internal(format!(
				"Connection failed: {}",
				e
			))));
		}

		let mut session = Self {
			client,
			token: token.clone(),
			authenticated: false,
		};

		// Authenticate if token provided
		if token.is_some() {
			session.authenticate()?;
		}

		Ok(session)
	}

	/// Create from URL (e.g., "http://localhost:8080")
	pub fn from_url(
		url: &str,
		token: Option<String>,
	) -> Result<Self, Error> {
		let client = HttpClient::from_url(url).map_err(|e| {
			Error(internal(format!("Invalid URL: {}", e)))
		})?;

		// Test connection
		if let Err(e) = client.test_connection() {
			return Err(Error(internal(format!(
				"Connection failed: {}",
				e
			))));
		}

		let mut session = Self {
			client,
			token: token.clone(),
			authenticated: false,
		};

		// Authenticate if token provided
		if token.is_some() {
			session.authenticate()?;
		}

		Ok(session)
	}

	/// Set timeout for requests
	pub fn with_timeout(mut self, timeout: Duration) -> Self {
		self.client = self.client.with_timeout(timeout);
		self
	}

	/// Authenticate the session
	fn authenticate(&mut self) -> Result<(), Error> {
		if self.token.is_none() {
			return Ok(());
		}

		// For HTTP, we'll handle authentication per-request basis or
		// via headers For now, we'll just mark as authenticated if
		// token is provided In a real implementation, this might send
		// an auth request to /v1/auth
		self.authenticated = true;
		Ok(())
	}

	/// Send a command and wait for response
	pub fn command(
		&mut self,
		rql: &str,
		params: Option<Params>,
	) -> Result<CommandResult, Error> {
		let request = CommandRequest {
			statements: vec![rql.to_string()],
			params,
		};

		let response = self.client.send_command(&request)?;

		Ok(CommandResult {
			frames: convert_execute_response(response),
		})
	}

	/// Send a query and wait for response
	pub fn query(
		&mut self,
		rql: &str,
		params: Option<Params>,
	) -> Result<QueryResult, Error> {
		let request = QueryRequest {
			statements: vec![rql.to_string()],
			params,
		};

		let response = self.client.send_query(&request)?;

		Ok(QueryResult {
			frames: convert_query_response(response),
		})
	}

	/// Check if the session is authenticated
	pub fn is_authenticated(&self) -> bool {
		self.authenticated
	}
}
