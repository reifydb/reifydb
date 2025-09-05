// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{sync::mpsc, time::Duration};

use reifydb_type::{Error, diagnostic::internal};

use crate::{
	Params,
	http::{
		client::HttpClient,
		session::{
			HttpChannelResponse, HttpChannelSession,
			HttpResponseMessage,
		},
	},
	session::{CommandResult, QueryResult},
};

/// A blocking HTTP session that waits for responses synchronously
pub struct HttpBlockingSession {
	channel_session: HttpChannelSession,
	receiver: mpsc::Receiver<HttpResponseMessage>,
	authenticated: bool,
	timeout: Duration,
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
		// Create a channel session and get the receiver
		let (channel_session, receiver) =
			HttpChannelSession::from_client(client, token.clone())?;

		let mut session = Self {
			channel_session,
			receiver,
			authenticated: false,
			timeout: Duration::from_secs(30),
		};

		// If token provided, wait for authentication response
		if token.is_some() {
			session.wait_for_auth()?;
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
		Self::from_client(client, token)
	}

	/// Wait for authentication response
	fn wait_for_auth(&mut self) -> Result<(), Error> {
		// Authentication was already initiated by
		// HttpChannelSession::from_client We just need to wait for
		// the response
		match self.receiver.recv_timeout(self.timeout) {
			Ok(msg) => match msg.response {
				Ok(HttpChannelResponse::Auth {
					..
				}) => {
					self.authenticated = true;
					Ok(())
				}
				Err(e) => Err(e),
				_ => Err(Error(internal(
					"Unexpected response type during authentication",
				))),
			},
			Err(_) => {
				Err(Error(internal("Authentication timeout")))
			}
		}
	}

	/// Set timeout for requests
	pub fn with_timeout(mut self, timeout: Duration) -> Self {
		self.timeout = timeout;
		self
	}

	/// Send a command and wait for response
	pub fn command(
		&mut self,
		rql: &str,
		params: Option<Params>,
	) -> Result<CommandResult, Error> {
		// Send command through channel session
		let request_id = self
			.channel_session
			.command(rql, params)
			.map_err(|e| {
				Error(internal(format!(
					"Failed to send command: {}",
					e
				)))
			})?;

		// Wait for response
		match self.receiver.recv_timeout(self.timeout) {
			Ok(msg) => {
				if msg.request_id != request_id {
					return Err(Error(internal(
						"Received response for wrong request ID",
					)));
				}
				match msg.response {
					Ok(HttpChannelResponse::Command {
						result,
						..
					}) => Ok(result),
					Err(e) => Err(e),
					_ => Err(Error(internal(
						"Unexpected response type for command",
					))),
				}
			}
			Err(_) => Err(Error(internal("Command timeout"))),
		}
	}

	/// Send a query and wait for response
	pub fn query(
		&mut self,
		rql: &str,
		params: Option<Params>,
	) -> Result<QueryResult, Error> {
		// Send query through channel session
		let request_id = self
			.channel_session
			.query(rql, params)
			.map_err(|e| {
				Error(internal(format!(
					"Failed to send query: {}",
					e
				)))
			})?;

		// Wait for response
		match self.receiver.recv_timeout(self.timeout) {
			Ok(msg) => {
				if msg.request_id != request_id {
					return Err(Error(internal(
						"Received response for wrong request ID",
					)));
				}
				match msg.response {
					Ok(HttpChannelResponse::Query {
						result,
						..
					}) => Ok(result),
					Err(e) => Err(e),
					_ => Err(Error(internal(
						"Unexpected response type for query",
					))),
				}
			}
			Err(_) => Err(Error(internal("Query timeout"))),
		}
	}

	/// Check if the session is authenticated
	pub fn is_authenticated(&self) -> bool {
		self.authenticated
	}
}
