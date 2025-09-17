// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{sync::mpsc, time::Instant};

use reifydb_type::{Error, diagnostic::internal};

use crate::{
	CommandRequest, Params, QueryRequest,
	http::{
		client::HttpClient,
		message::{HttpInternalMessage, HttpResponseRoute},
	},
	utils::generate_request_id,
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
		result: crate::session::CommandResult,
	},
	/// Query execution response with frames
	Query {
		request_id: String,
		result: crate::session::QueryResult,
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
	client: HttpClient,
	token: Option<String>,
	response_tx: mpsc::Sender<HttpResponseMessage>,
}

impl HttpChannelSession {
	/// Create a new channel HTTP session
	pub fn new(
		host: &str,
		port: u16,
		token: Option<String>,
	) -> Result<(Self, mpsc::Receiver<HttpResponseMessage>), Error> {
		let client = HttpClient::new((host, port))
			.map_err(|e| Error(internal(format!("Failed to create client: {}", e))))?;
		Self::from_client(client, token)
	}

	/// Create a new channel HTTP session from an existing client
	pub fn from_client(
		client: HttpClient,
		token: Option<String>,
	) -> Result<(Self, mpsc::Receiver<HttpResponseMessage>), Error> {
		let (tx, rx) = mpsc::channel();

		let session = Self {
			client,
			token: token.clone(),
			response_tx: tx,
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
		let client = HttpClient::from_url(url).map_err(|e| Error(internal(format!("Invalid URL: {}", e))))?;
		Self::from_client(client, token)
	}

	/// Authenticate (response arrives on channel)
	fn authenticate(&self) -> Result<String, Error> {
		if self.token.is_none() {
			return Ok(String::new());
		}

		let id = generate_request_id();

		// Send auth message to worker thread
		if let Err(e) = self.client.command_tx().send(HttpInternalMessage::Auth {
			id: id.clone(),
			_token: self.token.clone(),
			route: HttpResponseRoute::Channel(self.response_tx.clone()),
		}) {
			return Err(Error(internal(format!("Failed to send auth request: {}", e))));
		}

		Ok(id)
	}

	/// Send a command (response arrives on channel)
	pub fn command(&self, rql: &str, params: Option<Params>) -> Result<String, Box<dyn std::error::Error>> {
		let id = generate_request_id();

		let request = CommandRequest {
			statements: vec![rql.to_string()],
			params,
		};

		// Send command message to worker thread
		if let Err(e) = self.client.command_tx().send(HttpInternalMessage::Command {
			id: id.clone(),
			request,
			route: HttpResponseRoute::Channel(self.response_tx.clone()),
		}) {
			return Err(format!("Failed to send command request: {}", e).into());
		}

		Ok(id)
	}

	/// Send a query (response arrives on channel)
	pub fn query(&self, rql: &str, params: Option<Params>) -> Result<String, Box<dyn std::error::Error>> {
		let id = generate_request_id();

		let request = QueryRequest {
			statements: vec![rql.to_string()],
			params,
		};

		// Send query message to worker thread
		if let Err(e) = self.client.command_tx().send(HttpInternalMessage::Query {
			id: id.clone(),
			request,
			route: HttpResponseRoute::Channel(self.response_tx.clone()),
		}) {
			return Err(format!("Failed to send query request: {}", e).into());
		}

		Ok(id)
	}
}
