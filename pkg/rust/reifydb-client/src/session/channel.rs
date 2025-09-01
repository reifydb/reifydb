// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{
	sync::{Arc, mpsc},
	time::Instant,
};

use super::ResponseMessage;
use crate::{
	AuthRequest, CommandRequest, Params, QueryRequest, Request,
	RequestPayload, Response,
	client::{
		ClientInner, InternalMessage, ResponseRoute,
		generate_request_id,
	},
};

/// A channel-based session for message-passing style communication
pub struct ChannelSession {
	client: Arc<ClientInner>,
	token: Option<String>,
	response_tx: mpsc::Sender<ResponseMessage>,
}

impl ChannelSession {
	/// Create a new channel session
	pub(crate) fn new(
		client: Arc<ClientInner>,
		token: Option<String>,
	) -> Result<
		(Self, mpsc::Receiver<ResponseMessage>),
		Box<dyn std::error::Error>,
	> {
		let (tx, rx) = mpsc::channel();

		let session = Self {
			client: client.clone(),
			token: token.clone(),
			response_tx: tx,
		};

		// Authenticate if token provided
		if token.is_some() {
			session.authenticate()?;
		}

		Ok((session, rx))
	}

	/// Authenticate (response arrives on channel)
	fn authenticate(&self) -> Result<String, Box<dyn std::error::Error>> {
		if self.token.is_none() {
			return Ok(String::new());
		}

		let id = generate_request_id();

		let request = Request {
			id: id.clone(),
			payload: RequestPayload::Auth(AuthRequest {
				token: self.token.clone(),
			}),
		};

		self.client.command_tx.send(InternalMessage::Request {
			id: id.clone(),
			request,
			route: ResponseRoute::Channel(self.response_tx.clone()),
		})?;

		Ok(id)
	}

	/// Send a command (response arrives on channel)
	pub fn command(
		&self,
		rql: &str,
		params: Option<Params>,
	) -> Result<String, Box<dyn std::error::Error>> {
		let id = generate_request_id();

		let request = Request {
			id: id.clone(),
			payload: RequestPayload::Command(CommandRequest {
				statements: vec![rql.to_string()],
				params,
			}),
		};

		self.client.command_tx.send(InternalMessage::Request {
			id: id.clone(),
			request,
			route: ResponseRoute::Channel(self.response_tx.clone()),
		})?;

		Ok(id)
	}

	/// Send a query (response arrives on channel)
	pub fn query(
		&self,
		rql: &str,
		params: Option<Params>,
	) -> Result<String, Box<dyn std::error::Error>> {
		let id = generate_request_id();

		let request = Request {
			id: id.clone(),
			payload: RequestPayload::Query(QueryRequest {
				statements: vec![rql.to_string()],
				params,
			}),
		};

		self.client.command_tx.send(InternalMessage::Request {
			id: id.clone(),
			request,
			route: ResponseRoute::Channel(self.response_tx.clone()),
		})?;

		Ok(id)
	}

	/// Send multiple commands (response arrives on channel)
	pub fn command_batch(
		&self,
		statements: Vec<&str>,
	) -> Result<String, Box<dyn std::error::Error>> {
		let id = generate_request_id();

		let request = Request {
			id: id.clone(),
			payload: RequestPayload::Command(CommandRequest {
				statements: statements
					.iter()
					.map(|s| s.to_string())
					.collect(),
				params: None,
			}),
		};

		self.client.command_tx.send(InternalMessage::Request {
			id: id.clone(),
			request,
			route: ResponseRoute::Channel(self.response_tx.clone()),
		})?;

		Ok(id)
	}

	/// Send multiple queries (response arrives on channel)
	pub fn query_batch(
		&self,
		statements: Vec<&str>,
	) -> Result<String, Box<dyn std::error::Error>> {
		let id = generate_request_id();

		let request = Request {
			id: id.clone(),
			payload: RequestPayload::Query(QueryRequest {
				statements: statements
					.iter()
					.map(|s| s.to_string())
					.collect(),
				params: None,
			}),
		};

		self.client.command_tx.send(InternalMessage::Request {
			id: id.clone(),
			request,
			route: ResponseRoute::Channel(self.response_tx.clone()),
		})?;

		Ok(id)
	}

	/// Send a command with positional parameters
	pub fn command_with_params(
		&self,
		rql: &str,
		params: Vec<crate::Value>,
	) -> Result<String, Box<dyn std::error::Error>> {
		self.command(rql, Some(Params::Positional(params)))
	}

	/// Send a query with positional parameters
	pub fn query_with_params(
		&self,
		rql: &str,
		params: Vec<crate::Value>,
	) -> Result<String, Box<dyn std::error::Error>> {
		self.query(rql, Some(Params::Positional(params)))
	}

	/// Send a command with named parameters
	pub fn command_with_named_params(
		&self,
		rql: &str,
		params: std::collections::HashMap<String, crate::Value>,
	) -> Result<String, Box<dyn std::error::Error>> {
		self.command(rql, Some(Params::Named(params)))
	}

	/// Send a query with named parameters
	pub fn query_with_named_params(
		&self,
		rql: &str,
		params: std::collections::HashMap<String, crate::Value>,
	) -> Result<String, Box<dyn std::error::Error>> {
		self.query(rql, Some(Params::Named(params)))
	}
}

/// Helper methods for working with channel responses
impl ChannelSession {
	/// Helper to receive with timeout
	pub fn recv_timeout(
		rx: &mpsc::Receiver<ResponseMessage>,
		timeout: std::time::Duration,
	) -> Result<ResponseMessage, Box<dyn std::error::Error>> {
		rx.recv_timeout(timeout).map_err(|e| {
			format!("Channel receive error: {}", e).into()
		})
	}

	/// Helper to try receive without blocking
	pub fn try_recv(
		rx: &mpsc::Receiver<ResponseMessage>,
	) -> Option<ResponseMessage> {
		rx.try_recv().ok()
	}

	/// Helper to wait for a specific response by ID
	pub fn wait_for_response(
		rx: &mpsc::Receiver<ResponseMessage>,
		expected_id: &str,
		timeout: std::time::Duration,
	) -> Result<Response, Box<dyn std::error::Error>> {
		let deadline = Instant::now() + timeout;

		loop {
			let remaining = deadline
				.saturating_duration_since(Instant::now());
			if remaining.is_zero() {
				return Err(
					"Timeout waiting for response".into()
				);
			}

			match rx.recv_timeout(remaining) {
				Ok(msg) if msg.request_id == expected_id => {
					return msg
						.response
						.map_err(|e| e.into());
				}
				Ok(_) => continue, // Not our response, keep
				// waiting
				Err(mpsc::RecvTimeoutError::Timeout) => {
					return Err(
						"Timeout waiting for response"
							.into(),
					);
				}
				Err(e) => {
					return Err(format!(
						"Channel error: {}",
						e
					)
					.into());
				}
			}
		}
	}
}
