// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{
	error::Error,
	sync::{Arc, mpsc},
	time::Duration,
};

use super::{
	CommandResult, QueryResult, parse_command_response,
	parse_query_response,
};
use crate::{
	AuthRequest, CommandRequest, Params, QueryRequest, Request,
	RequestPayload,
	client::{
		ClientInner, InternalMessage, ResponseRoute,
		generate_request_id,
	},
};

/// A blocking session that waits for responses synchronously
pub struct BlockingSession {
	client: Arc<ClientInner>,
	token: Option<String>,
	authenticated: bool,
	timeout: Duration,
}

impl BlockingSession {
	/// Create a new blocking session
	pub(crate) fn new(
		client: Arc<ClientInner>,
		token: Option<String>,
	) -> Result<Self, Box<dyn Error>> {
		let mut session = Self {
			client,
			token: token.clone(),
			authenticated: false,
			timeout: Duration::from_secs(30),
		};

		// Authenticate if token provided
		if token.is_some() {
			session.authenticate()?;
		}

		Ok(session)
	}

	/// Authenticate the session
	fn authenticate(&mut self) -> Result<(), Box<dyn Error>> {
		if self.token.is_none() {
			return Ok(());
		}

		let id = generate_request_id();
		let (tx, rx) = mpsc::channel();

		let request = Request {
			id: id.clone(),
			payload: RequestPayload::Auth(AuthRequest {
				token: self.token.clone(),
			}),
		};

		self.client.command_tx.send(InternalMessage::Request {
			id: id.clone(),
			request,
			route: ResponseRoute::Blocking(tx),
		})?;

		let response = rx
			.recv_timeout(self.timeout)
			.map_err(|_| "Authentication timeout")?
			.map_err(|e| format!("Authentication failed: {}", e))?;

		match response.payload {
            crate::ResponsePayload::Auth(_) => {
                self.authenticated = true;
                Ok(())
            }
            crate::ResponsePayload::Err(e) => {
                Err(format!("Authentication failed: {:?}", e).into())
            }
            _ => Err("Unexpected response type during authentication".into()),
        }
	}

	/// Send a command and wait for response
	pub fn command(
		&mut self,
		rql: &str,
		params: Option<Params>,
	) -> Result<CommandResult, Box<dyn Error>> {
		let id = generate_request_id();
		let (tx, rx) = mpsc::channel();

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
			route: ResponseRoute::Blocking(tx),
		})?;

		let response = rx
			.recv_timeout(self.timeout)
			.map_err(|_| "Command timeout")?
			.map_err(|e| format!("Command failed: {}", e))?;

		parse_command_response(response).map_err(|e| e.into())
	}

	/// Send a query and wait for response
	pub fn query(
		&mut self,
		rql: &str,
		params: Option<Params>,
	) -> Result<QueryResult, Box<dyn Error>> {
		let id = generate_request_id();
		let (tx, rx) = mpsc::channel();

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
			route: ResponseRoute::Blocking(tx),
		})?;

		let response = rx
			.recv_timeout(self.timeout)
			.map_err(|_| "Query timeout")?
			.map_err(|e| format!("Query failed: {}", e))?;

		parse_query_response(response).map_err(|e| e.into())
	}

	/// Send multiple commands in a batch
	pub fn command_batch(
		&mut self,
		statements: Vec<&str>,
	) -> Result<CommandResult, Box<dyn Error>> {
		let id = generate_request_id();
		let (tx, rx) = mpsc::channel();

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
			route: ResponseRoute::Blocking(tx),
		})?;

		let response = rx
			.recv_timeout(self.timeout)
			.map_err(|_| "Command batch timeout")?
			.map_err(|e| format!("Command batch failed: {}", e))?;

		parse_command_response(response).map_err(|e| e.into())
	}

	/// Send multiple queries in a batch
	pub fn query_batch(
		&mut self,
		statements: Vec<&str>,
	) -> Result<QueryResult, Box<dyn Error>> {
		let id = generate_request_id();
		let (tx, rx) = mpsc::channel();

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
			route: ResponseRoute::Blocking(tx),
		})?;

		let response = rx
			.recv_timeout(self.timeout)
			.map_err(|_| "Query batch timeout")?
			.map_err(|e| format!("Query batch failed: {}", e))?;

		parse_query_response(response).map_err(|e| e.into())
	}

	/// Set timeout for blocking operations
	pub fn set_timeout(&mut self, timeout: Duration) {
		self.timeout = timeout;
	}

	/// Get the current timeout setting
	pub fn timeout(&self) -> Duration {
		self.timeout
	}

	/// Check if the session is authenticated
	pub fn is_authenticated(&self) -> bool {
		self.authenticated
	}
}
