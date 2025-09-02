// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{
	sync::{Arc, mpsc},
	time::Duration,
};

use reifydb_type::Error;

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
	) -> Result<Self, Error> {
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
	fn authenticate(&mut self) -> Result<(), Error> {
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

		if let Err(e) =
			self.client.command_tx.send(InternalMessage::Request {
				id: id.clone(),
				request,
				route: ResponseRoute::Blocking(tx),
			}) {
			panic!("Failed to send auth request: {}", e);
		}

		let response = match rx.recv_timeout(self.timeout) {
			Ok(Ok(resp)) => resp,
			Ok(Err(e)) => return Err(e),
			Err(_) => panic!("Authentication timeout"),
		};

		match response.payload {
			crate::ResponsePayload::Auth(_) => {
				self.authenticated = true;
				Ok(())
			}
			crate::ResponsePayload::Err(e) => {
				panic!("Authentication failed: {:?}", e)
			}
			_ => panic!(
				"Unexpected response type during authentication"
			),
		}
	}

	/// Send a command and wait for response
	pub fn command(
		&mut self,
		rql: &str,
		params: Option<Params>,
	) -> Result<CommandResult, Error> {
		let id = generate_request_id();
		let (tx, rx) = mpsc::channel();

		let request = Request {
			id: id.clone(),
			payload: RequestPayload::Command(CommandRequest {
				statements: vec![rql.to_string()],
				params,
			}),
		};

		if let Err(e) =
			self.client.command_tx.send(InternalMessage::Request {
				id: id.clone(),
				request,
				route: ResponseRoute::Blocking(tx),
			}) {
			panic!("Failed to send command request: {}", e);
		}

		let response = match rx.recv_timeout(self.timeout) {
			Ok(Ok(resp)) => resp,
			Ok(Err(e)) => return Err(e),
			Err(_) => panic!("Command timeout"),
		};

		parse_command_response(response)
	}

	/// Send a query and wait for response
	pub fn query(
		&mut self,
		rql: &str,
		params: Option<Params>,
	) -> Result<QueryResult, Error> {
		let id = generate_request_id();
		let (tx, rx) = mpsc::channel();

		let request = Request {
			id: id.clone(),
			payload: RequestPayload::Query(QueryRequest {
				statements: vec![rql.to_string()],
				params,
			}),
		};

		if let Err(e) =
			self.client.command_tx.send(InternalMessage::Request {
				id: id.clone(),
				request,
				route: ResponseRoute::Blocking(tx),
			}) {
			panic!("Failed to send query request: {}", e);
		}

		let response = match rx.recv_timeout(self.timeout) {
			Ok(Ok(resp)) => resp,
			Ok(Err(e)) => return Err(e),
			Err(_) => panic!("Query timeout"),
		};

		parse_query_response(response)
	}

	/// Check if the session is authenticated
	pub fn is_authenticated(&self) -> bool {
		self.authenticated
	}
}
