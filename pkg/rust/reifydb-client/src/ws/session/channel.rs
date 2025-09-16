// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::sync::{mpsc, Arc};

use super::ResponseMessage;
use crate::{
	utils::generate_request_id,
	ws::{
		client::ClientInner,
		message::{InternalMessage, ResponseRoute},
	},
	AuthRequest, CommandRequest, Params, QueryRequest, Request, RequestPayload,
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
	) -> Result<(Self, mpsc::Receiver<ResponseMessage>), reifydb_type::Error> {
		let (tx, rx) = mpsc::channel();

		let session = Self {
			client: client.clone(),
			token: token.clone(),
			response_tx: tx,
		};

		// Authenticate if token provided
		if token.is_some() {
			let _ = session.authenticate();
		}

		Ok((session, rx))
	}

	/// Authenticate (response arrives on channel)
	fn authenticate(&self) -> Result<String, reifydb_type::Error> {
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

		if let Err(e) = self.client.command_tx.send(InternalMessage::Request {
			id: id.clone(),
			request,
			route: ResponseRoute::Channel(self.response_tx.clone()),
		}) {
			panic!("Failed to send request: {}", e);
		}

		Ok(id)
	}

	/// Send a command (response arrives on channel)
	pub fn command(&self, rql: &str, params: Option<Params>) -> Result<String, Box<dyn std::error::Error>> {
		let id = generate_request_id();

		let request = Request {
			id: id.clone(),
			payload: RequestPayload::Command(CommandRequest {
				statements: vec![rql.to_string()],
				params,
			}),
		};

		if let Err(e) = self.client.command_tx.send(InternalMessage::Request {
			id: id.clone(),
			request,
			route: ResponseRoute::Channel(self.response_tx.clone()),
		}) {
			panic!("Failed to send request: {}", e);
		}

		Ok(id)
	}

	/// Send a query (response arrives on channel)
	pub fn query(&self, rql: &str, params: Option<Params>) -> Result<String, Box<dyn std::error::Error>> {
		let id = generate_request_id();

		let request = Request {
			id: id.clone(),
			payload: RequestPayload::Query(QueryRequest {
				statements: vec![rql.to_string()],
				params,
			}),
		};

		if let Err(e) = self.client.command_tx.send(InternalMessage::Request {
			id: id.clone(),
			request,
			route: ResponseRoute::Channel(self.response_tx.clone()),
		}) {
			panic!("Failed to send request: {}", e);
		}

		Ok(id)
	}
}
