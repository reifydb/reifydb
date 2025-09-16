// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{
	sync::{mpsc, Arc},
	time::Duration,
};

use reifydb_type::Error;

use crate::{
	session::{CommandResult, QueryResult},
	ws::{
		client::ClientInner,
		session::{ChannelResponse, ChannelSession, ResponseMessage},
	},
	Params,
};

/// A blocking session that waits for responses synchronously
pub struct BlockingSession {
	channel_session: ChannelSession,
	receiver: mpsc::Receiver<ResponseMessage>,
	authenticated: bool,
	timeout: Duration,
}

impl BlockingSession {
	/// Create a new blocking session
	pub(crate) fn new(client: Arc<ClientInner>, token: Option<String>) -> Result<Self, Error> {
		// Create a channel session and get the receiver
		let (channel_session, receiver) = ChannelSession::new(client, token.clone())?;

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

	/// Wait for authentication response
	fn wait_for_auth(&mut self) -> Result<(), Error> {
		// Authentication was already initiated by ChannelSession::new
		// We just need to wait for the response
		match self.receiver.recv_timeout(self.timeout) {
			Ok(msg) => match msg.response {
				Ok(ChannelResponse::Auth {
					..
				}) => {
					self.authenticated = true;
					Ok(())
				}
				Err(e) => Err(e),
				_ => panic!("Unexpected response type during authentication"),
			},
			Err(_) => panic!("Authentication timeout"),
		}
	}

	/// Send a command and wait for response
	pub fn command(&mut self, rql: &str, params: Option<Params>) -> Result<CommandResult, Error> {
		// Send command through channel session
		let request_id = self.channel_session.command(rql, params).map_err(|e| {
			reifydb_type::Error(reifydb_type::diagnostic::internal(format!(
				"Failed to send command: {}",
				e
			)))
		})?;

		// Wait for response
		match self.receiver.recv_timeout(self.timeout) {
			Ok(msg) => {
				if msg.request_id != request_id {
					panic!("Received response for wrong request ID");
				}
				match msg.response {
					Ok(ChannelResponse::Command {
						result,
						..
					}) => Ok(result),
					Err(e) => Err(e),
					_ => panic!("Unexpected response type for command"),
				}
			}
			Err(_) => panic!("Command timeout"),
		}
	}

	/// Send a query and wait for response
	pub fn query(&mut self, rql: &str, params: Option<Params>) -> Result<QueryResult, Error> {
		// Send query through channel session
		let request_id = self.channel_session.query(rql, params).map_err(|e| {
			reifydb_type::Error(reifydb_type::diagnostic::internal(format!("Failed to send query: {}", e)))
		})?;

		// Wait for response
		match self.receiver.recv_timeout(self.timeout) {
			Ok(msg) => {
				if msg.request_id != request_id {
					panic!("Received response for wrong request ID");
				}
				match msg.response {
					Ok(ChannelResponse::Query {
						result,
						..
					}) => Ok(result),
					Err(e) => Err(e),
					_ => panic!("Unexpected response type for query"),
				}
			}
			Err(_) => panic!("Query timeout"),
		}
	}

	/// Check if the session is authenticated
	pub fn is_authenticated(&self) -> bool {
		self.authenticated
	}
}
