// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::sync::{Arc, Mutex};

use super::{
	CommandResult, QueryResult, parse_command_response,
	parse_query_response,
};
use crate::{
	AuthRequest, CommandRequest, Params, QueryRequest, Request,
	RequestPayload, Response,
	client::{
		ClientInner, InternalMessage, ResponseRoute,
		generate_request_id,
	},
};

/// A callback-based session for asynchronous operations
pub struct CallbackSession {
	client: Arc<ClientInner>,
	token: Option<String>,
	authenticated: Arc<Mutex<bool>>,
}

impl CallbackSession {
	/// Create a new callback session
	pub(crate) fn new(
		client: Arc<ClientInner>,
		token: Option<String>,
	) -> Result<Self, Box<dyn std::error::Error>> {
		let session = Self {
			client: client.clone(),
			token: token.clone(),
			authenticated: Arc::new(Mutex::new(false)),
		};

		// Authenticate if token provided
		if let Some(_token) = &token {
			let auth_flag = session.authenticated.clone();
			session.authenticate(move |result| match result {
				Ok(_) => {
					*auth_flag.lock().unwrap() = true;
					println!("Authentication successful");
				}
				Err(e) => {
					eprintln!(
						"Authentication failed: {}",
						e
					);
				}
			})?;
		}

		Ok(session)
	}

	/// Authenticate with callback
	fn authenticate<F>(
		&self,
		callback: F,
	) -> Result<String, Box<dyn std::error::Error>>
	where
		F: FnOnce(Result<(), String>) + Send + 'static,
	{
		let id = generate_request_id();

		let request = Request {
			id: id.clone(),
			payload: RequestPayload::Auth(AuthRequest {
				token: self.token.clone(),
			}),
		};

		let callback =
			Box::new(move |result: Result<Response, String>| {
				match result {
				Ok(response) => match response.payload {
					crate::ResponsePayload::Auth(_) => {
						callback(Ok(()))
					}
					crate::ResponsePayload::Err(e) => {
						callback(Err(format!(
							"Authentication error: {:?}",
							e
						)))
					}
					_ => callback(Err(
						"Unexpected response type"
							.to_string(),
					)),
				},
				Err(e) => callback(Err(e)),
			}
			});

		self.client.command_tx.send(InternalMessage::Request {
			id: id.clone(),
			request,
			route: ResponseRoute::Callback(callback),
		})?;

		Ok(id)
	}

	/// Send a command with callback
	pub fn command<F>(
		&self,
		rql: &str,
		params: Option<Params>,
		callback: F,
	) -> Result<String, Box<dyn std::error::Error>>
	where
		F: FnOnce(Result<CommandResult, String>) + Send + 'static,
	{
		let id = generate_request_id();

		let request = Request {
			id: id.clone(),
			payload: RequestPayload::Command(CommandRequest {
				statements: vec![rql.to_string()],
				params,
			}),
		};

		let callback =
			Box::new(move |result: Result<Response, String>| {
				callback(
					result.and_then(parse_command_response),
				)
			});

		self.client.command_tx.send(InternalMessage::Request {
			id: id.clone(),
			request,
			route: ResponseRoute::Callback(callback),
		})?;

		Ok(id)
	}

	/// Send a query with callback
	pub fn query<F>(
		&self,
		rql: &str,
		params: Option<Params>,
		callback: F,
	) -> Result<String, Box<dyn std::error::Error>>
	where
		F: FnOnce(Result<QueryResult, String>) + Send + 'static,
	{
		let id = generate_request_id();

		let request = Request {
			id: id.clone(),
			payload: RequestPayload::Query(QueryRequest {
				statements: vec![rql.to_string()],
				params,
			}),
		};

		let callback =
			Box::new(move |result: Result<Response, String>| {
				callback(result.and_then(parse_query_response))
			});

		self.client.command_tx.send(InternalMessage::Request {
			id: id.clone(),
			request,
			route: ResponseRoute::Callback(callback),
		})?;

		Ok(id)
	}

	/// Send multiple commands with callback
	pub fn command_batch<F>(
		&self,
		statements: Vec<&str>,
		callback: F,
	) -> Result<String, Box<dyn std::error::Error>>
	where
		F: FnOnce(Result<CommandResult, String>) + Send + 'static,
	{
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

		let callback =
			Box::new(move |result: Result<Response, String>| {
				callback(
					result.and_then(parse_command_response),
				)
			});

		self.client.command_tx.send(InternalMessage::Request {
			id: id.clone(),
			request,
			route: ResponseRoute::Callback(callback),
		})?;

		Ok(id)
	}

	/// Send multiple queries with callback
	pub fn query_batch<F>(
		&self,
		statements: Vec<&str>,
		callback: F,
	) -> Result<String, Box<dyn std::error::Error>>
	where
		F: FnOnce(Result<QueryResult, String>) + Send + 'static,
	{
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

		let callback =
			Box::new(move |result: Result<Response, String>| {
				callback(result.and_then(parse_query_response))
			});

		self.client.command_tx.send(InternalMessage::Request {
			id: id.clone(),
			request,
			route: ResponseRoute::Callback(callback),
		})?;

		Ok(id)
	}

	/// Check if the session is authenticated
	pub fn is_authenticated(&self) -> bool {
		*self.authenticated.lock().unwrap()
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
	) -> Result<String, Box<dyn std::error::Error>> {
		self.command(rql, params, move |result| match result {
			Ok(data) => handler.on_success(data),
			Err(e) => handler.on_error(e),
		})
	}

	/// Execute query with a response handler
	pub fn query_with_handler(
		&self,
		rql: &str,
		params: Option<Params>,
		mut handler: impl QueryHandler + 'static,
	) -> Result<String, Box<dyn std::error::Error>> {
		self.query(rql, params, move |result| match result {
			Ok(data) => handler.on_success(data),
			Err(e) => handler.on_error(e),
		})
	}
}
