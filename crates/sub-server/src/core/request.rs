use std::sync::mpsc;

use mpsc::Sender;
use reifydb_core::{
	Result,
	interface::{Engine, Identity, Params},
};
use reifydb_sub_api::{OnceTask, Priority, TaskContext};

use crate::protocols::ws::Response;

/// Task for executing a query in the worker pool
pub struct QueryTask {
	request_id: String,
	query: String,
	identity: Identity,
	params: Params,
	tx: Sender<Result<Response>>,
}

impl QueryTask {
	pub fn new(
		request_id: String,
		query: String,
		identity: Identity,
		params: Params,
		tx: Sender<Result<Response>>,
	) -> Self {
		Self {
			request_id,
			query,
			identity,
			params,
			tx,
		}
	}
}

impl OnceTask for QueryTask {
	fn name(&self) -> &str {
		"query-execution"
	}

	fn priority(&self) -> Priority {
		Priority::Normal
	}

	fn execute_once(self: Box<Self>, ctx: &TaskContext) -> Result<()> {
		use crate::protocols::{
			convert::convert_frames,
			ws::{ErrorResponse, QueryResponse, ResponsePayload},
		};

		let result = ctx.engine().query_as(&self.identity, &self.query, self.params);

		let query_result = match result {
			Ok(frames) => {
				// Convert frames to websocket frames
				match convert_frames(frames) {
					Ok(ws_frames) => Ok(Response {
						id: self.request_id,
						payload: ResponsePayload::Query(QueryResponse {
							frames: ws_frames,
						}),
					}),
					Err(e) => {
						// Conversion error - internal error
						use reifydb_type::diagnostic::Diagnostic;
						let diagnostic = Diagnostic {
							code: "INTERNAL_ERROR".to_string(),
							message: format!("Failed to convert frames: {}", e),
							..Default::default()
						};
						Ok(Response {
							id: self.request_id,
							payload: ResponsePayload::Err(ErrorResponse {
								diagnostic,
							}),
						})
					}
				}
			}
			Err(e) => {
				// Query execution error - add statement context
				let mut diagnostic = e.diagnostic();
				diagnostic.with_statement(self.query.clone());
				Ok(Response {
					id: self.request_id,
					payload: ResponsePayload::Err(ErrorResponse {
						diagnostic,
					}),
				})
			}
		};

		let _ = self.tx.send(query_result);
		Ok(())
	}
}

/// Task for executing a command in the worker pool
pub struct CommandTask {
	request_id: String,
	command: String,
	identity: Identity,
	params: Params,
	tx: Sender<Result<Response>>,
}

impl CommandTask {
	pub fn new(
		request_id: String,
		command: String,
		identity: Identity,
		params: Params,
		tx: Sender<Result<Response>>,
	) -> Self {
		Self {
			command,
			identity,
			params,
			request_id,
			tx,
		}
	}
}

impl OnceTask for CommandTask {
	fn name(&self) -> &str {
		"command-execution"
	}

	fn priority(&self) -> Priority {
		Priority::High
	}

	fn execute_once(self: Box<Self>, ctx: &TaskContext) -> Result<()> {
		use crate::protocols::{
			convert::convert_frames,
			ws::{CommandResponse, ErrorResponse, ResponsePayload},
		};

		let result = ctx.engine().command_as(&self.identity, &self.command, self.params);

		let command_result = match result {
			Ok(frames) => {
				// Convert frames to websocket frames
				match convert_frames(frames) {
					Ok(ws_frames) => Ok(Response {
						id: self.request_id,
						payload: ResponsePayload::Command(CommandResponse {
							frames: ws_frames,
						}),
					}),
					Err(e) => {
						// Conversion error - internal error
						use reifydb_type::diagnostic::Diagnostic;
						let diagnostic = Diagnostic {
							code: "INTERNAL_ERROR".to_string(),
							message: format!("Failed to convert frames: {}", e),
							..Default::default()
						};
						Ok(Response {
							id: self.request_id,
							payload: ResponsePayload::Err(ErrorResponse {
								diagnostic,
							}),
						})
					}
				}
			}
			Err(e) => {
				// Command execution error - add statement context
				let mut diagnostic = e.diagnostic();
				diagnostic.with_statement(self.command.clone());
				Ok(Response {
					id: self.request_id,
					payload: ResponsePayload::Err(ErrorResponse {
						diagnostic,
					}),
				})
			}
		};

		let _ = self.tx.send(command_result);
		Ok(())
	}
}
