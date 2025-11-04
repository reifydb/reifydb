// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::mpsc;

use reifydb_core::interface::Identity;
use reifydb_type::diagnostic::Diagnostic;

use crate::{
	core::{Connection, connection::RequestType, request::CommandTask},
	protocols::{
		convert::convert_params,
		ws::{CommandRequest, CommandResponse, ErrorResponse},
	},
};

/// Result of handling a command - either immediate response or pending
pub enum CommandHandlerResult {
	/// Response is ready immediately (sync execution)
	Immediate(Result<CommandResponse, ErrorResponse>),
	/// Response will be available later (async execution)
	Pending,
}

/// Handle /v1/command endpoint with async support
pub fn handle_v1_command(conn: &mut Connection, cmd_req: &CommandRequest) -> CommandHandlerResult {
	let (tx, rx) = mpsc::channel();

	let command = cmd_req.statements.join("; ");

	let identity = Identity::System {
		id: 1,
		name: "root".to_string(),
	};

	let params = match convert_params(&cmd_req.params) {
		Ok(p) => p,
		Err(_) => {
			return CommandHandlerResult::Immediate(Err(ErrorResponse {
				diagnostic: Diagnostic {
					code: "PARAM_CONVERSION_ERROR".to_string(),
					message: "Failed to convert parameters".to_string(),
					..Default::default()
				},
			}));
		}
	};

	let task = CommandTask::new("".to_string(), command, identity, params, tx);

	if let Err(e) = conn.scheduler().once(Box::new(task)) {
		return CommandHandlerResult::Immediate(Err(ErrorResponse {
			diagnostic: Diagnostic {
				code: "SCHEDULER_ERROR".to_string(),
				message: format!("Failed to submit command to worker pool: {}", e),
				..Default::default()
			},
		}));
	}

	conn.submit_query(rx, RequestType::HttpCommand);

	CommandHandlerResult::Pending
}
