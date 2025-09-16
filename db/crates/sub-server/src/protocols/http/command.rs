// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{Engine, Identity, Transaction};
use reifydb_type::diagnostic::Diagnostic;

use crate::{
	core::Connection,
	protocols::{
		convert::{convert_params, convert_result_to_frames},
		ws::{CommandRequest, CommandResponse, ErrResponse},
	},
};

/// Handle /v1/command endpoint
pub fn handle_v1_command<T: Transaction>(
	conn: &Connection<T>,
	cmd_req: &CommandRequest,
) -> Result<CommandResponse, ErrResponse> {
	let mut all_frames = Vec::new();

	for statement in &cmd_req.statements {
		let params =
			convert_params(&cmd_req.params).map_err(|_| {
				ErrResponse {
			diagnostic: Diagnostic {
				code: "PARAM_CONVERSION_ERROR".to_string(),
				message: "Failed to convert parameters".to_string(),
				..Default::default()
			},
		}
			})?;

		match conn.engine().command_as(
			&Identity::System {
				id: 1,
				name: "root".to_string(),
			},
			statement,
			params,
		) {
			Ok(result) => {
				let frames = convert_result_to_frames(result)
					.map_err(|_| {
					ErrResponse {
					diagnostic: Diagnostic {
						code: "FRAME_CONVERSION_ERROR".to_string(),
						message: "Failed to convert result frames".to_string(),
						..Default::default()
					},
				}
				})?;
				all_frames.extend(frames);
			}
			Err(e) => {
				let mut diagnostic = e.diagnostic();
				diagnostic.with_statement(statement.clone());
				return Err(ErrResponse {
					diagnostic,
				});
			}
		}
	}

	Ok(CommandResponse {
		frames: all_frames,
	})
}
