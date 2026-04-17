// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{err, error::Error};
use reifydb_wire_format::json::from::convert_envelope_response;

use crate::{AdminResult, CommandResult, QueryResult, Response, ResponsePayload};

// Helper functions for parsing responses - made public for ws module
pub fn parse_admin_response(response: Response) -> Result<AdminResult, Error> {
	match response.payload {
		ResponsePayload::Admin(admin_response) => Ok(AdminResult {
			frames: convert_envelope_response(admin_response.body),
			meta: admin_response.meta,
		}),
		// Admin responses may come back as Command responses from the server
		ResponsePayload::Command(cmd_response) => Ok(AdminResult {
			frames: convert_envelope_response(cmd_response.body),
			meta: cmd_response.meta,
		}),
		ResponsePayload::Err(err) => {
			err!(err.diagnostic)
		}
		other => {
			println!("Unexpected execute response: {:?}", other);
			panic!("Unexpected execute response type")
		}
	}
}

pub fn parse_command_response(response: Response) -> Result<CommandResult, Error> {
	match response.payload {
		ResponsePayload::Command(cmd_response) => Ok(CommandResult {
			frames: convert_envelope_response(cmd_response.body),
			meta: cmd_response.meta,
		}),
		ResponsePayload::Err(err) => {
			err!(err.diagnostic)
		}
		other => {
			println!("Unexpected execute response: {:?}", other);
			panic!("Unexpected execute response type")
		}
	}
}

pub fn parse_query_response(response: Response) -> Result<QueryResult, Error> {
	match response.payload {
		ResponsePayload::Query(query_response) => {
			let frames = convert_envelope_response(query_response.body);
			Ok(QueryResult {
				frames,
				meta: query_response.meta,
			})
		}
		ResponsePayload::Err(err) => {
			err!(err.diagnostic)
		}
		other => {
			println!("Unexpected execute response: {:?}", other);
			panic!("Unexpected execute response type")
		}
	}
}
