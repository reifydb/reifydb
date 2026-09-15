// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::json::{excerpt, from::frames_from_envelope};
use reifydb_value::{err, error::Error, value::frame::frame::Frame};
use serde_json::Value as JsonValue;

use crate::{AdminResult, CommandResult, QueryResult, Response, ResponsePayload, error::ClientError};

fn unexpected_response(expected: &str, other: ResponsePayload) -> Error {
	ClientError::UnexpectedResponse(format!(
		"unexpected response to {expected}: {}",
		excerpt(&format!("{other:?}"))
	))
	.into()
}

fn envelope_frames(body: JsonValue) -> Result<Vec<Frame>, Error> {
	frames_from_envelope(body)
		.map_err(|error| ClientError::Decode(format!("failed to decode frames envelope: {error}")).into())
}

pub fn parse_admin_response(response: Response) -> Result<AdminResult, Error> {
	match response.payload {
		ResponsePayload::Admin(admin_response) => Ok(AdminResult {
			frames: envelope_frames(admin_response.body)?,
			meta: admin_response.meta,
		}),
		ResponsePayload::Command(cmd_response) => Ok(AdminResult {
			frames: envelope_frames(cmd_response.body)?,
			meta: cmd_response.meta,
		}),
		ResponsePayload::Err(err) => {
			err!(err.diagnostic)
		}
		other => Err(unexpected_response("admin", other)),
	}
}

pub fn parse_command_response(response: Response) -> Result<CommandResult, Error> {
	match response.payload {
		ResponsePayload::Command(cmd_response) => Ok(CommandResult {
			frames: envelope_frames(cmd_response.body)?,
			meta: cmd_response.meta,
		}),
		ResponsePayload::Err(err) => {
			err!(err.diagnostic)
		}
		other => Err(unexpected_response("command", other)),
	}
}

pub fn parse_query_response(response: Response) -> Result<QueryResult, Error> {
	match response.payload {
		ResponsePayload::Query(query_response) => {
			let frames = envelope_frames(query_response.body)?;
			Ok(QueryResult {
				frames,
				meta: query_response.meta,
			})
		}
		ResponsePayload::Err(err) => {
			err!(err.diagnostic)
		}
		other => Err(unexpected_response("query", other)),
	}
}

pub fn parse_call_response(response: Response) -> Result<CommandResult, Error> {
	match response.payload {
		ResponsePayload::Call(call_response) => Ok(CommandResult {
			frames: envelope_frames(call_response.body)?,
			meta: call_response.meta,
		}),
		ResponsePayload::Err(err) => {
			err!(err.diagnostic)
		}
		other => Err(unexpected_response("call", other)),
	}
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::{parse_admin_response, parse_call_response, parse_command_response, parse_query_response};
	use crate::{QueryResponse, Response, ResponsePayload, UnsubscribedResponse};

	fn unsubscribed() -> Response {
		Response {
			id: "req-1".to_string(),
			payload: ResponsePayload::Unsubscribed(UnsubscribedResponse {
				subscription_id: "sub-1".to_string(),
			}),
		}
	}

	#[test]
	fn a_malformed_frames_envelope_fails_the_request_with_a_decode_error() {
		// Zero frames is a valid answer, so a body that fails to decode must reach the caller as an error.
		let response = Response {
			id: "req-1".to_string(),
			payload: ResponsePayload::Query(QueryResponse {
				content_type: "application/vnd.reifydb.frames".to_string(),
				body: json!({ "frames": [{ "columns": "not a column list" }] }),
				meta: None,
			}),
		};

		match parse_query_response(response) {
			Err(error) => assert_eq!(error.0.code, "DECODE"),
			Ok(result) => panic!("a malformed envelope decoded as {} frames", result.frames.len()),
		}
	}

	#[test]
	fn an_unexpected_response_type_fails_the_request_instead_of_panicking() {
		// A server reply of the wrong kind is bad wire data, so it must reach the caller, not abort its task.
		assert_eq!(
			parse_admin_response(unsubscribed()).err().map(|error| error.0.code),
			Some("UNEXPECTED_RESPONSE".to_string())
		);
		assert_eq!(
			parse_command_response(unsubscribed()).err().map(|error| error.0.code),
			Some("UNEXPECTED_RESPONSE".to_string())
		);
		assert_eq!(
			parse_query_response(unsubscribed()).err().map(|error| error.0.code),
			Some("UNEXPECTED_RESPONSE".to_string())
		);
		assert_eq!(
			parse_call_response(unsubscribed()).err().map(|error| error.0.code),
			Some("UNEXPECTED_RESPONSE".to_string())
		);
	}
}
