// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

mod blocking;
mod callback;
mod channel;

use std::time::Instant;

pub use blocking::BlockingSession;
pub use callback::CallbackSession;
pub use channel::ChannelSession;
use reifydb_type::{
	Blob, Date, DateTime, Error, IdentityId, RowNumber, Time, Uuid4, Uuid7,
	err,
};

use crate::{
	CommandResponse, OrderedF32, OrderedF64, QueryResponse, Response,
	ResponsePayload, Type, Value,
	domain::{Frame, FrameColumn},
};

/// Response message for channel sessions
#[derive(Debug)]
pub struct ResponseMessage {
	pub request_id: String,
	pub response: Result<Response, Error>,
	pub timestamp: Instant,
}

/// Result type for command operations
#[derive(Debug)]
pub struct CommandResult {
	pub frames: Vec<Frame>,
}

/// Result type for query operations
#[derive(Debug)]
pub struct QueryResult {
	pub frames: Vec<Frame>,
}

// Helper functions for parsing responses
pub(crate) fn parse_command_response(
	response: Response,
) -> Result<CommandResult, Error> {
	match response.payload {
		ResponsePayload::Command(cmd_response) => Ok(CommandResult {
			frames: convert_execute_response(cmd_response),
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

pub(crate) fn parse_query_response(
	response: Response,
) -> Result<QueryResult, Error> {
	match response.payload {
		ResponsePayload::Query(query_response) => {
			let frames = convert_query_response(query_response);
			Ok(QueryResult {
				frames,
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

fn convert_execute_response(payload: CommandResponse) -> Vec<Frame> {
	let mut result = Vec::new();

	for frame in payload.frames {
		let columns = frame
			.columns
			.into_iter()
			.map(|col| FrameColumn {
				schema: col.schema,
				store: col.store,
				name: col.name,
				r#type: col.r#type,
				data: convert_column_values(
					col.r#type, col.data,
				),
			})
			.collect();

		result.push(Frame::new(columns))
	}

	result
}

fn convert_query_response(payload: QueryResponse) -> Vec<Frame> {
	let mut result = Vec::new();

	for frame in payload.frames {
		let columns = frame
			.columns
			.into_iter()
			.map(|col| FrameColumn {
				schema: col.schema,
				store: col.store,
				name: col.name,
				r#type: col.r#type,
				data: convert_column_values(
					col.r#type, col.data,
				),
			})
			.collect();

		result.push(Frame::new(columns))
	}

	result
}

fn convert_column_values(target: Type, data: Vec<String>) -> Vec<Value> {
	data.into_iter().map(|s| parse_value_from_string(&s, &target)).collect()
}

fn parse_value_from_string(s: &str, value_type: &Type) -> Value {
	if s == "⟪undefined⟫" || s == "NULL" || s == "Undefined" {
		return Value::Undefined;
	}

	match value_type {
		Type::Undefined => Value::Undefined,
		Type::Bool => match s {
			"true" => Value::Bool(true),
			"false" => Value::Bool(false),
			_ => Value::Undefined,
		},
		Type::Float4 => s
			.parse::<f32>()
			.ok()
			.and_then(|f| OrderedF32::try_from(f).ok())
			.map(Value::Float4)
			.unwrap_or(Value::Undefined),
		Type::Float8 => s
			.parse::<f64>()
			.ok()
			.and_then(|f| OrderedF64::try_from(f).ok())
			.map(Value::Float8)
			.unwrap_or(Value::Undefined),
		Type::Int1 => s
			.parse::<i8>()
			.map(Value::Int1)
			.unwrap_or(Value::Undefined),
		Type::Int2 => s
			.parse::<i16>()
			.map(Value::Int2)
			.unwrap_or(Value::Undefined),
		Type::Int4 => s
			.parse::<i32>()
			.map(Value::Int4)
			.unwrap_or(Value::Undefined),
		Type::Int8 => s
			.parse::<i64>()
			.map(Value::Int8)
			.unwrap_or(Value::Undefined),
		Type::Int16 => s
			.parse::<i128>()
			.map(Value::Int16)
			.unwrap_or(Value::Undefined),
		Type::Uint1 => s
			.parse::<u8>()
			.map(Value::Uint1)
			.unwrap_or(Value::Undefined),
		Type::Uint2 => s
			.parse::<u16>()
			.map(Value::Uint2)
			.unwrap_or(Value::Undefined),
		Type::Uint4 => s
			.parse::<u32>()
			.map(Value::Uint4)
			.unwrap_or(Value::Undefined),
		Type::Uint8 => s
			.parse::<u64>()
			.map(Value::Uint8)
			.unwrap_or(Value::Undefined),
		Type::Uint16 => s
			.parse::<u128>()
			.map(Value::Uint16)
			.unwrap_or(Value::Undefined),
		Type::Utf8 => Value::Utf8(s.to_string()),
		Type::Date => {
			// Parse date from ISO format (YYYY-MM-DD)
			let parts: Vec<&str> = s.split('-').collect();
			if parts.len() == 3 {
				let year =
					parts[0].parse::<i32>().unwrap_or(1970);
				let month =
					parts[1].parse::<u32>().unwrap_or(1);
				let day = parts[2].parse::<u32>().unwrap_or(1);
				Date::from_ymd(year, month, day)
					.map(Value::Date)
					.unwrap_or(Value::Undefined)
			} else {
				Value::Undefined
			}
		}
		Type::DateTime => {
			// Try parsing as timestamp first
			if let Ok(timestamp) = s.parse::<i64>() {
				DateTime::from_timestamp(timestamp)
					.map(Value::DateTime)
					.unwrap_or(Value::Undefined)
			} else {
				// For now, store as string - proper RFC3339
				// parsing would need chrono
				Value::Utf8(s.to_string())
			}
		}
		Type::Time => {
			// Parse time from HH:MM:SS.nnnnnnnnn format
			let parts: Vec<&str> = s.split(':').collect();
			if parts.len() >= 3 {
				let hour = parts[0].parse::<u32>().unwrap_or(0);
				let min = parts[1].parse::<u32>().unwrap_or(0);

				// Handle seconds and nanoseconds
				let sec_parts: Vec<&str> =
					parts[2].split('.').collect();
				let sec = sec_parts[0]
					.parse::<u32>()
					.unwrap_or(0);

				let nano = if sec_parts.len() > 1 {
					let frac_str = sec_parts[1];
					let padded = if frac_str.len() < 9 {
						format!("{:0<9}", frac_str)
					} else {
						frac_str[..9].to_string()
					};
					padded.parse::<u32>().unwrap_or(0)
				} else {
					0
				};

				Time::from_hms_nano(hour, min, sec, nano)
					.map(Value::Time)
					.unwrap_or(Value::Undefined)
			} else {
				Value::Undefined
			}
		}
		Type::Interval => {
			// For now, store as string - proper ISO 8601 duration
			// parsing would need additional logic
			Value::Utf8(s.to_string())
		}
		Type::RowNumber => {
			if let Ok(id) = s.parse::<u64>() {
				Value::RowNumber(RowNumber::new(id))
			} else {
				Value::Undefined
			}
		}
		Type::Uuid4 => {
			// Try to parse UUID
			if let Ok(uuid) = uuid::Uuid::parse_str(s) {
				Value::Uuid4(Uuid4::from(uuid))
			} else {
				Value::Undefined
			}
		}
		Type::Uuid7 => {
			// Try to parse UUID
			if let Ok(uuid) = uuid::Uuid::parse_str(s) {
				Value::Uuid7(Uuid7::from(uuid))
			} else {
				Value::Undefined
			}
		}
		Type::IdentityId => {
			// Try to parse UUID for IdentityId
			if let Ok(uuid) = uuid::Uuid::parse_str(s) {
				Value::IdentityId(IdentityId::from(
					Uuid7::from(uuid),
				))
			} else {
				Value::Undefined
			}
		}
		Type::Blob => {
			// Parse hex string (assuming 0x prefix)
			if s.starts_with("0x") {
				if let Ok(bytes) = hex::decode(&s[2..]) {
					Value::Blob(Blob::new(bytes))
				} else {
					Value::Undefined
				}
			} else {
				Value::Undefined
			}
		}
	}
}
