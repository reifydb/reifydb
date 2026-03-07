// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{Value, frame::frame::Frame, r#type::Type};
use serde::{Deserialize, Serialize};
use serde_json::{self, Map, Value as JsonValue, to_string as json_to_string};

/// A resolved JSON response for `?format=json` mode.
pub struct ResolvedResponse {
	pub content_type: String,
	pub body: String,
}

/// Resolve frames into a JSON response body.
///
/// If a `body` column exists in the first frame, extracts it:
/// - Utf8 body: pass through raw (pre-serialized JSON, never re-parsed)
/// - Structured body (Record/List/etc): serialize via `Value::to_json_value()`, always array
///
/// Otherwise, converts all frames to row-oriented JSON objects.
pub fn resolve_response_json(frames: Vec<Frame>, unwrap: bool) -> Result<ResolvedResponse, String> {
	if frames.is_empty() {
		return Ok(ResolvedResponse {
			content_type: "application/json".to_string(),
			body: "[]".to_string(),
		});
	}

	// Check if the first frame has a "body" column
	let has_body_col = frames.first().map(|f| f.columns.iter().any(|c| c.name == "body")).unwrap_or(false);

	if has_body_col {
		// Existing body-column path
		let frame = frames.into_iter().next().unwrap();
		let body_col_idx = frame.columns.iter().position(|c| c.name == "body").unwrap();
		let body_col = &frame.columns[body_col_idx];

		let row_count = body_col.data.len();
		let body = if body_col.data.is_utf8() {
			// Utf8: pre-serialized JSON, pass through raw (NEVER re-parse)
			let values: Vec<String> = (0..row_count).map(|i| body_col.data.as_string(i)).collect();
			if unwrap || values.len() == 1 {
				values.into_iter().next().unwrap()
			} else {
				format!("[{}]", values.join(", "))
			}
		} else {
			// Structured (Record/List/etc): serialize to JSON
			let json_values: Vec<JsonValue> =
				(0..row_count).map(|i| body_col.data.get_value(i).to_json_value()).collect();
			if unwrap {
				json_to_string(&json_values[0]).unwrap()
			} else {
				json_to_string(&json_values).unwrap()
			}
		};

		Ok(ResolvedResponse {
			content_type: "application/json".to_string(),
			body,
		})
	} else {
		// Generic path: convert all frames to row-oriented JSON
		let json_frames = frames_to_json_rows(&frames);

		let body = if unwrap && json_frames.len() == 1 && json_frames[0].len() == 1 {
			json_to_string(&json_frames[0][0]).unwrap()
		} else {
			json_to_string(&json_frames).unwrap()
		};

		Ok(ResolvedResponse {
			content_type: "application/json".to_string(),
			body,
		})
	}
}

/// Convert frames to row-oriented JSON arrays.
///
/// Returns one array of JSON objects per frame. Each object maps column names to JSON values.
fn frames_to_json_rows(frames: &[Frame]) -> Vec<Vec<JsonValue>> {
	frames.iter()
		.map(|frame| {
			let row_count = frame.columns.first().map(|c| c.data.len()).unwrap_or(0);
			(0..row_count)
				.map(|i| {
					let mut obj = Map::new();
					for col in frame.iter() {
						obj.insert(col.name.clone(), col.data.get_value(i).to_json_value());
					}
					JsonValue::Object(obj)
				})
				.collect()
		})
		.collect()
}

/// A response frame containing query/command results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseFrame {
	pub row_numbers: Vec<u64>,
	pub columns: Vec<ResponseColumn>,
}

/// A column in a response frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseColumn {
	pub name: String,
	#[serde(rename = "type")]
	pub r#type: Type,
	pub data: Vec<String>,
}

/// Convert database result frames to response frames.
///
/// This function converts the internal `Frame` type to the serializable
/// `ResponseFrame` type expected by clients.
pub fn convert_frames(frames: Vec<Frame>) -> Vec<ResponseFrame> {
	let mut result = Vec::new();

	for frame in frames {
		let row_numbers: Vec<u64> = frame.row_numbers.iter().map(|rn| rn.value()).collect();

		let mut columns = Vec::new();

		for column in frame.iter() {
			let column_data: Vec<String> = column
				.data
				.iter()
				.map(|value| match value {
					Value::None {
						..
					} => "⟪none⟫".to_string(),
					Value::Blob(b) => b.to_hex(),
					_ => value.to_string(),
				})
				.collect();

			columns.push(ResponseColumn {
				name: column.name.clone(),
				r#type: column.data.get_type(),
				data: column_data,
			});
		}

		result.push(ResponseFrame {
			row_numbers,
			columns,
		});
	}

	result
}
