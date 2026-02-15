// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{Value, frame::frame::Frame, r#type::Type};
use serde::{Deserialize, Serialize};

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
					Value::None => "⟪undefined⟫".to_string(),
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
