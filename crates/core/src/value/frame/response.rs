// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{Value, frame::frame::Frame, r#type::Type};
use serde::{Deserialize, Serialize};

/// A response frame containing query/command results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseFrame {
	pub row_numbers: Vec<u64>,
	pub created_at: Vec<String>,
	pub updated_at: Vec<String>,
	pub columns: Vec<ResponseColumn>,
}

/// A column in a response frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseColumn {
	pub name: String,
	#[serde(rename = "type")]
	pub r#type: Type,
	pub payload: Vec<String>,
}

/// Convert database result frames to response frames.
///
/// This function converts the internal `Frame` type to the serializable
/// `ResponseFrame` type expected by clients.
pub fn convert_frames(frames: &[Frame]) -> Vec<ResponseFrame> {
	let mut result = Vec::new();

	for frame in frames {
		let row_numbers: Vec<u64> = frame.row_numbers.iter().map(|rn| rn.value()).collect();
		let created_at: Vec<String> = frame.created_at.iter().map(|dt| dt.to_string()).collect();
		let updated_at: Vec<String> = frame.updated_at.iter().map(|dt| dt.to_string()).collect();

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
				payload: column_data,
			});
		}

		result.push(ResponseFrame {
			row_numbers,
			created_at,
			updated_at,
			columns,
		});
	}

	result
}
