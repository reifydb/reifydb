// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{Value, frame::frame::Frame};

use crate::json::types::{ResponseColumn, ResponseFrame};

/// Convert database result frames to the JSON wire shape.
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

/// Serialize frames to a JSON string of `[ResponseFrame, ...]`.
pub fn frames_to_json(frames: &[Frame]) -> Result<String, serde_json::Error> {
	let response_frames = convert_frames(frames);
	serde_json::to_string(&response_frames)
}
