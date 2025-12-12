// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! Frame conversion for WebSocket responses.

use reifydb_core::Frame;
use reifydb_type::Value;

use crate::response::{ResponseColumn, ResponseFrame};

/// Convert database result frames to WebSocket response frames.
///
/// This function converts the internal `Frame` type to the serializable
/// `ResponseFrame` type expected by WebSocket clients.
pub fn convert_frames(frames: Vec<Frame>) -> Vec<ResponseFrame> {
	let mut result = Vec::new();

	for frame in frames {
		let row_numbers: Vec<u64> = frame.row_numbers.iter().map(|rn| rn.value()).collect();

		let mut ws_columns = Vec::new();

		for column in frame.iter() {
			let column_data: Vec<String> = column
				.data
				.iter()
				.map(|value| match value {
					Value::Undefined => "⟪undefined⟫".to_string(),
					Value::Blob(b) => b.to_hex(),
					_ => value.to_string(),
				})
				.collect();

			ws_columns.push(ResponseColumn {
				namespace: column.namespace.clone(),
				store: column.source.clone(),
				name: column.name.clone(),
				r#type: column.data.get_type(),
				data: column_data,
			});
		}

		result.push(ResponseFrame {
			row_numbers,
			columns: ws_columns,
		});
	}

	result
}
