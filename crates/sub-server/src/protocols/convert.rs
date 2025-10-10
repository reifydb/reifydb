// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{Frame, interface::Params};
use reifydb_type::Value;

use crate::protocols::{
	ProtocolResult,
	ws::{WebsocketColumn, WebsocketFrame},
};

/// Convert WebSocket params to engine params
pub fn convert_params(params: &Option<Params>) -> ProtocolResult<Params> {
	match params {
		Some(Params::Positional(values)) => Ok(Params::Positional(values.clone())),
		Some(Params::Named(map)) => Ok(Params::Named(map.clone())),
		&Some(Params::None) => Ok(Params::None),
		None => Ok(Params::None),
	}
}

/// Convert database result frames to WebSocket frames
pub fn convert_result_to_frames(result: Vec<Frame>) -> ProtocolResult<Vec<WebsocketFrame>> {
	let mut ws_frames = Vec::new();

	for frame in result {
		let row_numbers: Vec<u64> = frame.row_numbers.iter().map(|rn| rn.value()).collect();

		let mut ws_columns = Vec::new();

		for column in frame.iter() {
			let column_data: Vec<String> = column
				.data
				.iter()
				.map(|value| match value {
					Value::Undefined => "⟪undefined⟫".to_string(),
					Value::Blob(b) => reifydb_type::util::hex::encode(&b),
					_ => value.to_string(),
				})
				.collect();

			ws_columns.push(WebsocketColumn {
				namespace: column.namespace.clone(),
				store: column.source.clone(),
				name: column.name.clone(),
				r#type: column.data.get_type(),
				data: column_data,
			});
		}

		ws_frames.push(WebsocketFrame {
			row_numbers,
			columns: ws_columns,
		});
	}

	Ok(ws_frames)
}
