// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{Frame, interface::Params};
use reifydb_type::Value;

use crate::protocols::{
	ProtocolResult,
	ws::{WebsocketColumn, WebsocketFrame, WsParams},
};

/// Convert WebSocket params to engine params
pub fn convert_params(params: &Option<WsParams>) -> ProtocolResult<Params> {
	match params {
		Some(WsParams::Positional(values)) => {
			Ok(Params::Positional(values.clone()))
		}
		Some(WsParams::Named(map)) => Ok(Params::Named(map.clone())),
		None => Ok(Params::None),
	}
}

/// Convert database result frames to WebSocket frames
pub fn convert_result_to_frames(
	result: Vec<Frame>,
) -> ProtocolResult<Vec<WebsocketFrame>> {
	let mut ws_frames = Vec::new();

	for frame in result {
		let mut ws_columns = Vec::new();

		for column in frame.iter() {
			let column_data: Vec<String> =
				column.data
					.iter()
					.map(|value| {
						match value {
						Value::Undefined => "⟪undefined⟫".to_string(),
						Value::Blob(b) => reifydb_type::util::hex::encode(&b),
						_ => value.to_string(),
					}
					})
					.collect();

			ws_columns.push(WebsocketColumn {
				schema: column.schema.clone(),
				store: column.store.clone(),
				name: column.name.clone(),
				r#type: column.data.get_type(),
				data: column_data,
			});
		}

		ws_frames.push(WebsocketFrame {
			columns: ws_columns,
		});
	}

	Ok(ws_frames)
}
