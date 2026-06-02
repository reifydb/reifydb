// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::reifydb_assertions;
use reifydb_value::value::frame::frame::Frame;
use reifydb_wire_format::{encode::encode_frames, options::EncodeOptions};
use serde_json::{self, Map, Value as JsonValue, to_string as json_to_string};

pub const CONTENT_TYPE_JSON: &str = "application/vnd.reifydb.json";
pub const CONTENT_TYPE_FRAMES: &str = "application/vnd.reifydb.frames";
pub const CONTENT_TYPE_RBCF: &str = "application/vnd.reifydb.rbcf";
pub const CONTENT_TYPE_PROTO: &str = "application/vnd.reifydb.proto";

pub fn encode_frames_rbcf(frames: &[Frame]) -> Result<Vec<u8>, String> {
	encode_frames(frames, &EncodeOptions::fast()).map_err(|e| e.to_string())
}

pub struct ResolvedResponse {
	pub content_type: String,
	pub body: String,
}

pub fn resolve_response_json(frames: Vec<Frame>, unwrap: bool) -> Result<ResolvedResponse, String> {
	if frames.is_empty() {
		return Ok(json_response("[]".to_string()));
	}

	if has_body_column(&frames) {
		let frame = frames.into_iter().next().unwrap();
		return Ok(json_response(render_body_column(frame, unwrap)));
	}

	Ok(json_response(render_frame_rows(&frames, unwrap)))
}

#[inline]
fn json_response(body: String) -> ResolvedResponse {
	ResolvedResponse {
		content_type: CONTENT_TYPE_JSON.to_string(),
		body,
	}
}

#[inline]
fn has_body_column(frames: &[Frame]) -> bool {
	frames.first().map(|f| f.columns.iter().any(|c| c.name == "body")).unwrap_or(false)
}

#[inline]
fn render_body_column(frame: Frame, unwrap: bool) -> String {
	let body_col_idx = frame.columns.iter().position(|c| c.name == "body").unwrap();
	let body_col = &frame.columns[body_col_idx];

	let row_count = body_col.data.len();
	if body_col.data.is_utf8() {
		let values: Vec<String> = (0..row_count).map(|i| body_col.data.as_string(i)).collect();
		if unwrap || values.len() == 1 {
			values.into_iter().next().unwrap()
		} else {
			format!("[{}]", values.join(", "))
		}
	} else {
		let json_values: Vec<JsonValue> =
			(0..row_count).map(|i| body_col.data.get_value(i).to_json_value()).collect();
		reifydb_assertions! {
			let len = json_values.len();
			assert!(
				len > 0,
				"render_body_column reached the non-utf8 branch with an empty body column, but the \
				 caller already established the frame is non-empty by detecting a body column on it; \
				 indexing json_values[0] would panic on a row_count={len} column"
			);
		}
		if unwrap {
			json_to_string(&json_values[0]).unwrap()
		} else {
			json_to_string(&json_values).unwrap()
		}
	}
}

#[inline]
fn render_frame_rows(frames: &[Frame], unwrap: bool) -> String {
	let json_frames = frames_to_json_rows(frames);

	if unwrap && json_frames.len() == 1 && json_frames[0].len() == 1 {
		json_to_string(&json_frames[0][0]).unwrap()
	} else {
		json_to_string(&json_frames).unwrap()
	}
}

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
