// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	frame::{encode::encode_frames, options::EncodeOptions},
	json::{none_marker, wire_type::to_json as type_to_json},
};
use reifydb_value::{
	reifydb_assertions,
	value::{Value, diff_type::DiffType, frame::frame::Frame, system_columns::SystemColumn, value_type::ValueType},
};
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

pub fn resolve_change_json(frames: Vec<Frame>) -> Result<ResolvedResponse, String> {
	let envelopes: Vec<JsonValue> = frames.iter().map(change_envelope).collect();
	Ok(json_response(json_to_string(&envelopes).map_err(|e| e.to_string())?))
}

fn change_envelope(frame: &Frame) -> JsonValue {
	let row_count = frame.columns.first().map(|c| c.data.len()).unwrap_or(0);
	let row_numbers = frame.row_numbers();
	let rows: Vec<JsonValue> = (0..row_count)
		.map(|i| {
			let mut obj = Map::new();
			if let Some(rn) = row_numbers.get(i) {
				obj.insert(SystemColumn::RowNumbers.name().to_string(), JsonValue::from(rn.value()));
			}
			for col in frame.iter() {
				obj.insert(col.name.clone(), row_value(&col.data.get_type(), col.data.get_value(i)));
			}
			JsonValue::Object(obj)
		})
		.collect();

	let mut envelope = Map::new();
	if let Some(op) = frame.op {
		envelope.insert("op".to_string(), JsonValue::from(DiffType::as_u8(op)));
	}
	envelope.insert("types".to_string(), frame_types(frame));
	envelope.insert("rows".to_string(), JsonValue::Array(rows));
	JsonValue::Object(envelope)
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

fn option_depth(ty: &ValueType) -> u32 {
	let mut depth = 0;
	let mut base = ty;
	while let ValueType::Option(inner) = base {
		depth += 1;
		base = inner;
	}
	depth
}

fn row_value(column_type: &ValueType, value: Value) -> JsonValue {
	match &value {
		Value::None {
			inner,
		} => {
			let wrapped = option_depth(column_type).saturating_sub(option_depth(inner) + 1);
			if wrapped == 0 {
				JsonValue::Null
			} else {
				JsonValue::String(none_marker(wrapped))
			}
		}
		_ => value.to_json_value(),
	}
}

fn frame_types(frame: &Frame) -> JsonValue {
	let mut types = Map::new();
	for col in frame.iter() {
		types.insert(col.name.clone(), type_to_json(&col.data.get_type()));
	}
	JsonValue::Object(types)
}

#[inline]
fn render_frame_rows(frames: &[Frame], unwrap: bool) -> String {
	let json_frames = frames_to_json_rows(frames);

	if unwrap && json_frames.len() == 1 && json_frames[0].len() == 1 {
		return json_to_string(&json_frames[0][0]).unwrap();
	}

	let envelopes: Vec<JsonValue> = frames
		.iter()
		.zip(json_frames)
		.map(|(frame, rows)| {
			let mut envelope = Map::new();
			envelope.insert("types".to_string(), frame_types(frame));
			envelope.insert("rows".to_string(), JsonValue::Array(rows));
			JsonValue::Object(envelope)
		})
		.collect();
	json_to_string(&envelopes).unwrap()
}

fn frames_to_json_rows(frames: &[Frame]) -> Vec<Vec<JsonValue>> {
	frames.iter()
		.map(|frame| {
			let row_count = frame.columns.first().map(|c| c.data.len()).unwrap_or(0);
			(0..row_count)
				.map(|i| {
					let mut obj = Map::new();
					for col in frame.iter() {
						obj.insert(
							col.name.clone(),
							row_value(&col.data.get_type(), col.data.get_value(i)),
						);
					}
					JsonValue::Object(obj)
				})
				.collect()
		})
		.collect()
}
