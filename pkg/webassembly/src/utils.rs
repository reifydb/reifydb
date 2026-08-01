// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use js_sys::{Array, JSON, Object, Reflect};
use reifydb_codec::json::to::convert_frames;
use reifydb_value::{params::Params, value::frame::frame::Frame};
use serde_json::{Value as JsonValue, from_str as json_from_str};
use wasm_bindgen::prelude::*;
use web_sys::console;

use crate::error::JsError;

/// Shares `convert_frames` with the server so value formatting is identical (Duration renders as `0s`,
/// not its Debug form).
pub fn frames_to_js(frames: &[Frame]) -> Result<JsValue, JsValue> {
	let response_frames = convert_frames(frames);

	let js_array = Array::new();

	for response_frame in &response_frames {
		let row_count = response_frame.columns.first().map_or(0, |c| c.payload.len());

		for row_idx in 0..row_count {
			let row_obj = Object::new();

			for column in &response_frame.columns {
				let js_value = JsValue::from_str(&column.payload[row_idx]);
				Reflect::set(&row_obj, &JsValue::from_str(&column.name), &js_value)?;
			}

			js_array.push(&row_obj);
		}
	}

	Ok(js_array.into())
}

pub fn parse_params(params_js: JsValue) -> Result<Params, JsValue> {
	if params_js.is_null() || params_js.is_undefined() {
		return Ok(Params::None);
	}

	let json_str = JSON::stringify(&params_js).map_err(|_| JsError::from_message("Failed to stringify params"))?;

	let json_str: String = json_str.into();

	let _json_value: JsonValue = json_from_str(&json_str).map_err(|e| JsError::from_error(&e))?;

	// TODO: Implement proper Params conversion
	Ok(Params::None)
}

#[allow(unused)]
pub fn log(message: &str) {
	console::log_1(&JsValue::from_str(message));
}

#[allow(unused)]
pub fn error(message: &str) {
	console::error_1(&JsValue::from_str(message));
}
