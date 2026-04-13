// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Utility functions for WASM bindings

use js_sys::{Array, JSON, Object, Reflect};
use reifydb_type::{params::Params, value::frame::frame::Frame};
use reifydb_wire_format::json::convert_frames;
use serde_json::{Value as JsonValue, from_str as json_from_str};
use wasm_bindgen::prelude::*;
use web_sys::console;

use crate::error::JsError;

/// Convert Frames to JavaScript array of objects
///
/// Uses the same `convert_frames` logic as the server to ensure identical
/// value formatting (e.g. Duration renders as `0s`, not Debug format).
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

/// Parse JavaScript parameters to Rust Params
pub fn parse_params(params_js: JsValue) -> Result<Params, JsValue> {
	// If params is null or undefined, return Params::None
	if params_js.is_null() || params_js.is_undefined() {
		return Ok(Params::None);
	}

	// Try to parse as JSON
	let json_str = JSON::stringify(&params_js).map_err(|_| JsError::from_message("Failed to stringify params"))?;

	let json_str: String = json_str.into();

	// Parse JSON string to JsonValue
	let _json_value: JsonValue = json_from_str(&json_str).map_err(|e| JsError::from_error(&e))?;

	// Convert to Params
	// For now, we'll use Params::None if conversion is complex
	// TODO: Implement proper Params conversion
	Ok(Params::None)
}

/// Log a message to browser console
#[allow(unused)]
pub fn log(message: &str) {
	console::log_1(&JsValue::from_str(message));
}

/// Log an error to browser console
#[allow(unused)]
pub fn error(message: &str) {
	console::error_1(&JsValue::from_str(message));
}
