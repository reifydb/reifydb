// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Utility functions for WASM bindings

use wasm_bindgen::prelude::*;
use reifydb_type::value::frame::frame::Frame;
use reifydb_type::value::Value;
use reifydb_type::params::Params;
use crate::error::JsError;

/// Convert Frames to JavaScript array of objects
///
/// Transforms columnar Frame data into row-oriented JavaScript objects
pub fn frames_to_js(frames: &[Frame]) -> Result<JsValue, JsValue> {
	let mut all_results = Vec::new();

	for frame in frames {
		// Get row count from first column
		let row_count = frame.columns.first().map_or(0, |c| c.data.len());

		// Convert each row to a JavaScript object
		for row_idx in 0..row_count {
			let row_obj = js_sys::Object::new();

			for column in &frame.columns {
				// Get value from column
				let value = column.data.get_value(row_idx);

				// Convert to JavaScript value
				let js_value = value_to_js(&value)?;

				// Set property on object
				js_sys::Reflect::set(
					&row_obj,
					&JsValue::from_str(&column.name),
					&js_value,
				)?;
			}

			all_results.push(row_obj);
		}
	}

	// Convert Vec<Object> to JavaScript Array
	let js_array = js_sys::Array::new();
	for obj in all_results {
		js_array.push(&obj);
	}

	Ok(js_array.into())
}

/// Convert ReifyDB Value to JavaScript value
fn value_to_js(value: &Value) -> Result<JsValue, JsValue> {
	match value {
		Value::Undefined => Ok(JsValue::NULL),
		Value::Boolean(b) => Ok(JsValue::from(*b)),
		Value::Int1(n) => Ok(JsValue::from(*n)),
		Value::Int2(n) => Ok(JsValue::from(*n)),
		Value::Int4(n) => Ok(JsValue::from(*n)),
		Value::Int8(n) => {
			// JavaScript numbers are f64, which can't represent all i64 values precisely
			// For large numbers, we convert to f64 and accept precision loss
			if *n >= -(2_i64.pow(53)) && *n <= 2_i64.pow(53) {
				Ok(JsValue::from(*n as f64))
			} else {
				// For very large numbers, convert to string to preserve exact value
				Ok(JsValue::from_str(&n.to_string()))
			}
		}
		Value::Int16(n) => {
			// i128 always needs string representation
			Ok(JsValue::from_str(&n.to_string()))
		}
		Value::Uint1(n) => Ok(JsValue::from(*n)),
		Value::Uint2(n) => Ok(JsValue::from(*n)),
		Value::Uint4(n) => Ok(JsValue::from(*n)),
		Value::Uint8(n) => {
			// Same precision handling as Int8
			if *n <= 2_u64.pow(53) {
				Ok(JsValue::from(*n as f64))
			} else {
				Ok(JsValue::from_str(&n.to_string()))
			}
		}
		Value::Uint16(n) => {
			// u128 always needs string representation
			Ok(JsValue::from_str(&n.to_string()))
		}
		Value::Float4(f) => Ok(JsValue::from(f.value())),
		Value::Float8(f) => Ok(JsValue::from(f.value())),
		Value::Utf8(s) => Ok(JsValue::from_str(s.as_str())),
		Value::Blob(blob) => {
			// Convert blob to Uint8Array
			let bytes = blob.as_bytes();
			let uint8_array = js_sys::Uint8Array::new_with_length(bytes.len() as u32);
			uint8_array.copy_from(bytes);
			Ok(uint8_array.into())
		}
		// Handle other Value variants as needed
		_ => {
			// For unsupported types, convert to string representation
			Ok(JsValue::from_str(&format!("{:?}", value)))
		}
	}
}

/// Parse JavaScript parameters to Rust Params
pub fn parse_params(params_js: JsValue) -> Result<Params, JsValue> {
	// If params is null or undefined, return Params::None
	if params_js.is_null() || params_js.is_undefined() {
		return Ok(Params::None);
	}

	// Try to parse as JSON
	let json_str = js_sys::JSON::stringify(&params_js)
		.map_err(|_| JsError::from_str("Failed to stringify params"))?;

	let json_str: String = json_str.into();

	// Parse JSON string to serde_json::Value
	let _json_value: serde_json::Value = serde_json::from_str(&json_str)
		.map_err(|e| JsError::from_error(&e))?;

	// Convert to Params
	// For now, we'll use Params::None if conversion is complex
	// TODO: Implement proper Params conversion
	Ok(Params::None)
}

/// Log a message to browser console
#[allow(unused)]
pub fn log(message: &str) {
	web_sys::console::log_1(&JsValue::from_str(message));
}

/// Log an error to browser console
#[allow(unused)]
pub fn error(message: &str) {
	web_sys::console::error_1(&JsValue::from_str(message));
}
