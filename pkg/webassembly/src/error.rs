// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Error handling for WASM bindings

use std::fmt::Display;

use wasm_bindgen::prelude::*;

/// JavaScript-compatible error wrapper
#[wasm_bindgen]
pub struct JsError {
	message: String,
}

impl JsError {
	/// Create a JsError from any error type
	pub fn from_error<E: Display>(error: &E) -> JsValue {
		let message = error.to_string();
		JsValue::from_str(&message)
	}

	/// Create a JsError from a string message
	pub fn from_str(message: &str) -> JsValue {
		JsValue::from_str(message)
	}
}

#[wasm_bindgen]
impl JsError {
	/// Get the error message
	#[wasm_bindgen(getter)]
	pub fn message(&self) -> String {
		self.message.clone()
	}
}
