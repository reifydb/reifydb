// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Display;

use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct JsError {
	message: String,
}

impl JsError {
	pub fn from_error<E: Display>(error: &E) -> JsValue {
		let message = error.to_string();
		JsValue::from_str(&message)
	}

	pub fn from_message(message: &str) -> JsValue {
		JsValue::from_str(message)
	}
}

#[wasm_bindgen]
impl JsError {
	#[wasm_bindgen(getter)]
	pub fn message(&self) -> String {
		self.message.clone()
	}
}
