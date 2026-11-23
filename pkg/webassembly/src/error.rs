// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Display;

use js_sys::{Error as JsErrorObject, JSON, Reflect};
use reifydb_value::error::ValueError;
use serde_json::to_string as json_to_string;
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

	pub fn from_diagnostic(error: &ValueError) -> JsValue {
		let js_error = JsErrorObject::new(&error.to_string());
		let json = json_to_string(&**error).expect("failed to serialize diagnostic to json");
		let diagnostic = JSON::parse(&json).expect("failed to parse diagnostic json");
		Reflect::set(&js_error, &JsValue::from_str("diagnostic"), &diagnostic)
			.expect("failed to set diagnostic on error");
		js_error.into()
	}
}

#[wasm_bindgen]
impl JsError {
	#[wasm_bindgen(getter)]
	pub fn message(&self) -> String {
		self.message.clone()
	}
}
