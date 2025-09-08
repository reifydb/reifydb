// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use super::HttpResponse;

pub struct HttpResponseBuilder {
	status_code: u16,
	status_text: String,
	headers: HashMap<String, String>,
	body: Vec<u8>,
}

impl HttpResponseBuilder {
	pub fn new() -> Self {
		Self {
			status_code: 200,
			status_text: "OK".to_string(),
			headers: HashMap::new(),
			body: Vec::new(),
		}
	}

	pub fn status(mut self, code: u16, text: &str) -> Self {
		self.status_code = code;
		self.status_text = text.to_string();
		self
	}

	pub fn header(mut self, key: &str, value: &str) -> Self {
		self.headers.insert(key.to_string(), value.to_string());
		self
	}

	pub fn body(mut self, body: Vec<u8>) -> Self {
		self.body = body;
		self
	}

	pub fn json(mut self, json: &str) -> Self {
		self.headers.insert(
			"Content-Type".to_string(),
			"application/json".to_string(),
		);
		self.body = json.as_bytes().to_vec();
		self
	}

	pub fn html(mut self, html: &str) -> Self {
		self.headers.insert(
			"Content-Type".to_string(),
			"text/html; charset=utf-8".to_string(),
		);
		self.body = html.as_bytes().to_vec();
		self
	}

	pub fn build(self) -> HttpResponse {
		HttpResponse {
			status_code: self.status_code,
			status_text: self.status_text,
			headers: self.headers,
			body: self.body,
		}
	}
}

impl Default for HttpResponseBuilder {
	fn default() -> Self {
		Self::new()
	}
}
