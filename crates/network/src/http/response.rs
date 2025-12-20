// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct HttpResponse {
	pub status_code: u16,
	pub status_text: String,
	pub headers: HashMap<String, String>,
	pub body: Vec<u8>,
}

impl HttpResponse {
	pub fn new(status_code: u16, status_text: String) -> Self {
		Self {
			status_code,
			status_text,
			headers: HashMap::new(),
			body: Vec::new(),
		}
	}

	pub fn ok() -> Self {
		Self::new(200, "OK".to_string())
	}

	pub fn not_found() -> Self {
		Self::new(404, "Not Found".to_string())
	}

	pub fn bad_request() -> Self {
		Self::new(400, "Bad Request".to_string())
	}

	pub fn internal_server_error() -> Self {
		Self::new(500, "Internal Server Error".to_string())
	}

	pub fn with_body(mut self, body: Vec<u8>) -> Self {
		self.body = body;
		self
	}

	pub fn with_json(mut self, json: &str) -> Self {
		self.headers.insert("Content-Type".to_string(), "application/json".to_string());
		self.body = json.as_bytes().to_vec();
		self
	}

	pub fn with_html(mut self, html: &str) -> Self {
		self.headers.insert("Content-Type".to_string(), "text/html; charset=utf-8".to_string());
		self.body = html.as_bytes().to_vec();
		self
	}

	pub fn with_header(mut self, key: String, value: String) -> Self {
		self.headers.insert(key, value);
		self
	}

	pub fn with_cors_allow_all(mut self) -> Self {
		self.headers.insert("Access-Control-Allow-Origin".to_string(), "*".to_string());
		self.headers.insert(
			"Access-Control-Allow-Methods".to_string(),
			"GET, POST, PUT, DELETE, OPTIONS".to_string(),
		);
		self.headers.insert("Access-Control-Allow-Headers".to_string(), "*".to_string());
		self.headers.insert("Access-Control-Max-Age".to_string(), "86400".to_string());
		self
	}

	pub fn to_bytes(&self) -> Vec<u8> {
		let mut response = format!("HTTP/1.1 {} {}\r\n", self.status_code, self.status_text);

		// Add content-length header
		response.push_str(&format!("Content-Length: {}\r\n", self.body.len()));

		// Add custom headers
		for (key, value) in &self.headers {
			response.push_str(&format!("{}: {}\r\n", key, value));
		}

		// End headers
		response.push_str("\r\n");

		// Combine header and body
		let mut bytes = response.into_bytes();
		bytes.extend_from_slice(&self.body);
		bytes
	}
}
