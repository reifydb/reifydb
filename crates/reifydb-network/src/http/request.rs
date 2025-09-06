// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct HttpRequest {
	pub method: String,
	pub path: String,
	pub headers: HashMap<String, String>,
	pub body: Vec<u8>,
}

impl HttpRequest {
	pub fn new(
		method: String,
		path: String,
		headers: HashMap<String, String>,
		body: Vec<u8>,
	) -> Self {
		Self {
			method,
			path,
			headers,
			body,
		}
	}

	pub fn get_header(&self, name: &str) -> Option<&String> {
		self.headers.get(&name.to_lowercase())
	}

	pub fn content_length(&self) -> Option<usize> {
		self.get_header("content-length").and_then(|v| v.parse().ok())
	}

	pub fn is_websocket_upgrade(&self) -> bool {
		self.get_header("upgrade")
			.map(|v| v.to_lowercase() == "websocket")
			.unwrap_or(false) && self
			.get_header("connection")
			.map(|v| {
				let v_lower = v.to_lowercase();
				v_lower.contains("upgrade")
			})
			.unwrap_or(false)
	}
}
