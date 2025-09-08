// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use super::HttpRequest;

pub fn parse_request(data: &[u8]) -> Result<HttpRequest, String> {
	let mut headers_buf = [httparse::EMPTY_HEADER; 32];
	let mut req = httparse::Request::new(&mut headers_buf);

	let status =
		req.parse(data).map_err(|e| format!("Parse error: {:?}", e))?;

	if status.is_partial() {
		return Err("Incomplete request".to_string());
	}

	let method = req.method.ok_or("Missing method")?.to_string();
	let path = req.path.ok_or("Missing path")?.to_string();

	let mut headers = HashMap::new();
	for header in req.headers {
		let key = header.name.to_lowercase();
		let value = String::from_utf8_lossy(header.value).to_string();
		headers.insert(key, value);
	}

	// Extract body if present
	let header_len = status.unwrap();
	let body = if data.len() > header_len {
		data[header_len..].to_vec()
	} else {
		Vec::new()
	};

	Ok(HttpRequest::new(method, path, headers, body))
}
