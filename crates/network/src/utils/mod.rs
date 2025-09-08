// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

pub fn find_header_end(buf: &[u8]) -> Option<usize> {
	let pattern = b"\r\n\r\n";
	buf.windows(4).position(|w| w == pattern).map(|i| i + 4)
}

pub fn parse_headers(data: &[u8]) -> Result<HashMap<String, String>, String> {
	let request_str = String::from_utf8_lossy(data);
	let lines: Vec<&str> = request_str.lines().collect();

	let mut headers = HashMap::new();
	for line in lines.iter().skip(1) {
		if line.is_empty() {
			break;
		}
		if let Some(colon_pos) = line.find(':') {
			let key = line[..colon_pos].trim().to_lowercase();
			let value = line[colon_pos + 1..].trim().to_string();
			headers.insert(key, value);
		}
	}

	Ok(headers)
}
