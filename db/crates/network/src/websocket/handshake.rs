// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_hash::sha1;
use reifydb_type::util::base64;

pub fn build_ws_response(req_bytes: &[u8]) -> Result<(Vec<u8>, String), Box<dyn std::error::Error>> {
	let mut headers = [httparse::EMPTY_HEADER; 32];
	let mut req = httparse::Request::new(&mut headers);
	let status = req.parse(req_bytes)?;

	if status.is_partial() {
		return Err("partial request".into());
	}

	// Find Sec-WebSocket-Key
	let sec_key = req
		.headers
		.iter()
		.find(|h| h.name.eq_ignore_ascii_case("Sec-WebSocket-Key"))
		.ok_or("missing Sec-WebSocket-Key header")?
		.value;

	let sec_key = std::str::from_utf8(sec_key)?;

	// WebSocket magic string
	let magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
	let combined = format!("{}{}", sec_key, magic);

	// Create SHA1 hash and base64 encode it
	let hash = sha1(combined.as_bytes());

	let accept = base64::Engine::STANDARD.encode(&hash.0);

	// Build response
	let response = format!(
		"HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {}\r\n\
         \r\n",
		accept
	);

	Ok((response.into_bytes(), sec_key.to_string()))
}
