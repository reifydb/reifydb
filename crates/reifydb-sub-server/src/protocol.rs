// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use base64::{Engine, prelude::BASE64_STANDARD};
use sha1::{Sha1, digest::Digest};

// === HTTP -> WebSocket handshake helpers ===

pub fn find_header_end(buf: &[u8]) -> Option<usize> {
	let pattern = b"\r\n\r\n";
	buf.windows(4).position(|w| w == pattern).map(|i| i + 4)
}

pub fn build_ws_response(
	req_bytes: &[u8],
) -> Result<(Vec<u8>, String), Box<dyn std::error::Error>> {
	let mut headers = [httparse::EMPTY_HEADER; 32];
	let mut req = httparse::Request::new(&mut headers);
	let status = req.parse(req_bytes)?;

	if status.is_partial() {
		return Err("partial HTTP request".into());
	}

	if req.method != Some("GET") || req.version != Some(1) {
		return Err("invalid HTTP method/version".into());
	}

	let mut key: Option<&[u8]> = None;
	let mut upgrade_ok = false;
	let mut conn_upgrade = false;
	let mut version13 = false;

	for header in req.headers.iter() {
		match header.name.to_ascii_lowercase().as_str() {
			"sec-websocket-key" => key = Some(header.value),
			"upgrade" => {
				if eq_case_insensitive(
					header.value,
					b"websocket",
				) {
					upgrade_ok = true;
				}
			}
			"connection" => {
				if bytes_contains_ci(header.value, b"upgrade") {
					conn_upgrade = true;
				}
			}
			"sec-websocket-version" => {
				if eq_case_insensitive(header.value, b"13") {
					version13 = true;
				}
			}
			_ => {}
		}
	}

	if !(upgrade_ok && conn_upgrade && version13) {
		return Err("missing/invalid WebSocket upgrade headers".into());
	}

	let key = key.ok_or("missing Sec-WebSocket-Key")?;
	let accept = compute_accept_key(key);

	let response = format!(
		"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n",
		accept
	);

	Ok((response.into_bytes(), accept))
}

fn compute_accept_key(sec_websocket_key: &[u8]) -> String {
	const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
	let mut sha1 = Sha1::new();
	sha1.update(sec_websocket_key);
	sha1.update(GUID.as_bytes());
	let result = sha1.finalize();
	BASE64_STANDARD.encode(result)
}

fn eq_case_insensitive(a: &[u8], b: &[u8]) -> bool {
	a.eq_ignore_ascii_case(b)
}

fn bytes_contains_ci(haystack: &[u8], needle: &[u8]) -> bool {
	haystack.windows(needle.len())
		.any(|window| window.eq_ignore_ascii_case(needle))
}

// === WebSocket Frame Handling ===

pub fn parse_ws_frame(
	data: &[u8],
) -> Result<Option<(u8, Vec<u8>)>, Box<dyn std::error::Error>> {
	if data.len() < 2 {
		return Ok(None);
	}

	let first_byte = data[0];
	let second_byte = data[1];

	let _fin = (first_byte & 0x80) != 0;
	let opcode = first_byte & 0x0F;
	let masked = (second_byte & 0x80) != 0;
	let mut payload_len = (second_byte & 0x7F) as usize;

	let mut header_len = 2;

	// Extended payload length
	if payload_len == 126 {
		if data.len() < 4 {
			return Ok(None);
		}
		payload_len = u16::from_be_bytes([data[2], data[3]]) as usize;
		header_len = 4;
	} else if payload_len == 127 {
		if data.len() < 10 {
			return Ok(None);
		}
		payload_len = u64::from_be_bytes([
			data[2], data[3], data[4], data[5], data[6], data[7],
			data[8], data[9],
		]) as usize;
		header_len = 10;
	}

	// Masking key
	let mask_key = if masked {
		if data.len() < header_len + 4 {
			return Ok(None);
		}
		let key = [
			data[header_len],
			data[header_len + 1],
			data[header_len + 2],
			data[header_len + 3],
		];
		header_len += 4;
		Some(key)
	} else {
		None
	};

	// Check if we have the full payload
	if data.len() < header_len + payload_len {
		return Ok(None);
	}

	// Extract payload
	let mut payload = data[header_len..header_len + payload_len].to_vec();

	// Unmask if needed
	if let Some(mask) = mask_key {
		for (i, byte) in payload.iter_mut().enumerate() {
			*byte ^= mask[i % 4];
		}
	}

	Ok(Some((opcode, payload)))
}

pub fn build_ws_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
	let mut frame = Vec::new();

	// First byte: FIN=1, RSV=0, opcode
	frame.push(0x80 | (opcode & 0x0F));

	// Second byte and extended length
	let payload_len = payload.len();
	if payload_len < 126 {
		frame.push(payload_len as u8);
	} else if payload_len <= 65535 {
		frame.push(126);
		frame.extend_from_slice(&(payload_len as u16).to_be_bytes());
	} else {
		frame.push(127);
		frame.extend_from_slice(&(payload_len as u64).to_be_bytes());
	}

	// Payload (server frames are not masked)
	frame.extend_from_slice(payload);

	frame
}

// === Application Protocol ===

#[derive(serde::Serialize, serde::Deserialize)]
pub struct RequestMsg {
	#[serde(default)]
	pub q: String,
}

#[derive(serde::Serialize)]
pub struct ResponseMsg {
	pub ok: bool,
	pub result: String,
}

pub fn handle_request(text: &str) -> ResponseMsg {
	match serde_json::from_str::<RequestMsg>(text) {
		Ok(req) => {
			println!("Processing request: {}", req.q);
			ResponseMsg {
				ok: true,
				result: format!("Processed: {}", req.q),
			}
		}
		Err(e) => {
			eprintln!("Failed to parse request: {}", e);
			ResponseMsg {
				ok: false,
				result: format!("Parse error: {}", e),
			}
		}
	}
}
