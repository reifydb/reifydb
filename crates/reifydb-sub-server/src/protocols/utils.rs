// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::util::base64;
use serde::{Deserialize, Serialize};
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
	let mut hasher = Sha1::new();
	hasher.update(combined.as_bytes());
	let hash = hasher.finalize();

	let accept = base64::Engine::STANDARD.encode(&hash);

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

// === WebSocket frame parsing and building ===

pub fn parse_ws_frame(
	data: &[u8],
) -> Result<Option<(u8, Vec<u8>)>, Box<dyn std::error::Error>> {
	if data.len() < 2 {
		return Ok(None);
	}

	let first_byte = data[0];
	let second_byte = data[1];

	let fin = (first_byte & 0x80) != 0;
	let opcode = first_byte & 0x0F;
	let masked = (second_byte & 0x80) != 0;
	let mut payload_len = (second_byte & 0x7F) as usize;

	let mut pos = 2;

	// Extended payload length
	if payload_len == 126 {
		if data.len() < pos + 2 {
			return Ok(None);
		}
		payload_len =
			u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
		pos += 2;
	} else if payload_len == 127 {
		if data.len() < pos + 8 {
			return Ok(None);
		}
		payload_len = u64::from_be_bytes([
			data[pos],
			data[pos + 1],
			data[pos + 2],
			data[pos + 3],
			data[pos + 4],
			data[pos + 5],
			data[pos + 6],
			data[pos + 7],
		]) as usize;
		pos += 8;
	}

	// Masking key
	let mask_key = if masked {
		if data.len() < pos + 4 {
			return Ok(None);
		}
		let key = [
			data[pos],
			data[pos + 1],
			data[pos + 2],
			data[pos + 3],
		];
		pos += 4;
		Some(key)
	} else {
		None
	};

	// Payload
	if data.len() < pos + payload_len {
		return Ok(None);
	}

	let mut payload = data[pos..pos + payload_len].to_vec();

	// Unmask payload if necessary
	if let Some(mask) = mask_key {
		for (i, byte) in payload.iter_mut().enumerate() {
			*byte ^= mask[i % 4];
		}
	}

	if !fin {
		// Handle fragmented frames - for now, just return what we have
		return Ok(Some((opcode, payload)));
	}

	Ok(Some((opcode, payload)))
}

pub fn build_ws_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
	let mut frame = Vec::new();

	// First byte: FIN = 1, RSV = 0, OPCODE = opcode
	frame.push(0x80 | opcode);

	// Payload length
	let payload_len = payload.len();
	if payload_len < 126 {
		frame.push(payload_len as u8);
	} else if payload_len < 65536 {
		frame.push(126);
		frame.extend_from_slice(&(payload_len as u16).to_be_bytes());
	} else {
		frame.push(127);
		frame.extend_from_slice(&(payload_len as u64).to_be_bytes());
	}

	// Payload (no masking for server->client frames)
	frame.extend_from_slice(payload);

	frame
}

// === Message types for query handling ===

#[derive(Debug, Deserialize)]
pub struct RequestMsg {
	pub q: String,
}

#[derive(Debug, Serialize)]
pub struct ResponseMsg {
	pub ok: bool,
	pub result: String,
}
