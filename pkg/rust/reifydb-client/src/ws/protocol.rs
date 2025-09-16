// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use reifydb_hash::sha1;

/// Find the end of HTTP headers (double CRLF)
pub(crate) fn find_header_end(data: &[u8]) -> Option<usize> {
	let pattern = b"\r\n\r\n";
	data.windows(4).position(|window| window == pattern).map(|pos| pos + 4)
}

/// Generate a random WebSocket key for handshake
pub(crate) fn generate_websocket_key() -> String {
	let random_bytes: Vec<u8> = (0..16).map(|_| rand::random::<u8>()).collect();
	base64_encode(&random_bytes)
}

/// Calculate the expected Sec-WebSocket-Accept value
pub(crate) fn calculate_accept_key(key: &str) -> String {
	const MAGIC: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
	let combined = format!("{}{}", key, MAGIC);

	let hash = sha1(combined.as_bytes());

	base64_encode(&hash.0)
}

/// Simple base64 encoding
fn base64_encode(data: &[u8]) -> String {
	const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	let mut result = String::new();

	let mut i = 0;
	while i < data.len() {
		let b1 = data[i];
		let b2 = if i + 1 < data.len() {
			data[i + 1]
		} else {
			0
		};
		let b3 = if i + 2 < data.len() {
			data[i + 2]
		} else {
			0
		};

		result.push(TABLE[(b1 >> 2) as usize] as char);
		result.push(TABLE[(((b1 & 0x03) << 4) | (b2 >> 4)) as usize] as char);

		if i + 1 < data.len() {
			result.push(TABLE[(((b2 & 0x0F) << 2) | (b3 >> 6)) as usize] as char);
		} else {
			result.push('=');
		}

		if i + 2 < data.len() {
			result.push(TABLE[(b3 & 0x3F) as usize] as char);
		} else {
			result.push('=');
		}

		i += 3;
	}

	result
}

/// Parse a WebSocket frame from the buffer
pub(crate) fn parse_ws_frame(data: &[u8]) -> Result<Option<(u8, Vec<u8>)>, Box<dyn std::error::Error>> {
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
		payload_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
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
		let key = [data[pos], data[pos + 1], data[pos + 2], data[pos + 3]];
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

/// Build a WebSocket frame
pub(crate) fn build_ws_frame(opcode: u8, payload: &[u8], mask: bool) -> Vec<u8> {
	let mut frame = Vec::new();

	// First byte: FIN = 1, RSV = 0, OPCODE = opcode
	frame.push(0x80 | opcode);

	// Payload length (with mask bit for client->server)
	let payload_len = payload.len();
	if payload_len < 126 {
		let len_byte = if mask {
			0x80 | payload_len as u8
		} else {
			payload_len as u8
		};
		frame.push(len_byte);
	} else if payload_len < 65536 {
		frame.push(if mask {
			0xFE
		} else {
			126
		});
		frame.extend_from_slice(&(payload_len as u16).to_be_bytes());
	} else {
		frame.push(if mask {
			0xFF
		} else {
			127
		});
		frame.extend_from_slice(&(payload_len as u64).to_be_bytes());
	}

	// Masking key and masked payload (for client->server frames)
	if mask {
		// Generate random masking key
		let mask_key: [u8; 4] = [rand::random(), rand::random(), rand::random(), rand::random()];
		frame.extend_from_slice(&mask_key);

		// Mask and add payload
		for (i, &byte) in payload.iter().enumerate() {
			frame.push(byte ^ mask_key[i % 4]);
		}
	} else {
		// No masking (server->client frames)
		frame.extend_from_slice(payload);
	}

	frame
}

/// Calculate the size of a WebSocket frame
pub(crate) fn calculate_frame_size(payload: &[u8], masked: bool) -> usize {
	let mut size = 2; // First two bytes
	let payload_len = payload.len();

	if payload_len >= 126 && payload_len < 65536 {
		size += 2;
	} else if payload_len >= 65536 {
		size += 8;
	}

	if masked {
		size += 4; // Masking key
	}

	size + payload_len
}

// Simple random number generation for WebSocket key and masking
mod rand {
	use std::time::{SystemTime, UNIX_EPOCH};

	static mut SEED: u64 = 0;

	pub fn random<T>() -> T
	where
		T: Random,
	{
		T::random()
	}

	pub trait Random {
		fn random() -> Self;
	}

	impl Random for u8 {
		fn random() -> Self {
			unsafe {
				if SEED == 0 {
					SEED = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
				}
				SEED = SEED.wrapping_mul(1664525).wrapping_add(1013904223);
				(SEED >> 24) as u8
			}
		}
	}
}
