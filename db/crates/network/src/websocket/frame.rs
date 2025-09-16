// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[derive(Debug, Clone)]
pub struct WebSocketFrame {
	pub fin: bool,
	pub opcode: WebSocketOpcode,
	pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WebSocketOpcode {
	Continuation,
	Text,
	Binary,
	Close,
	Ping,
	Pong,
	Unknown(u8),
}

impl From<u8> for WebSocketOpcode {
	fn from(value: u8) -> Self {
		match value {
			0x0 => Self::Continuation,
			0x1 => Self::Text,
			0x2 => Self::Binary,
			0x8 => Self::Close,
			0x9 => Self::Ping,
			0xA => Self::Pong,
			other => Self::Unknown(other),
		}
	}
}

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
