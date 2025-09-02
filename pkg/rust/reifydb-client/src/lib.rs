// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

mod client;
mod domain;
pub mod session;

use std::{
	collections::HashMap,
	io::{Read, Write},
	net::TcpStream,
};

// Re-export main client and session types
pub use client::Client;
pub use domain::{Frame, FrameColumn};
use reifydb_type::diagnostic::Diagnostic;
// Re-export types from reifydb
pub use reifydb_type::{OrderedF32, OrderedF64, Type, Value};
use serde::{Deserialize, Serialize};
pub use session::{
	BlockingSession, CallbackSession, ChannelSession, CommandResult,
	QueryResult, ResponseMessage,
};
use sha1::{Digest, Sha1};

// ============================================================================
// Request Types (matching server)
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
	pub id: String,
	#[serde(flatten)]
	pub payload: RequestPayload,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum RequestPayload {
	Auth(AuthRequest),
	Command(CommandRequest),
	Query(QueryRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthRequest {
	pub token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandRequest {
	pub statements: Vec<String>,
	pub params: Option<Params>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
	pub statements: Vec<String>,
	pub params: Option<Params>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Params {
	Positional(Vec<Value>),
	Named(HashMap<String, Value>),
}

// ============================================================================
// Response Types (matching server)
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
	pub id: String,
	#[serde(flatten)]
	pub payload: ResponsePayload,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum ResponsePayload {
	Auth(AuthResponse),
	Err(ErrResponse),
	Command(CommandResponse),
	Query(QueryResponse),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthResponse {}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrResponse {
	pub diagnostic: Diagnostic,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandResponse {
	pub frames: Vec<WebsocketFrame>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResponse {
	pub frames: Vec<WebsocketFrame>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketFrame {
	pub columns: Vec<WebsocketColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketColumn {
	pub schema: Option<String>,
	pub store: Option<String>,
	pub name: String,
	pub r#type: Type,
	pub data: Vec<String>,
}

// ============================================================================
// WebSocket Client Implementation
// ============================================================================

pub(crate) struct WebSocketClient {
	pub(crate) stream: TcpStream,
	read_buffer: Vec<u8>,
	pub(crate) is_connected: bool,
}

impl WebSocketClient {
	/// Create a new WebSocket client and connect to the specified address
	/// Supports both plain addresses (e.g., "127.0.0.1:8080") and WebSocket
	/// URLs (e.g., "ws://127.0.0.1:8080")
	pub fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
		// Parse the address, removing ws:// or wss:// prefix if present
		let socket_addr =
			if addr.starts_with("ws://") {
				&addr[5..] // Remove "ws://"
			} else if addr.starts_with("wss://") {
				return Err("WSS (secure WebSocket) is not yet supported".into());
			} else {
				addr
			};

		// Parse address and connect
		let stream = TcpStream::connect(socket_addr)?;
		stream.set_nonblocking(true)?;

		let mut client = WebSocketClient {
			stream,
			read_buffer: Vec::with_capacity(4096),
			is_connected: false,
		};

		// Perform WebSocket handshake
		client.handshake()?;

		Ok(client)
	}

	/// Perform WebSocket handshake
	fn handshake(&mut self) -> Result<(), Box<dyn std::error::Error>> {
		// Generate WebSocket key
		let key = generate_websocket_key();

		// Build handshake request
		let request = format!(
			"GET / HTTP/1.1\r\n\
             Host: localhost\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: {}\r\n\
             Sec-WebSocket-Version: 13\r\n\
             \r\n",
			key
		);

		// Send handshake
		self.stream.write_all(request.as_bytes())?;
		self.stream.flush()?;

		// Read response with timeout
		let mut response = Vec::new();
		let mut buffer = [0u8; 1024];
		let start = std::time::Instant::now();
		let timeout = std::time::Duration::from_secs(5);

		loop {
			match self.stream.read(&mut buffer) {
				Ok(0) => return Err(
					"Connection closed during handshake"
						.into(),
				),
				Ok(n) => {
					response.extend_from_slice(
						&buffer[..n],
					);

					// Check if we have the complete HTTP
					// response
					if let Some(end_pos) =
						find_header_end(&response)
					{
						response.truncate(end_pos);
						break;
					}
				}
				Err(e) if e.kind()
					== std::io::ErrorKind::WouldBlock =>
				{
					// No data available yet
					if start.elapsed() > timeout {
						return Err(
							"Handshake timeout"
								.into(),
						);
					}
					std::thread::sleep(std::time::Duration::from_millis(10));
					continue;
				}
				Err(e) => return Err(e.into()),
			}
		}

		// Verify handshake response
		let response_str = String::from_utf8_lossy(&response);
		if !response_str.contains("HTTP/1.1 101") {
			return Err(format!(
				"Invalid handshake response: {}",
				response_str
			)
			.into());
		}

		// Verify Sec-WebSocket-Accept
		let expected_accept = calculate_accept_key(&key);
		if !response_str.contains(&format!(
			"Sec-WebSocket-Accept: {}",
			expected_accept
		)) {
			return Err("Invalid Sec-WebSocket-Accept".into());
		}

		self.is_connected = true;
		Ok(())
	}

	/// Send a request over the WebSocket connection
	pub(crate) fn send_request(
		&mut self,
		request: &Request,
	) -> Result<(), Box<dyn std::error::Error>> {
		if !self.is_connected {
			return Err("Not connected".into());
		}

		// Serialize request to JSON
		let json = serde_json::to_string(request)?;
		let payload = json.as_bytes();

		// Build WebSocket frame (text frame, opcode = 1)
		let frame = build_ws_frame(0x01, payload, true);

		// Send frame
		self.stream.write_all(&frame)?;
		self.stream.flush()?;

		Ok(())
	}

	/// Receive a response from the WebSocket connection
	pub fn receive(
		&mut self,
	) -> Result<Option<Response>, Box<dyn std::error::Error>> {
		if !self.is_connected {
			return Err("Not connected".into());
		}

		// Read data into buffer
		let mut buf = vec![0u8; 4096];
		match self.stream.read(&mut buf) {
			Ok(0) => {
				self.is_connected = false;
				return Err("Connection closed".into());
			}
			Ok(n) => {
				self.read_buffer.extend_from_slice(&buf[..n]);
			}
			Err(e) if e.kind()
				== std::io::ErrorKind::WouldBlock =>
			{
				// No data available
				return Ok(None);
			}
			Err(e) => return Err(e.into()),
		}

		// Try to parse WebSocket frame
		if let Some((opcode, payload)) =
			parse_ws_frame(&self.read_buffer)?
		{
			// Remove parsed frame from buffer
			let frame_size = calculate_frame_size(&payload, false);
			self.read_buffer.drain(..frame_size);

			match opcode {
				0x01 | 0x02 => {
					// Text or binary frame
					let response: Response =
						serde_json::from_slice(
							&payload,
						)?;
					return Ok(Some(response));
				}
				0x08 => {
					// Close frame
					self.is_connected = false;
					return Err(
						"Connection closed by server"
							.into(),
					);
				}
				0x09 => {
					// Ping frame - respond with pong
					let pong = build_ws_frame(
						0x0A, &payload, true,
					);
					self.stream.write_all(&pong)?;
					self.stream.flush()?;
				}
				0x0A => {
					// Pong frame - ignore
				}
				_ => {
					// Unknown opcode
					return Err(format!(
						"Unknown opcode: {}",
						opcode
					)
					.into());
				}
			}
		}

		Ok(None)
	}

	/// Close the WebSocket connection
	pub fn close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
		if self.is_connected {
			// Send close frame
			let close_frame = build_ws_frame(0x08, &[], true);
			self.stream.write_all(&close_frame)?;
			self.stream.flush()?;
			self.is_connected = false;
		}
		Ok(())
	}

	/// Check if the client is connected
	pub fn is_connected(&self) -> bool {
		self.is_connected
	}
}

impl Drop for WebSocketClient {
	fn drop(&mut self) {
		let _ = self.close();
	}
}

// ============================================================================
// WebSocket Protocol Utilities
// ============================================================================

/// Find the end of HTTP headers (double CRLF)
fn find_header_end(data: &[u8]) -> Option<usize> {
	let pattern = b"\r\n\r\n";
	data.windows(4).position(|window| window == pattern).map(|pos| pos + 4)
}

/// Generate a random WebSocket key for handshake
fn generate_websocket_key() -> String {
	let random_bytes: Vec<u8> =
		(0..16).map(|_| rand::random::<u8>()).collect();
	base64_encode(&random_bytes)
}

/// Calculate the expected Sec-WebSocket-Accept value
fn calculate_accept_key(key: &str) -> String {
	const MAGIC: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
	let combined = format!("{}{}", key, MAGIC);

	let mut hasher = Sha1::new();
	hasher.update(combined.as_bytes());
	let hash = hasher.finalize();

	base64_encode(&hash)
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
		result.push(TABLE[(((b1 & 0x03) << 4) | (b2 >> 4)) as usize]
			as char);

		if i + 1 < data.len() {
			result.push(TABLE
				[(((b2 & 0x0F) << 2) | (b3 >> 6)) as usize]
				as char);
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
fn parse_ws_frame(
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

/// Build a WebSocket frame
pub(crate) fn build_ws_frame(
	opcode: u8,
	payload: &[u8],
	mask: bool,
) -> Vec<u8> {
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
		let mask_key: [u8; 4] = [
			rand::random(),
			rand::random(),
			rand::random(),
			rand::random(),
		];
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
fn calculate_frame_size(payload: &[u8], masked: bool) -> usize {
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
					SEED = SystemTime::now()
						.duration_since(UNIX_EPOCH)
						.unwrap()
						.as_nanos() as u64;
				}
				SEED = SEED
					.wrapping_mul(1664525)
					.wrapping_add(1013904223);
				(SEED >> 24) as u8
			}
		}
	}
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_websocket_key_generation() {
		let key1 = generate_websocket_key();
		let key2 = generate_websocket_key();

		assert_ne!(key1, key2);
		assert_eq!(key1.len(), 24); // Base64 encoded 16 bytes = 24 chars
	}

	#[test]
	fn test_frame_building_and_parsing() {
		let payload = b"Hello, WebSocket!";
		let frame = build_ws_frame(0x01, payload, false);

		let parsed = parse_ws_frame(&frame).unwrap().unwrap();
		assert_eq!(parsed.0, 0x01);
		assert_eq!(parsed.1, payload);
	}

	#[test]
	fn test_request_serialization() {
		let request = Request {
			id: "123".to_string(),
			payload: RequestPayload::Auth(AuthRequest {
				token: Some("test-token".to_string()),
			}),
		};

		let json = serde_json::to_string(&request).unwrap();
		let parsed: Request = serde_json::from_str(&json).unwrap();

		assert_eq!(parsed.id, "123");
	}
}
