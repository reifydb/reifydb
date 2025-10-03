// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::io::{Read, Write};

use reifydb_core::interface::{Engine, Identity};

use super::{CommandResponse, QueryResponse, Request, Response, ResponsePayload, WebSocketConnectionData, WsState};
use crate::{
	core::Connection,
	protocols::{
		ProtocolError, ProtocolHandler, ProtocolResult,
		convert::{convert_params, convert_result_to_frames},
		utils::{build_ws_frame, build_ws_response, find_header_end, parse_ws_frame},
	},
};

#[derive(Clone)]
pub struct WebSocketHandler;

impl WebSocketHandler {
	pub fn new() -> Self {
		Self
	}
}

impl ProtocolHandler for WebSocketHandler {
	fn name(&self) -> &'static str {
		"ws"
	}

	fn can_handle(&self, buffer: &[u8]) -> bool {
		// Check for WebSocket handshake signature
		if buffer.len() < 16 {
			return false;
		}

		let request = String::from_utf8_lossy(buffer);

		let request_lower = request.to_lowercase();

		request_lower.contains("get ")
			&& request_lower.contains("http/1.1")
			&& request_lower.contains("upgrade: websocket")
			&& (request_lower.contains("connection: upgrade")
				|| request_lower.contains("connection: keep-alive, upgrade"))
	}

	fn handle_connection(&self, conn: &mut Connection) -> ProtocolResult<()> {
		// Initialize WebSocket state
		let ws_state = WsState::Handshake(WebSocketConnectionData::new());
		conn.set_state(crate::core::ConnectionState::WebSocket(ws_state));
		Ok(())
	}

	fn handle_read(&self, conn: &mut Connection) -> ProtocolResult<()> {
		if let crate::core::ConnectionState::WebSocket(ws_state) = conn.state() {
			match ws_state {
				WsState::Handshake(_) => self.handle_handshake_read(conn),
				WsState::Active(_) => self.handle_ws_read(conn),
				WsState::Closed => Ok(()),
			}
		} else {
			Err(ProtocolError::InvalidFrame)
		}
	}

	fn handle_write(&self, conn: &mut Connection) -> ProtocolResult<()> {
		if let crate::core::ConnectionState::WebSocket(ws_state) = conn.state() {
			match ws_state {
				WsState::Handshake(_) => self.handle_handshake_write(conn),
				WsState::Active(_) => self.handle_ws_write(conn),
				WsState::Closed => Ok(()),
			}
		} else {
			Err(ProtocolError::InvalidFrame)
		}
	}

	fn should_close(&self, conn: &Connection) -> bool {
		matches!(
			conn.state(),
			crate::core::ConnectionState::WebSocket(WsState::Closed) | crate::core::ConnectionState::Closed
		)
	}
}

impl WebSocketHandler {
	fn handle_handshake_read(&self, conn: &mut Connection) -> ProtocolResult<()> {
		// First, check if we already have complete headers in the
		// buffer (from protocol detection)
		if !conn.buffer().is_empty() {
			if let Some(hlen) = find_header_end(conn.buffer()) {
				let (resp, _key) = build_ws_response(&conn.buffer()[..hlen])
					.map_err(|e| ProtocolError::Custom(format!("Handshake error: {}", e)))?;

				// Update WebSocket state with response
				if let crate::core::ConnectionState::WebSocket(WsState::Handshake(data)) =
					conn.state_mut()
				{
					data.handshake_response = Some(resp);
				}

				// Clear the handshake data from buffer
				conn.buffer_mut().drain(0..hlen);
				return Ok(());
			}
		}

		// If we don't have complete headers yet, read more data
		let mut buf = [0u8; 2048];
		loop {
			match conn.stream().read(&mut buf) {
				Ok(0) => return Err(ProtocolError::ConnectionClosed),
				Ok(n) => {
					conn.buffer_mut().extend_from_slice(&buf[..n]);
					if let Some(hlen) = find_header_end(conn.buffer()) {
						let (resp, _key) =
							build_ws_response(&conn.buffer()[..hlen]).map_err(|e| {
								ProtocolError::Custom(format!("Handshake error: {}", e))
							})?;

						// Update WebSocket state with response
						if let crate::core::ConnectionState::WebSocket(WsState::Handshake(
							data,
						)) = conn.state_mut()
						{
							data.handshake_response = Some(resp);
						}

						// Clear the handshake data from buffer
						conn.buffer_mut().drain(0..hlen);
						return Ok(());
					}
					if conn.buffer().len() > 16 * 1024 {
						return Err(ProtocolError::BufferOverflow);
					}
					if n < buf.len() {
						break;
					}
				}
				Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
				Err(e) => return Err(ProtocolError::Io(e)),
			}
		}
		Ok(())
	}

	fn handle_handshake_write(&self, conn: &mut Connection) -> ProtocolResult<()> {
		// Extract the necessary data to avoid borrowing issues
		let (response, written) =
			if let crate::core::ConnectionState::WebSocket(WsState::Handshake(data)) = conn.state() {
				if let Some(ref response) = data.handshake_response {
					(response.clone(), data.written)
				} else {
					return Ok(());
				}
			} else {
				return Ok(());
			};

		let mut bytes_written = written;
		loop {
			if bytes_written >= response.len() {
				// Handshake complete, transition to active
				// state
				let active_data = WebSocketConnectionData::active();
				conn.set_state(crate::core::ConnectionState::WebSocket(WsState::Active(active_data)));
				break;
			}

			match conn.stream().write(&response[bytes_written..]) {
				Ok(0) => return Err(ProtocolError::ConnectionClosed),
				Ok(n) => {
					bytes_written += n;
					// Update the state with the new written count
					if let crate::core::ConnectionState::WebSocket(WsState::Handshake(data)) =
						conn.state_mut()
					{
						data.written = bytes_written;
					}
				}
				Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
				Err(e) => return Err(ProtocolError::Io(e)),
			}
		}
		Ok(())
	}

	fn handle_ws_read(&self, conn: &mut Connection) -> ProtocolResult<()> {
		let mut buf = [0u8; 8192];

		loop {
			match conn.stream().read(&mut buf) {
				Ok(0) => return Err(ProtocolError::ConnectionClosed),
				Ok(n) => {
					// Add data to connection buffer
					conn.buffer_mut().extend_from_slice(&buf[..n]);

					// Process complete frames from buffer
					self.process_buffered_ws_data(conn)?;

					if n < buf.len() {
						break;
					}
				}
				Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
				Err(e) => return Err(ProtocolError::Io(e)),
			}
		}
		Ok(())
	}

	fn handle_ws_write(&self, conn: &mut Connection) -> ProtocolResult<()> {
		loop {
			// Check if there's a frame to send
			let frame_to_send =
				if let crate::core::ConnectionState::WebSocket(WsState::Active(data)) = conn.state() {
					data.outbox.front().cloned()
				} else {
					break;
				};

			if let Some(frame) = frame_to_send {
				match conn.stream().write(&frame) {
					Ok(n) => {
						if n == frame.len() {
							// Full frame written
							if let crate::core::ConnectionState::WebSocket(
								WsState::Active(data),
							) = conn.state_mut()
							{
								let written_frame = data.outbox.pop_front().unwrap();
								data.outbox_bytes = data
									.outbox_bytes
									.saturating_sub(written_frame.len());
							}
						} else {
							// Partial write - update the frame
							if let crate::core::ConnectionState::WebSocket(
								WsState::Active(data),
							) = conn.state_mut()
							{
								let mut remaining = data.outbox.pop_front().unwrap();
								remaining.drain(0..n);
								data.outbox.push_front(remaining);
								data.outbox_bytes = data.outbox_bytes.saturating_sub(n);
							}
							break;
						}
					}
					Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
					Err(e) => return Err(ProtocolError::Io(e)),
				}
			} else {
				break;
			}
		}
		Ok(())
	}

	fn process_buffered_ws_data(&self, conn: &mut Connection) -> ProtocolResult<()> {
		let mut total_processed = 0;

		// Process frames directly from buffer to avoid copies
		loop {
			let remaining_len = {
				let buffer = conn.buffer();
				if total_processed >= buffer.len() {
					break;
				}
				buffer.len() - total_processed
			};

			if remaining_len == 0 {
				break;
			}

			// Parse frame directly from buffer slice
			let frame_result = {
				let buffer = conn.buffer();
				let remaining_data = &buffer[total_processed..];

				match parse_ws_frame(remaining_data)
					.map_err(|e| ProtocolError::Custom(format!("Frame parse error: {}", e)))?
				{
					Some((opcode, payload)) => {
						let frame_size = self.calculate_frame_size(remaining_data)?;
						Some((opcode, payload, frame_size))
					}
					None => None, // Incomplete frame
				}
			};

			match frame_result {
				Some((opcode, payload, frame_size)) => {
					total_processed += frame_size;

					// Process frame immediately to avoid
					// storing payload
					self.process_ws_frame(conn, opcode, payload)?;
				}
				None => {
					// Incomplete frame, wait for more data
					break;
				}
			}
		}

		// Remove processed data from connection buffer
		if total_processed > 0 {
			conn.buffer_mut().drain(0..total_processed);

			// Optimize buffer after processing to manage memory
			// efficiently
			conn.optimize_buffer();
		}

		Ok(())
	}

	fn calculate_frame_size(&self, data: &[u8]) -> ProtocolResult<usize> {
		if data.len() < 2 {
			return Ok(0);
		}

		let second_byte = data[1];
		let masked = (second_byte & 0x80) != 0;
		let mut payload_len = (second_byte & 0x7F) as usize;
		let mut pos = 2;

		// Extended payload length
		if payload_len == 126 {
			if data.len() < pos + 2 {
				return Ok(0);
			}
			payload_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
			pos += 2;
		} else if payload_len == 127 {
			if data.len() < pos + 8 {
				return Ok(0);
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

		// Add masking key size
		if masked {
			pos += 4;
		}

		// Add payload size
		pos += payload_len;

		Ok(pos)
	}

	fn process_ws_frame(&self, conn: &mut Connection, opcode: u8, payload: Vec<u8>) -> ProtocolResult<()> {
		match opcode {
			1 => {
				// Text frame - try to parse as WebSocket
				// Request
				let text = String::from_utf8_lossy(&payload);

				match serde_json::from_str::<Request>(&text) {
					Ok(request) => {
						let response_payload = self.handle_request(conn, &request)?;
						let response = Response {
							id: request.id,
							payload: response_payload,
						};
						let response_json = serde_json::to_string(&response).map_err(|e| {
							ProtocolError::Custom(format!("JSON error: {}", e))
						})?;
						let response_frame = build_ws_frame(1, response_json.as_bytes());
						self.send_frame(conn, response_frame)?;
					}
					Err(parse_error) => {
						// Not a valid WebSocket Request
						// - send error response
						eprintln!("WebSocket request parse error: {}", parse_error);
						let error_response = serde_json::json!({
							"error": "Invalid request format",
							"message": format!("Failed to parse WebSocket request: {}", parse_error)
						});
						let error_json =
							serde_json::to_string(&error_response).map_err(|e| {
								ProtocolError::Custom(format!("JSON error: {}", e))
							})?;
						let error_frame = build_ws_frame(1, error_json.as_bytes());
						self.send_frame(conn, error_frame)?;
					}
				}
			}
			2 => {
				// Binary frame - echo it back
				let response_frame = build_ws_frame(2, &payload);
				self.send_frame(conn, response_frame)?;
			}
			8 => {
				// Close frame - send close response and mark
				// connection for closure
				let close_code = if payload.len() >= 2 {
					u16::from_be_bytes([payload[0], payload[1]])
				} else {
					1000 // Normal closure
				};

				let _close_reason = if payload.len() > 2 {
					String::from_utf8_lossy(&payload[2..]).to_string()
				} else {
					"Connection closed by client".to_string()
				};

				// Send close response with same code
				let mut close_payload = close_code.to_be_bytes().to_vec();
				close_payload.extend_from_slice(b"Server closing connection");
				let close_response = build_ws_frame(8, &close_payload);
				self.send_frame(conn, close_response)?;

				// Mark connection as closed
				conn.set_state(crate::core::ConnectionState::WebSocket(WsState::Closed));
			}
			9 => {
				// Ping frame - respond with pong
				let pong_response = build_ws_frame(10, &payload);
				self.send_frame(conn, pong_response)?;
			}
			10 => {
				// Pong frame - client response to our ping, no
				// action needed
			}
			_ => {
				// Ignore other opcodes for now
			}
		}
		Ok(())
	}

	fn handle_request(&self, conn: &mut Connection, request: &Request) -> ProtocolResult<ResponsePayload> {
		use super::{AuthResponse, RequestPayload};

		match &request.payload {
			RequestPayload::Auth(_auth_req) => {
				// For now, always return success for auth
				Ok(ResponsePayload::Auth(AuthResponse {}))
			}
			RequestPayload::Command(cmd_req) => self.handle_command_request(conn, cmd_req),
			RequestPayload::Query(query_req) => self.handle_query_request(conn, query_req),
		}
	}

	fn handle_command_request(
		&self,
		conn: &mut Connection,
		cmd_req: &super::CommandRequest,
	) -> ProtocolResult<ResponsePayload> {
		// Execute each statement and collect results
		let mut all_frames = Vec::new();

		for statement in &cmd_req.statements {
			let params = convert_params(&cmd_req.params)?;

			match conn.engine().command_as(
				&Identity::System {
					id: 1,
					name: "root".to_string(),
				},
				statement,
				params,
			) {
				Ok(result) => {
					let frames = convert_result_to_frames(result)?;
					all_frames.extend(frames);
				}
				Err(e) => {
					// Get the diagnostic from the error and
					// add statement context
					let mut diagnostic = e.diagnostic();
					diagnostic.with_statement(statement.clone());

					return Ok(ResponsePayload::Err(super::ErrResponse {
						diagnostic,
					}));
				}
			}
		}

		Ok(ResponsePayload::Command(CommandResponse {
			frames: all_frames,
		}))
	}

	fn handle_query_request(
		&self,
		conn: &mut Connection,
		query_req: &super::QueryRequest,
	) -> ProtocolResult<ResponsePayload> {
		// Execute each statement and collect results
		let mut all_frames = Vec::new();

		for statement in &query_req.statements {
			let params = convert_params(&query_req.params)?;

			match conn.engine().query_as(
				&Identity::System {
					id: 1,
					name: "root".to_string(),
				},
				statement,
				params,
			) {
				Ok(result) => {
					let frames = convert_result_to_frames(result)?;
					all_frames.extend(frames);
				}
				Err(e) => {
					// Get the diagnostic from the error and
					// add statement context
					let mut diagnostic = e.diagnostic();
					diagnostic.with_statement(statement.clone());

					return Ok(ResponsePayload::Err(super::ErrResponse {
						diagnostic,
					}));
				}
			}
		}

		Ok(ResponsePayload::Query(QueryResponse {
			frames: all_frames,
		}))
	}

	fn send_frame(&self, conn: &mut Connection, frame: Vec<u8>) -> ProtocolResult<()> {
		if let crate::core::ConnectionState::WebSocket(WsState::Active(data)) = conn.state_mut() {
			if data.outbox_bytes + frame.len() > data.max_outbox_bytes {
				return Err(ProtocolError::BufferOverflow);
			}

			data.outbox_bytes += frame.len();
			data.outbox.push_back(frame);
		}
		Ok(())
	}
}
