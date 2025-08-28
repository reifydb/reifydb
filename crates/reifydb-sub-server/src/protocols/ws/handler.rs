// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::io::{Read, Write};

use reifydb_core::{
	interface::{Engine, Identity, Params, Transaction},
	log_info,
};

use super::{WebSocketConnectionData, WsState};
use crate::{
	core::Connection,
	protocols::{
		ProtocolError, ProtocolHandler, ProtocolResult,
		utils::{
			RequestMsg, ResponseMsg, build_ws_frame,
			build_ws_response, find_header_end, parse_ws_frame,
		},
	},
};

#[derive(Clone)]
pub struct WebSocketHandler;

impl WebSocketHandler {
	pub fn new() -> Self {
		Self
	}
}

impl<T: Transaction> ProtocolHandler<T> for WebSocketHandler {
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
				|| request_lower.contains(
					"connection: keep-alive, upgrade",
				))
	}

	fn handle_connection(
		&self,
		conn: &mut Connection<T>,
	) -> ProtocolResult<()> {
		// Initialize WebSocket state
		let ws_state =
			WsState::Handshake(WebSocketConnectionData::new());
		conn.set_state(crate::core::ConnectionState::WebSocket(
			ws_state,
		));
		Ok(())
	}

	fn handle_read(&self, conn: &mut Connection<T>) -> ProtocolResult<()> {
		if let crate::core::ConnectionState::WebSocket(ws_state) =
			conn.state()
		{
			match ws_state {
				WsState::Handshake(_) => {
					self.handle_handshake_read(conn)
				}
				WsState::Active(_) => self.handle_ws_read(conn),
				WsState::Closed => Ok(()),
			}
		} else {
			Err(ProtocolError::InvalidFrame)
		}
	}

	fn handle_write(&self, conn: &mut Connection<T>) -> ProtocolResult<()> {
		if let crate::core::ConnectionState::WebSocket(ws_state) =
			conn.state()
		{
			match ws_state {
				WsState::Handshake(_) => {
					self.handle_handshake_write(conn)
				}
				WsState::Active(_) => {
					self.handle_ws_write(conn)
				}
				WsState::Closed => Ok(()),
			}
		} else {
			Err(ProtocolError::InvalidFrame)
		}
	}

	fn should_close(&self, conn: &Connection<T>) -> bool {
		matches!(
			conn.state(),
			crate::core::ConnectionState::WebSocket(
				WsState::Closed
			) | crate::core::ConnectionState::Closed
		)
	}
}

impl WebSocketHandler {
	fn handle_handshake_read<T: Transaction>(
		&self,
		conn: &mut Connection<T>,
	) -> ProtocolResult<()> {
		// First, check if we already have complete headers in the
		// buffer (from protocol detection)
		if !conn.buffer().is_empty() {
			if let Some(hlen) = find_header_end(conn.buffer()) {
				let (resp, _key) = build_ws_response(
					&conn.buffer()[..hlen],
				)
				.map_err(|e| {
					ProtocolError::Custom(format!(
						"Handshake error: {}",
						e
					))
				})?;

				// Update WebSocket state with response
				if let crate::core::ConnectionState::WebSocket(
					WsState::Handshake(data),
				) = conn.state_mut()
				{
					data.handshake_response = Some(resp);
				}
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
                        let (resp, _key) = build_ws_response(&conn.buffer()[..hlen])
                            .map_err(|e| ProtocolError::Custom(format!("Handshake error: {}", e)))?;

                        // Update WebSocket state with response
                        if let crate::core::ConnectionState::WebSocket(WsState::Handshake(data)) = conn.state_mut() {
                            data.handshake_response = Some(resp);
                        }
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

	fn handle_handshake_write<T: Transaction>(
		&self,
		conn: &mut Connection<T>,
	) -> ProtocolResult<()> {
		// Extract the necessary data to avoid borrowing issues
		let (response, written) =
			if let crate::core::ConnectionState::WebSocket(
				WsState::Handshake(data),
			) = conn.state()
			{
				if let Some(ref response) =
					data.handshake_response
				{
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
				let active_data =
					WebSocketConnectionData::active();
				conn.set_state(
					crate::core::ConnectionState::WebSocket(
						WsState::Active(active_data),
					),
				);
				break;
			}

			match conn.stream().write(&response[bytes_written..]) {
                Ok(0) => return Err(ProtocolError::ConnectionClosed),
                Ok(n) => {
                    bytes_written += n;
                    // Update the state with the new written count
                    if let crate::core::ConnectionState::WebSocket(WsState::Handshake(data)) = conn.state_mut() {
                        data.written = bytes_written;
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(ProtocolError::Io(e)),
            }
		}
		Ok(())
	}

	fn handle_ws_read<T: Transaction>(
		&self,
		conn: &mut Connection<T>,
	) -> ProtocolResult<()> {
		let mut buf = [0u8; 8192];

		loop {
			match conn.stream().read(&mut buf) {
                Ok(0) => return Err(ProtocolError::ConnectionClosed),
                Ok(n) => {
                    self.process_ws_data(conn, &buf[..n])?;
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

	fn handle_ws_write<T: Transaction>(
		&self,
		conn: &mut Connection<T>,
	) -> ProtocolResult<()> {
		loop {
			// Check if there's a frame to send
			let frame_to_send =
				if let crate::core::ConnectionState::WebSocket(
					WsState::Active(data),
				) = conn.state()
				{
					data.outbox.front().cloned()
				} else {
					break;
				};

			if let Some(frame) = frame_to_send {
				match conn.stream().write(&frame) {
                    Ok(n) => {
                        if n == frame.len() {
                            // Full frame written
                            if let crate::core::ConnectionState::WebSocket(WsState::Active(data)) = conn.state_mut() {
                                let written_frame = data.outbox.pop_front().unwrap();
                                data.outbox_bytes = data.outbox_bytes.saturating_sub(written_frame.len());
                            }
                        } else {
                            // Partial write - update the frame
                            if let crate::core::ConnectionState::WebSocket(WsState::Active(data)) = conn.state_mut() {
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

	fn process_ws_data<T: Transaction>(
		&self,
		conn: &mut Connection<T>,
		data: &[u8],
	) -> ProtocolResult<()> {
		if let Some((opcode, payload)) =
			parse_ws_frame(data).map_err(|e| {
				ProtocolError::Custom(format!(
					"Frame parse error: {}",
					e
				))
			})? {
			match opcode {
				1 => {
					// Text frame - try to parse as JSON
					// query
					let text = String::from_utf8_lossy(
						&payload,
					);

					match serde_json::from_str::<RequestMsg>(
						&text,
					) {
						Ok(req) => {
							// Execute the query
							// using the engine
							match conn.engine().query_as(
                                &Identity::System { id: 1, name: "root".to_string() },
                                &req.q,
                                Params::None
                            ) {
                                Ok(result) => {
                                    let response = ResponseMsg {
                                        ok: true,
                                        result: format!("Query executed successfully, {} frames returned", result.len()),
                                    };
                                    let response_json = serde_json::to_string(&response)
                                        .map_err(|e| ProtocolError::Custom(format!("JSON error: {}", e)))?;
                                    let response_frame = build_ws_frame(1, response_json.as_bytes());
                                    self.send_frame(conn, response_frame)?;
                                }
                                Err(e) => {
                                    let response = ResponseMsg {
                                        ok: false,
                                        result: format!("Query error: {}", e),
                                    };
                                    let response_json = serde_json::to_string(&response)
                                        .map_err(|e| ProtocolError::Custom(format!("JSON error: {}", e)))?;
                                    let response_frame = build_ws_frame(1, response_json.as_bytes());
                                    self.send_frame(conn, response_frame)?;
                                }
                            }
						}
						Err(_) => {
							// Not a valid JSON
							// request, echo back
							// the text
							let response_frame = build_ws_frame(1, &payload);
							self.send_frame(
								conn,
								response_frame,
							)?;
						}
					}
				}
				2 => {
					// Binary frame - echo it back
					let response_frame =
						build_ws_frame(2, &payload);
					self.send_frame(conn, response_frame)?;
				}
				8 => {
					// Close frame - send close response and
					// mark connection for closure
					let close_code = if payload.len() >= 2 {
						u16::from_be_bytes([
							payload[0], payload[1],
						])
					} else {
						1000 // Normal closure
					};

					let close_reason =
						if payload.len() > 2 {
							String::from_utf8_lossy(
								&payload[2..],
							)
							.to_string()
						} else {
							"Connection closed by client".to_string()
						};

					// Send close response with same code
					let mut close_payload = close_code
						.to_be_bytes()
						.to_vec();
					close_payload.extend_from_slice(
						b"Server closing connection",
					);
					let close_response = build_ws_frame(
						8,
						&close_payload,
					);
					self.send_frame(conn, close_response)?;

					// Mark connection as closed
					conn.set_state(crate::core::ConnectionState::WebSocket(WsState::Closed));
				}
				9 => {
					// Ping frame - respond with pong
					let pong_response =
						build_ws_frame(10, &payload);
					self.send_frame(conn, pong_response)?;
				}
				10 => {
					// Pong frame - client response to our
					// ping, no action needed
				}
				_ => {
					// Ignore other opcodes for now
				}
			}
		}
		Ok(())
	}

	fn send_frame<T: Transaction>(
		&self,
		conn: &mut Connection<T>,
		frame: Vec<u8>,
	) -> ProtocolResult<()> {
		if let crate::core::ConnectionState::WebSocket(
			WsState::Active(data),
		) = conn.state_mut()
		{
			if data.outbox_bytes + frame.len()
				> data.max_outbox_bytes
			{
				return Err(ProtocolError::BufferOverflow);
			}

			data.outbox_bytes += frame.len();
			data.outbox.push_back(frame);
		}
		Ok(())
	}
}
