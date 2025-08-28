// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	collections::VecDeque,
	io::{Read, Write},
	net::SocketAddr,
};

use mio::{Interest, Token, net::TcpStream};
use reifydb_core::interface::{Engine, Identity, Params, Transaction};
use reifydb_engine::StandardEngine;

use crate::protocol::{
	RequestMsg, ResponseMsg, build_ws_frame, build_ws_response,
	find_header_end, parse_ws_frame,
};

pub struct Connection<T: Transaction> {
	stream: TcpStream,
	peer: SocketAddr,
	token: Token,
	state: ConnState,
	outbox_bytes: usize,
	max_outbox_bytes: usize,
	engine: StandardEngine<T>,
}

enum ConnState {
	Handshake(HandshakeState),
	Active(ActiveState),
	Closed,
}

struct HandshakeState {
	buf: Vec<u8>,
	response: Option<Vec<u8>>,
	written: usize,
}

struct ActiveState {
	sendq: VecDeque<Vec<u8>>, // Raw WebSocket frames to send
}

impl<T: Transaction> Connection<T> {
	pub fn new(
		stream: TcpStream,
		peer: SocketAddr,
		token: Token,
		engine: StandardEngine<T>,
	) -> Self {
		Self {
			stream,
			peer,
			token,
			state: ConnState::Handshake(HandshakeState {
				buf: Vec::with_capacity(1024),
				response: None,
				written: 0,
			}),
			outbox_bytes: 0,
			max_outbox_bytes: 1 << 20, // 1MB
			engine,
		}
	}

	pub fn stream(&mut self) -> &mut TcpStream {
		&mut self.stream
	}

	pub fn peer(&self) -> SocketAddr {
		self.peer
	}

	pub fn token(&self) -> Token {
		self.token
	}

	pub fn interests(&self) -> Interest {
		match &self.state {
			ConnState::Handshake(hs) => {
				let mut interest = Interest::READABLE;
				if hs.response.is_some() {
					interest |= Interest::WRITABLE;
				}
				interest
			}
			ConnState::Active(active) => {
				let mut interest = Interest::READABLE;
				if !active.sendq.is_empty() {
					interest |= Interest::WRITABLE;
				}
				interest
			}
			ConnState::Closed => Interest::READABLE,
		}
	}

	pub fn handle_read(
		&mut self,
	) -> Result<(), Box<dyn std::error::Error>> {
		match &mut self.state {
			ConnState::Handshake(_) => self.handle_handshake_read(),
			ConnState::Active(_) => self.handle_ws_read(),
			ConnState::Closed => Ok(()),
		}
	}

	pub fn handle_write(
		&mut self,
	) -> Result<(), Box<dyn std::error::Error>> {
		match &mut self.state {
			ConnState::Handshake(_) => {
				self.handle_handshake_write()
			}
			ConnState::Active(_) => self.handle_ws_write(),
			ConnState::Closed => Ok(()),
		}
	}

	fn handle_handshake_read(
		&mut self,
	) -> Result<(), Box<dyn std::error::Error>> {
		let mut buf = [0u8; 2048];
		let mut should_process_handshake = false;

		if let ConnState::Handshake(hs) = &mut self.state {
			loop {
				match self.stream.read(&mut buf) {
                    Ok(0) => return Err("peer closed during handshake".into()),
                    Ok(n) => {
                        hs.buf.extend_from_slice(&buf[..n]);
                        if find_header_end(&hs.buf).is_some() {
                            should_process_handshake = true;
                            break;
                        }
                        if hs.buf.len() > 16 * 1024 {
                            return Err("handshake header too large".into());
                        }
                        if n < buf.len() {
                            break;
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e.into()),
                }
			}
		}

		if should_process_handshake {
			if let ConnState::Handshake(hs) = &mut self.state {
				if hs.response.is_none() {
					if let Some(hlen) =
						find_header_end(&hs.buf)
					{
						let (resp, _key) =
							build_ws_response(
								&hs.buf[..hlen],
							)?;
						hs.response = Some(resp);
					}
				}
			}
		}
		Ok(())
	}

	fn handle_handshake_write(
		&mut self,
	) -> Result<(), Box<dyn std::error::Error>> {
		if let ConnState::Handshake(hs) = &mut self.state {
			if let Some(ref response) = hs.response {
				loop {
					if hs.written >= response.len() {
						// Handshake complete,
						// transition to active state
						self.state = ConnState::Active(ActiveState {
                            sendq: VecDeque::new(),
                        });
						break;
					}

					match self.stream.write(&response[hs.written..]) {
                        Ok(0) => return Err("peer closed during handshake write".into()),
                        Ok(n) => {
                            hs.written += n;
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(e) => return Err(e.into()),
                    }
				}
			}
		}
		Ok(())
	}

	fn handle_ws_read(&mut self) -> Result<(), Box<dyn std::error::Error>> {
		let mut buf = [0u8; 8192];

		loop {
			match self.stream.read(&mut buf) {
                Ok(0) => return Err("peer closed connection".into()),
                Ok(n) => {
                    // Parse WebSocket frames and handle them
                    self.process_ws_data(&buf[..n])?;
                    if n < buf.len() {
                        break;
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e.into()),
            }
		}
		Ok(())
	}

	fn handle_ws_write(
		&mut self,
	) -> Result<(), Box<dyn std::error::Error>> {
		if let ConnState::Active(active) = &mut self.state {
			while let Some(frame) = active.sendq.front() {
				match self.stream.write(frame) {
                    Ok(n) => {
                        if n == frame.len() {
                            // Full frame written
                            let written_frame = active.sendq.pop_front().unwrap();
                            self.outbox_bytes = self.outbox_bytes.saturating_sub(written_frame.len());
                        } else {
                            // Partial write - update the frame
                            let mut remaining = active.sendq.pop_front().unwrap();
                            remaining.drain(0..n);
                            active.sendq.push_front(remaining);
                            self.outbox_bytes = self.outbox_bytes.saturating_sub(n);
                            break;
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e.into()),
                }
			}
		}
		Ok(())
	}

	fn process_ws_data(
		&mut self,
		data: &[u8],
	) -> Result<(), Box<dyn std::error::Error>> {
		if let Some((opcode, payload)) = parse_ws_frame(data)? {
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
							match self.engine.query_as(
                                &Identity::System { id: 1, name: "root".to_string() },
                                &req.q,
                                Params::None
                            ) {
                                Ok(result) => {
                                    dbg!(&result);
                                    let response = ResponseMsg {
                                        ok: true,
                                        result: format!("Query executed successfully, {} frames returned", result.len()),
                                    };
                                    let response_json = serde_json::to_string(&response)?;
                                    let response_frame = build_ws_frame(1, response_json.as_bytes());
                                    self.send_frame(response_frame)?;
                                }
                                Err(e) => {
                                    let response = ResponseMsg {
                                        ok: false,
                                        result: format!("Query error: {}", e),
                                    };
                                    let response_json = serde_json::to_string(&response)?;
                                    let response_frame = build_ws_frame(1, response_json.as_bytes());
                                    self.send_frame(response_frame)?;
                                }
                            }
						}
						Err(_) => {
							// Not a valid JSON
							// request, echo back
							// the text
							let response_frame = build_ws_frame(1, &payload);
							self.send_frame(
								response_frame,
							)?;
						}
					}
				}
				2 => {
					// Binary frame - echo it back
					let response_frame =
						build_ws_frame(2, &payload);
					self.send_frame(response_frame)?;
				}
				8 => {
					// Close frame
					self.state = ConnState::Closed;
				}
				_ => {
					// Ignore other opcodes for now
				}
			}
		}
		Ok(())
	}

	fn send_frame(
		&mut self,
		frame: Vec<u8>,
	) -> Result<(), Box<dyn std::error::Error>> {
		if let ConnState::Active(active) = &mut self.state {
			if self.outbox_bytes + frame.len()
				> self.max_outbox_bytes
			{
				return Err("outbox buffer full".into());
			}

			self.outbox_bytes += frame.len();
			active.sendq.push_back(frame);
		}
		Ok(())
	}
}
