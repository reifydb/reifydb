// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod handler;
pub mod request;
pub mod response;

use std::collections::VecDeque;

pub use handler::WebSocketHandler;
use mio::Interest;
pub use request::{CommandRequest, QueryRequest, Request, RequestPayload};
pub use response::{
	AuthResponse, CommandResponse, ErrResponse, QueryResponse, Response, ResponsePayload, WebsocketColumn,
	WebsocketFrame,
};

/// WebSocket connection state
#[derive(Debug, Clone, PartialEq)]
pub enum WsState {
	/// In handshake phase
	Handshake(WebSocketConnectionData),
	/// Active WebSocket connection
	Active(WebSocketConnectionData),
	/// Connection closed
	Closed,
}

/// WebSocket-specific connection data
#[derive(Debug, Clone, PartialEq)]
pub struct WebSocketConnectionData {
	/// Handshake response (if in handshake phase)
	pub handshake_response: Option<Vec<u8>>,
	/// Bytes written during handshake
	pub written: usize,
	/// Send queue for WebSocket frames
	pub outbox: VecDeque<Vec<u8>>,
	/// Current outbox size in bytes
	pub outbox_bytes: usize,
	/// Maximum outbox size
	pub max_outbox_bytes: usize,
}

impl WebSocketConnectionData {
	pub fn new() -> Self {
		Self {
			handshake_response: None,
			written: 0,
			outbox: VecDeque::new(),
			outbox_bytes: 0,
			max_outbox_bytes: 1 << 20, // 1MB
		}
	}

	pub fn active() -> Self {
		Self {
			handshake_response: None,
			written: 0,
			outbox: VecDeque::new(),
			outbox_bytes: 0,
			max_outbox_bytes: 1 << 20, // 1MB
		}
	}
}

impl WsState {
	pub fn interests(&self) -> Interest {
		match self {
			WsState::Handshake(data) => {
				let mut interest = Interest::READABLE;
				if data.handshake_response.is_some() {
					interest |= Interest::WRITABLE;
				}
				interest
			}
			WsState::Active(data) => {
				let mut interest = Interest::READABLE;
				if !data.outbox.is_empty() {
					interest |= Interest::WRITABLE;
				}
				interest
			}
			WsState::Closed => Interest::READABLE,
		}
	}
}
