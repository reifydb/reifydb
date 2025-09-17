// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{net::SocketAddr, time::Instant};

use mio::{Interest, Token, net::TcpStream};
use reifydb_core::interface::Transaction;
use reifydb_engine::StandardEngine;

use crate::protocols::{http::HttpState, ws::WsState};

/// Buffer management for connection buffers
const INITIAL_BUFFER_SIZE: usize = 8192;
const MAX_BUFFER_SIZE: usize = 1024 * 1024; // 1MB max
const SHRINK_THRESHOLD: usize = 64 * 1024; // Shrink if buffer grows above 64KB and is mostly empty

/// Connection state for protocol detection and handling
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
	/// Initial state - detecting protocol
	Detecting,
	/// WebSocket protocol
	WebSocket(WsState),
	/// HTTP protocol
	Http(HttpState),
	/// Connection closed
	Closed,
}

/// Generic connection wrapper that can handle multiple protocols
pub struct Connection<T: Transaction> {
	stream: TcpStream,
	peer: SocketAddr,
	token: Token,
	state: ConnectionState,
	engine: StandardEngine<T>,
	created_at: Instant,
	last_activity: Instant,
	buffer: Vec<u8>,
}

impl<T: Transaction> Connection<T> {
	pub fn new(stream: TcpStream, peer: SocketAddr, token: Token, engine: StandardEngine<T>) -> Self {
		let now = Instant::now();
		Self {
			stream,
			peer,
			token,
			state: ConnectionState::Detecting,
			engine,
			created_at: now,
			last_activity: now,
			buffer: Vec::with_capacity(INITIAL_BUFFER_SIZE),
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

	pub fn state(&self) -> &ConnectionState {
		&self.state
	}

	pub fn state_mut(&mut self) -> &mut ConnectionState {
		self.last_activity = Instant::now();
		&mut self.state
	}

	pub fn set_state(&mut self, state: ConnectionState) {
		self.state = state;
		self.last_activity = Instant::now();
	}

	pub fn engine(&self) -> &StandardEngine<T> {
		&self.engine
	}

	pub fn buffer(&self) -> &[u8] {
		&self.buffer
	}

	pub fn buffer_mut(&mut self) -> &mut Vec<u8> {
		self.last_activity = Instant::now();
		&mut self.buffer
	}

	/// Smart buffer management - shrink if buffer has grown large but is
	/// mostly empty
	pub fn optimize_buffer(&mut self) {
		let buffer_capacity = self.buffer.capacity();
		let buffer_len = self.buffer.len();

		// If buffer capacity is large but usage is low, shrink it
		if buffer_capacity > SHRINK_THRESHOLD && buffer_len < buffer_capacity / 4 {
			// Shrink to 2x current usage or initial size, whichever
			// is larger
			let new_capacity = std::cmp::max(buffer_len * 2, INITIAL_BUFFER_SIZE);

			if new_capacity < buffer_capacity {
				self.buffer.shrink_to(new_capacity);
			}
		}

		// Ensure buffer doesn't grow beyond max size
		if buffer_capacity > MAX_BUFFER_SIZE {
			self.buffer.truncate(MAX_BUFFER_SIZE);
			self.buffer.shrink_to(MAX_BUFFER_SIZE);
		}
	}

	pub fn reset_buffer(&mut self) {
		self.buffer.clear();
		if self.buffer.capacity() > INITIAL_BUFFER_SIZE * 4 {
			self.buffer.shrink_to(INITIAL_BUFFER_SIZE);
		}
	}

	/// Properly close the TCP connection with shutdown
	pub fn shutdown(&mut self) {
		use std::io::Write;

		// Flush any remaining data
		let _ = self.stream.flush();

		// Attempt graceful shutdown of TCP connection
		let _ = self.stream.shutdown(std::net::Shutdown::Both);

		// Mark connection state as closed
		self.state = ConnectionState::Closed;

		self.reset_buffer()
	}

	pub fn interests(&self) -> Interest {
		match &self.state {
			ConnectionState::Detecting => Interest::READABLE,
			ConnectionState::WebSocket(ws_state) => ws_state.interests(),
			ConnectionState::Http(http_state) => http_state.interests(),
			ConnectionState::Closed => Interest::READABLE,
		}
	}

	pub fn age(&self) -> std::time::Duration {
		self.created_at.elapsed()
	}

	pub fn idle_time(&self) -> std::time::Duration {
		self.last_activity.elapsed()
	}
}
