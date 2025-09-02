// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	io::Read,
	net::SocketAddr,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use mio::{
	Events, Interest, Poll, Token, Waker,
	event::Event,
	net::{TcpListener, TcpStream},
};
use reifydb_core::interface::Transaction;
use reifydb_engine::StandardEngine;
use slab::Slab;

use super::{Connection, ConnectionState};
use crate::{
	config::ServerConfig,
	protocols::{HttpHandler, ProtocolHandler, WebSocketHandler},
};

const LISTENER: Token = Token(0);
const WAKE_TOKEN: Token = Token(1);
const TOKEN_BASE: usize = 2;

pub struct Worker<T: Transaction> {
	worker_id: usize,
	listener: TcpListener,
	config: ServerConfig,
	shutdown: Arc<AtomicBool>,
	engine: StandardEngine<T>,
	websocket_handler: Option<WebSocketHandler>,
	http_handler: Option<HttpHandler>,
}

impl<T: Transaction> Worker<T> {
	pub fn new(
		worker_id: usize,
		std_listener: std::net::TcpListener,
		config: ServerConfig,
		shutdown: Arc<AtomicBool>,
		engine: StandardEngine<T>,
	) -> Self {
		let listener = TcpListener::from_std(std_listener);

		Self {
			worker_id,
			listener,
			config,
			shutdown,
			engine,
			websocket_handler: None,
			http_handler: None,
		}
	}

	pub fn with_websocket(
		&mut self,
		handler: WebSocketHandler,
	) -> &mut Self {
		self.websocket_handler = Some(handler);
		self
	}

	pub fn with_http(&mut self, handler: HttpHandler) -> &mut Self {
		self.http_handler = Some(handler);
		self
	}

	pub fn run(&mut self) {
		if self.config.network.pin_threads {
			if let Some(core) = core_affinity::get_core_ids()
				.and_then(|v| v.get(self.worker_id).cloned())
			{
				core_affinity::set_for_current(core);
				// Core pinned successfully
			}
		}

		let mut poll = Poll::new().expect("failed to create poll");
		let mut events = Events::with_capacity(1024);

		poll.registry()
			.register(
				&mut self.listener,
				LISTENER,
				Interest::READABLE,
			)
			.expect("failed to register listener");

		let waker = Waker::new(poll.registry(), WAKE_TOKEN)
			.expect("failed to create waker");
		let _ctrl = Arc::new(waker);

		let mut connections = Slab::<Connection<T>>::new();
		let mut last_cleanup = std::time::Instant::now();

		// Worker started, entering event loop

		loop {
			// Check for shutdown signal
			if self.shutdown.load(Ordering::Relaxed) {
				break;
			}

			if let Err(e) = poll.poll(
				&mut events,
				Some(std::time::Duration::from_millis(1)),
			) {
				if e.kind() == std::io::ErrorKind::Interrupted {
					continue;
				}
				// Poll error - break from event loop
				break;
			}

			for event in events.iter() {
				match event.token() {
					LISTENER => {
						self.handle_accept(
							&mut connections,
							&poll,
						);
					}
					WAKE_TOKEN => {
						// Handle control-plane
						// operations if needed
					}
					token => {
						self.handle_connection_event(
							&mut connections,
							&poll,
							token,
							event,
						);
					}
				}
			}

			// Periodic cleanup every 30 seconds
			if last_cleanup.elapsed().as_secs() >= 30 {
				self.cleanup_abandoned_connections(
					&mut connections,
				);
				last_cleanup = std::time::Instant::now();
			}
		}

		// Clean up connections on shutdown
		let keys: Vec<usize> =
			connections.iter().map(|(key, _)| key).collect();
		for key in keys {
			if let Some(mut conn) = connections.try_remove(key) {
				let _ = poll
					.registry()
					.deregister(conn.stream());
				// Properly close TCP connection
				conn.shutdown();
			}
		}

		// Worker stopped
	}

	fn handle_accept(
		&mut self,
		connections: &mut Slab<Connection<T>>,
		poll: &Poll,
	) {
		loop {
			match self.listener.accept() {
				Ok((stream, peer)) => {
					if let Err(_e) = self.on_accept(
						connections,
						poll,
						stream,
						peer,
					) {
						// Accept error - ignore and
						// continue
					}
				}
				Err(e) if e.kind()
					== std::io::ErrorKind::WouldBlock =>
				{
					break;
				}
				Err(e) => {
					// If it's "too many open files", force
					// immediate cleanup
					if e.kind() == std::io::ErrorKind::Other
					{
						self.cleanup_abandoned_connections(connections);
					}
					break;
				}
			}
		}
	}

	fn handle_connection_event(
		&self,
		connections: &mut Slab<Connection<T>>,
		_poll: &Poll,
		token: Token,
		event: &Event,
	) {
		let key = match token.0.checked_sub(TOKEN_BASE) {
			Some(k) => k,
			None => return,
		};

		if !connections.contains(key) {
			return;
		}

		if let Err(_e) = self.on_connection_event(
			connections,
			&_poll,
			key,
			event,
		) {
			// Silently close connection on error (suppress logging
			// for benchmarks)
			self.close_connection(connections, key);
		} else {
			// Check if connection should be closed after successful
			// event processing
			if let Some(conn) = connections.get(key) {
				let should_close = match conn.state() {
					ConnectionState::WebSocket(_) => self
						.websocket_handler
						.as_ref()
						.map(|h| h.should_close(conn))
						.unwrap_or(false),
					ConnectionState::Http(_) => self
						.http_handler
						.as_ref()
						.map(|h| h.should_close(conn))
						.unwrap_or(false),
					ConnectionState::Closed => true,
					_ => false,
				};

				if should_close {
					self.close_connection(connections, key);
					return;
				}
			}

			// Periodically optimize buffer for active connections
			if let Some(conn) = connections.get_mut(key) {
				// Optimize buffer every ~1000 events (rough
				// heuristic)
				if key % 1000 == 0 {
					conn.optimize_buffer();
				}
			}
		}
	}

	fn on_accept(
		&self,
		connections: &mut Slab<Connection<T>>,
		poll: &Poll,
		mut stream: TcpStream,
		peer: SocketAddr,
	) -> Result<(), Box<dyn std::error::Error>> {
		stream.set_nodelay(true)?;

		// Apply socket tuning to reduce TIME_WAIT accumulation
		// Use socket2 to access advanced socket options
		use socket2::SockRef;
		let socket_ref = SockRef::from(&stream);

		// Set linger to 0 to force immediate close without TIME_WAIT
		socket_ref
			.set_linger(Some(std::time::Duration::from_secs(0)))?;

		let entry = connections.vacant_entry();
		let key = entry.key();
		let token = Token(TOKEN_BASE + key);

		// Start with combined interests to minimize reregistration for
		// WebSocket connections
		poll.registry().register(
			&mut stream,
			token,
			Interest::READABLE | Interest::WRITABLE,
		)?;

		let conn = Connection::new(
			stream,
			peer,
			token,
			self.engine.clone(),
		);
		entry.insert(conn);
		Ok(())
	}

	fn on_connection_event(
		&self,
		connections: &mut Slab<Connection<T>>,
		poll: &Poll,
		key: usize,
		event: &Event,
	) -> Result<(), Box<dyn std::error::Error>> {
		let conn = &mut connections[key];

		if event.is_readable() {
			// Detect protocol if still in detecting state
			if matches!(
				conn.state(),
				crate::core::ConnectionState::Detecting
			) {
				self.detect_and_init_protocol(conn)?;
			}
			self.handle_read_event(conn)?;

			// Optimize interest registration based on protocol
			match conn.state() {
				ConnectionState::Http(crate::protocols::http::HttpState::WritingResponse(_)) => {
					// HTTP needs to switch to writable for response
					let token = conn.token();
					poll.registry().reregister(
						conn.stream(),
						token,
						Interest::WRITABLE,
					)?;
				}
				ConnectionState::WebSocket(crate::protocols::ws::WsState::Handshake(data)) if data.handshake_response.is_some() => {
					// WebSocket handshake needs to write response, then maintain both interests
					let token = conn.token();
					poll.registry().reregister(
						conn.stream(),
						token,
						Interest::READABLE | Interest::WRITABLE,
					)?;
				}
				ConnectionState::WebSocket(crate::protocols::ws::WsState::Active(_)) => {
					// WebSocket active connections should have both interests to avoid reregistration
					let token = conn.token();
					poll.registry().reregister(
						conn.stream(),
						token,
						Interest::READABLE | Interest::WRITABLE,
					)?;
				}
				_ => {
					// No reregistration needed for other states
				}
			}
		}

		if event.is_writable() {
			self.handle_write_event(conn)?;

			// For WebSocket connections, we maintain read/write
			// interests to avoid frequent reregistration
			// Only reregister if connection is closed or requires
			// different handling
			match conn.state() {
				ConnectionState::Closed => {
					// Connection is closed, will be cleaned up
				}
				ConnectionState::Http(crate::protocols::http::HttpState::ReadingRequest(_)) => {
					// HTTP may need to switch back to readable only
					let token = conn.token();
					poll.registry().reregister(
						conn.stream(),
						token,
						Interest::READABLE,
					)?;
				}
				_ => {
					// For WebSocket and other states, interests are already optimal
				}
			}
		}

		if event.is_error() {
			return Err("Connection error".into());
		}

		Ok(())
	}

	fn detect_and_init_protocol(
		&self,
		conn: &mut Connection<T>,
	) -> Result<(), Box<dyn std::error::Error>> {
		// Read some data to detect protocol
		let mut buf = [0u8; 1024];
		match conn.stream().read(&mut buf) {
			Ok(0) => {
				return Err("Connection closed during protocol detection".into());
			}
			Ok(n) => {
				conn.buffer_mut().extend_from_slice(&buf[..n]);

				// Try to detect protocol
				let detected_protocol =
					self.detect_protocol(conn.buffer());

				match detected_protocol {
					Some("ws") => {
						if let Some(ref ws_handler) =
							self.websocket_handler
						{
							ws_handler
								.handle_connection(
									conn,
								)?;
						} else {
							return Err("WebSocket handler not available".into());
						}
					}
					Some("http") => {
						if let Some(ref http_handler) =
							self.http_handler
						{
							http_handler
								.handle_connection(
									conn,
								)?;
						} else {
							return Err("HTTP handler not available".into());
						}
					}
					_ => {
						// If we can't detect yet and
						// buffer is small, wait for
						// more data
						if conn.buffer().len() < 64 {
							return Ok(());
						}
						// If buffer is large but no
						// protocol detected, default to
						// HTTP
						if let Some(ref http_handler) =
							self.http_handler
						{
							http_handler
								.handle_connection(
									conn,
								)?;
						} else {
							return Err("No suitable protocol handler found".into());
						}
					}
				}
			}
			Err(e) if e.kind()
				== std::io::ErrorKind::WouldBlock =>
			{
				// No data available yet
				return Ok(());
			}
			Err(e) => {
				return Err(e.into());
			}
		}
		Ok(())
	}

	fn detect_protocol(&self, buffer: &[u8]) -> Option<&'static str> {
		// Check WebSocket first (more specific)
		if let Some(ref ws_handler) = self.websocket_handler {
			if <WebSocketHandler as ProtocolHandler<T>>::can_handle(
				ws_handler, buffer,
			) {
				return Some("ws");
			}
		}

		// Check HTTP
		if let Some(ref http_handler) = self.http_handler {
			if <HttpHandler as ProtocolHandler<T>>::can_handle(
				http_handler,
				buffer,
			) {
				return Some("http");
			}
		}

		None
	}

	fn handle_read_event(
		&self,
		conn: &mut Connection<T>,
	) -> Result<(), Box<dyn std::error::Error>> {
		match conn.state() {
			ConnectionState::WebSocket(_) => {
				if let Some(ref ws_handler) =
					self.websocket_handler
				{
					ws_handler.handle_read(conn).map_err(
						|e| {
							format!(
								"WebSocket read error: {}",
								e
							)
						},
					)?;
				}
			}
			ConnectionState::Http(_) => {
				if let Some(ref http_handler) =
					self.http_handler
				{
					http_handler
						.handle_read(conn)
						.map_err(|e| {
							format!(
								"HTTP read error: {}",
								e
							)
						})?;
				}
			}
			ConnectionState::Detecting => {
				// Already handled in detect_and_init_protocol
			}
			ConnectionState::Closed => {
				// Connection is closed, nothing to read
			}
		}
		Ok(())
	}

	fn handle_write_event(
		&self,
		conn: &mut Connection<T>,
	) -> Result<(), Box<dyn std::error::Error>> {
		match conn.state() {
			ConnectionState::WebSocket(_) => {
				if let Some(ref ws_handler) =
					self.websocket_handler
				{
					ws_handler.handle_write(conn).map_err(
						|e| {
							format!(
								"WebSocket write error: {}",
								e
							)
						},
					)?;
				}
			}
			ConnectionState::Http(_) => {
				if let Some(ref http_handler) =
					self.http_handler
				{
					http_handler
						.handle_write(conn)
						.map_err(|e| {
							format!(
								"HTTP write error: {}",
								e
							)
						})?;
				}
			}
			ConnectionState::Detecting => {
				// Nothing to write during detection
			}
			ConnectionState::Closed => {
				// Connection is closed, nothing to write
			}
		}
		Ok(())
	}

	fn close_connection(
		&self,
		connections: &mut Slab<Connection<T>>,
		key: usize,
	) {
		if let Some(mut conn) = connections.try_remove(key) {
			conn.shutdown();
		}
	}

	fn cleanup_abandoned_connections(
		&self,
		connections: &mut Slab<Connection<T>>,
	) {
		let mut to_close = Vec::new();
		let mut _state_closed = 0;
		let mut _eof_connections = 0;
		let mut _error_connections = 0;
		let mut _active_connections = 0;

		// Collect keys to check for abandoned connections
		let keys_to_check: Vec<usize> =
			connections.iter().map(|(key, _)| key).collect();

		for key in keys_to_check {
			if let Some(conn) = connections.get_mut(key) {
				match conn.state() {
					ConnectionState::WebSocket(crate::protocols::ws::WsState::Closed) => {
						to_close.push(key);
						_state_closed += 1;
					}
					ConnectionState::Closed => {
						to_close.push(key);
						_state_closed += 1;
					}
					_ => {
						// For active connections, try to detect abandoned ones
						// by attempting a non-blocking read
						let mut buf = [0u8; 1];
						match conn.stream().read(&mut buf) {
							Ok(0) => {
								// EOF - connection is closed on the other end
								to_close.push(key);
								_eof_connections += 1;
							}
							Ok(_n) => {
								// Got data - put it back in buffer for proper processing
								conn.buffer_mut().extend_from_slice(&buf[..1]);
								_active_connections += 1;
							}
							Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
								// Connection is alive but no data available - this is fine
								_active_connections += 1;
							}
							Err(_) => {
								// Connection error - mark for closure
								to_close.push(key);
								_error_connections += 1;
							}
						}
					}
				}
			}
		}

		// Close abandoned connections

		// Close all identified abandoned connections
		for key in to_close {
			self.close_connection(connections, key);
		}
	}
}
