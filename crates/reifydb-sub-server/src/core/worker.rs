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

use super::Connection;
use crate::{
	config::ServerConfig,
	protocols::{HttpHandler, ProtocolHandler, WebSocketHandler},
};

const LISTENER: Token = Token(0);
const WAKE_TOKEN: Token = Token(1);
const TOKEN_BASE: usize = 2;

pub struct Worker<T: Transaction> {
	worker_id: usize,
	worker_count: usize,
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
		worker_count: usize,
		std_listener: std::net::TcpListener,
		config: ServerConfig,
		shutdown: Arc<AtomicBool>,
		engine: StandardEngine<T>,
	) -> Self {
		let listener = TcpListener::from_std(std_listener);

		Self {
			worker_id,
			worker_count,
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

		// Worker started, entering event loop

		loop {
			// Check for shutdown signal
			if self.shutdown.load(Ordering::Relaxed) {
				break;
			}

			if let Err(e) = poll.poll(
				&mut events,
				Some(std::time::Duration::from_millis(100)),
			) {
				if e.kind() == std::io::ErrorKind::Interrupted {
					continue;
				}
				eprintln!(
					"Poll error in worker {}: {:?}",
					self.worker_id, e
				);
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
		}

		// Clean up connections on shutdown
		let keys: Vec<usize> =
			connections.iter().map(|(key, _)| key).collect();
		for key in keys {
			if let Some(mut conn) = connections.try_remove(key) {
				if let Err(e) = poll
					.registry()
					.deregister(conn.stream())
				{
					eprintln!(
						"Failed to deregister connection {}: {:?}",
						key, e
					);
				}
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
					if let Err(e) = self.on_accept(
						connections,
						poll,
						stream,
						peer,
					) {
						eprintln!(
							"Accept error in worker {}: {:?}",
							self.worker_id, e
						);
					}
				}
				Err(e) if e.kind()
					== std::io::ErrorKind::WouldBlock =>
				{
					break;
				}
				Err(e) => {
					eprintln!(
						"Listener accept fatal in worker {}: {:?}",
						self.worker_id, e
					);
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

		if let Err(e) = self.on_connection_event(
			connections,
			&_poll,
			key,
			event,
		) {
			eprintln!(
				"Connection {} event error in worker {}: {:?} -> closing",
				key, self.worker_id, e
			);
			self.close_connection(connections, key);
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
		let entry = connections.vacant_entry();
		let key = entry.key();
		let token = Token(TOKEN_BASE + key);

		poll.registry().register(
			&mut stream,
			token,
			Interest::READABLE,
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

			// Check if we need to switch to writable after
			// processing
			let needs_write = match conn.state() {
                crate::core::ConnectionState::Http(crate::protocols::http::HttpState::WritingResponse(_)) => true,
                crate::core::ConnectionState::WebSocket(crate::protocols::ws::WsState::Handshake(data)) => data.handshake_response.is_some(),
                crate::core::ConnectionState::WebSocket(crate::protocols::ws::WsState::Active(data)) => !data.outbox.is_empty(),
                _ => false,
            };

			if needs_write {
				let token = conn.token();
				poll.registry().reregister(
					conn.stream(),
					token,
					Interest::WRITABLE,
				)?;
			}
		}

		if event.is_writable() {
			self.handle_write_event(conn)?;

			// After writing, check if we should switch back to
			// readable or close
			let next_interest = match conn.state() {
                crate::core::ConnectionState::Http(crate::protocols::http::HttpState::ReadingRequest(_)) => Some(Interest::READABLE),
                crate::core::ConnectionState::WebSocket(_) => Some(Interest::READABLE | Interest::WRITABLE), // Keep both for WebSocket
                crate::core::ConnectionState::Closed => None,
                _ => Some(Interest::READABLE),
            };

			if let Some(interest) = next_interest {
				let token = conn.token();
				poll.registry().reregister(
					conn.stream(),
					token,
					interest,
				)?;
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
			crate::core::ConnectionState::WebSocket(_) => {
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
			crate::core::ConnectionState::Http(_) => {
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
			crate::core::ConnectionState::Detecting => {
				// Already handled in detect_and_init_protocol
			}
			crate::core::ConnectionState::Closed => {
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
			crate::core::ConnectionState::WebSocket(_) => {
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
			crate::core::ConnectionState::Http(_) => {
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
			crate::core::ConnectionState::Detecting => {
				// Nothing to write during detection
			}
			crate::core::ConnectionState::Closed => {
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
		if let Some(_conn) = connections.try_remove(key) {
			// Connection automatically deregistered when dropped
		}
	}
}
