// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
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

use crate::{config::ServerConfig, connection::Connection};

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
		}
	}

	pub fn run(&mut self) {
		if self.config.pin_threads {
			if let Some(core) = core_affinity::get_core_ids()
				.and_then(|v| v.get(self.worker_id).cloned())
			{
				core_affinity::set_for_current(core);
				println!(
					"Worker {}/{} pinned to core {:?}",
					self.worker_id,
					self.worker_count,
					core.id
				);
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

		println!(
			"Worker {}/{} started, entering event loop",
			self.worker_id, self.worker_count
		);

		loop {
			// Check for shutdown signal
			if self.shutdown.load(Ordering::Relaxed) {
				println!(
					"Worker {}/{} shutting down",
					self.worker_id, self.worker_count
				);
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

		println!(
			"Worker {}/{} stopped",
			self.worker_id, self.worker_count
		);
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
		poll: &Poll,
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

		if let Err(e) =
			self.on_connection_event(connections, poll, key, event)
		{
			eprintln!(
				"Connection {} event error in worker {}: {:?} -> closing",
				key, self.worker_id, e
			);
			self.close_connection(connections, poll, key);
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
			conn.handle_read()?;
		}

		if event.is_writable() {
			conn.handle_write()?;
		}

		if event.is_error() {
			return Err("Connection error".into());
		}

		Ok(())
	}

	fn close_connection(
		&self,
		connections: &mut Slab<Connection<T>>,
		poll: &Poll,
		key: usize,
	) {
		if let Some(mut conn) = connections.try_remove(key) {
			if let Err(e) =
				poll.registry().deregister(conn.stream())
			{
				eprintln!(
					"Failed to deregister connection {}: {:?}",
					key, e
				);
			}
		}
	}
}
