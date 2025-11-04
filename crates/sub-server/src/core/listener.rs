// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	net::{SocketAddr, TcpListener, ToSocketAddrs},
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread::{self, JoinHandle},
};

use reifydb_engine::StandardEngine;
use reifydb_sub_api::SchedulerService;
use socket2::{Domain, Protocol, Socket, Type};

use super::worker::Worker;
use crate::{
	config::ServerConfig,
	protocols::{HttpHandler, WebSocketHandler},
};

/// Worker pool that manages multiple worker threads for handling connections
pub struct Listener {
	listeners: Vec<JoinHandle<()>>,
	shutdown: Arc<AtomicBool>,
	bound_port: u16,
}

impl Listener {
	pub fn new(
		config: ServerConfig,
		engine: StandardEngine,
		scheduler: SchedulerService,
		websocket_handler: Option<WebSocketHandler>,
		http_handler: Option<HttpHandler>,
	) -> Self {
		let listener_count = config.effective_listeners();
		let shutdown = Arc::new(AtomicBool::new(false));
		let mut listener_handles = Vec::with_capacity(listener_count);

		let addrs: Vec<SocketAddr> = config.bind_addr.to_socket_addrs().expect("invalid bind addr").collect();
		let addr = *addrs.first().expect("no resolved addr");

		let mut bound_port = addr.port();
		let mut actual_addr = addr;

		for listener_id in 0..listener_count {
			let listener = Self::create_listener(actual_addr, config.network.reuse_port)
				.expect("failed to create listener");

			// Get the actual bound port from the first listener
			// (important for port 0) and update the address for
			// subsequent workers
			if listener_id == 0 {
				if let Ok(local_addr) = listener.local_addr() {
					bound_port = local_addr.port();
					// Update the address with the actual
					// port for subsequent workers
					actual_addr.set_port(bound_port);
				}
			}

			let config_clone = config.clone();
			let engine_clone = engine.clone();
			let scheduler_clone = scheduler.clone();
			let shutdown_clone = Arc::clone(&shutdown);
			let ws_handler = websocket_handler.clone();
			let http_handler = http_handler.clone();

			let handle = thread::Builder::new()
				.name(format!("listener-{}", listener_id))
				.spawn(move || {
					let mut listener = Worker::new(
						listener_id,
						listener,
						config_clone,
						shutdown_clone,
						engine_clone,
						scheduler_clone,
					);

					// Add protocol handlers to the worker
					if let Some(ws) = ws_handler {
						listener.with_websocket(ws);
					}
					if let Some(http) = http_handler {
						listener.with_http(http);
					}

					listener.run();
				})
				.expect("failed to spawn listener thread");

			listener_handles.push(handle);
		}

		Self {
			listeners: listener_handles,
			shutdown,
			bound_port,
		}
	}

	fn create_listener(addr: SocketAddr, reuse_port: bool) -> Result<TcpListener, Box<dyn std::error::Error>> {
		let domain = match addr {
			SocketAddr::V4(_) => Domain::IPV4,
			SocketAddr::V6(_) => Domain::IPV6,
		};

		let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
		if reuse_port {
			socket.set_reuse_port(true)?;
		}

		socket.set_reuse_address(true)?;
		socket.set_nonblocking(true)?;

		socket.set_tcp_nodelay(true)?;
		socket.bind(&addr.into())?;
		socket.listen(4096)?;

		Ok(socket.into())
	}

	pub fn stop(self) {
		drop(self);
	}

	/// Get the actual bound port of the server
	pub fn port(&self) -> u16 {
		self.bound_port
	}
}

impl Drop for Listener {
	fn drop(&mut self) {
		self.shutdown.store(true, Ordering::Relaxed);

		// Wait for all workers to complete
		// Using drain to take ownership of the handles
		for handle in self.listeners.drain(..) {
			// Best effort join - ignore errors during drop
			let _ = handle.join();
		}
	}
}
