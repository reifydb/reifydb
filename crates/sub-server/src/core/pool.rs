// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	net::{SocketAddr, ToSocketAddrs},
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread::{self, JoinHandle},
};

use reifydb_engine::StandardEngine;
use socket2::{Domain, Protocol, Socket, Type};

use crate::{
	config::ServerConfig,
	protocols::{HttpHandler, WebSocketHandler},
};

/// Worker pool that manages multiple worker threads for handling connections
pub struct WorkerPool {
	workers: Vec<JoinHandle<()>>,
	shutdown: Arc<AtomicBool>,
	bound_port: u16,
}

impl WorkerPool {
	pub fn new(
		config: ServerConfig,
		engine: StandardEngine,
		websocket_handler: Option<WebSocketHandler>,
		http_handler: Option<HttpHandler>,
	) -> Self {
		let worker_count = config.effective_workers();
		let shutdown = Arc::new(AtomicBool::new(false));
		let mut workers = Vec::with_capacity(worker_count);

		// Parse bind address
		let addrs: Vec<SocketAddr> = config.bind_addr.to_socket_addrs().expect("invalid bind addr").collect();
		let addr = *addrs.first().expect("no resolved addr");

		let _enabled_protocols = Self::get_protocol_names(&websocket_handler, &http_handler);

		// Store the actual bound port from the first listener
		let mut bound_port = addr.port();
		let mut actual_addr = addr;

		// Create worker threads using the existing mio-based Worker
		for worker_id in 0..worker_count {
			let listener = Self::create_listener(actual_addr, config.network.reuse_port)
				.expect("failed to create listener");

			// Get the actual bound port from the first listener
			// (important for port 0) and update the address for
			// subsequent workers
			if worker_id == 0 {
				if let Ok(local_addr) = listener.local_addr() {
					bound_port = local_addr.port();
					// Update the address with the actual
					// port for subsequent workers
					actual_addr.set_port(bound_port);
				}
			}

			let config_clone = config.clone();
			let engine_clone = engine.clone();
			let shutdown_clone = Arc::clone(&shutdown);
			let ws_handler = websocket_handler.clone();
			let http_handler = http_handler.clone();

			let handle = thread::Builder::new()
				.name(format!("reifydb-proto-{}", worker_id))
				.spawn(move || {
					let mut worker = super::worker::Worker::new(
						worker_id,
						listener,
						config_clone,
						shutdown_clone,
						engine_clone,
					);

					// Add protocol handlers to the worker
					if let Some(ws) = ws_handler {
						worker.with_websocket(ws);
					}
					if let Some(http) = http_handler {
						worker.with_http(http);
					}

					worker.run();
				})
				.expect("failed to spawn worker thread");

			workers.push(handle);
		}

		Self {
			workers,
			shutdown,
			bound_port,
		}
	}

	fn get_protocol_names(websocket: &Option<WebSocketHandler>, http: &Option<HttpHandler>) -> Vec<String> {
		let mut protocols = Vec::new();
		if websocket.is_some() {
			protocols.push("WebSocket".to_string());
		}
		if http.is_some() {
			protocols.push("HTTP".to_string());
		}
		if protocols.is_empty() {
			protocols.push("None".to_string());
		}
		protocols
	}

	fn create_listener(
		addr: SocketAddr,
		reuse_port: bool,
	) -> Result<std::net::TcpListener, Box<dyn std::error::Error>> {
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

		// TCP tuning to reduce TIME_WAIT accumulation
		socket.set_tcp_nodelay(true)?;

		socket.bind(&addr.into())?;

		// Increase backlog to handle burst connections better
		socket.listen(4096)?;

		Ok(socket.into())
	}

	pub fn stop(self) {
		// The Drop implementation will handle cleanup
		drop(self);
	}

	/// Get the actual bound port of the server
	pub fn port(&self) -> u16 {
		self.bound_port
	}
}

impl Drop for WorkerPool {
	fn drop(&mut self) {
		// Signal all workers to stop
		self.shutdown.store(true, Ordering::Relaxed);

		// Wait for all workers to complete
		// Using drain to take ownership of the handles
		for handle in self.workers.drain(..) {
			// Best effort join - ignore errors during drop
			let _ = handle.join();
		}
	}
}
