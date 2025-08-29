// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	marker::PhantomData,
	net::{SocketAddr, ToSocketAddrs},
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread::{self, JoinHandle},
};

use reifydb_core::interface::Transaction;
use reifydb_engine::StandardEngine;
use socket2::{Domain, Protocol, Socket, Type};

use crate::{
	config::ServerConfig,
	protocols::{HttpHandler, WebSocketHandler},
};

/// Worker pool that manages multiple worker threads for handling connections
pub struct WorkerPool<T: Transaction> {
	workers: Vec<JoinHandle<()>>,
	shutdown: Arc<AtomicBool>,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> WorkerPool<T> {
	pub fn new(
		config: ServerConfig,
		engine: StandardEngine<T>,
		websocket_handler: Option<WebSocketHandler>,
		http_handler: Option<HttpHandler>,
	) -> Self {
		let worker_count = config.effective_workers();
		let shutdown = Arc::new(AtomicBool::new(false));
		let mut workers = Vec::with_capacity(worker_count);

		// Parse bind address
		let addrs: Vec<SocketAddr> = config
			.bind_addr
			.to_socket_addrs()
			.expect("invalid bind addr")
			.collect();
		let addr = *addrs.first().expect("no resolved addr");

		let _enabled_protocols = Self::get_protocol_names(
			&websocket_handler,
			&http_handler,
		);

		// Create worker threads using the existing mio-based Worker
		for worker_id in 0..worker_count {
			let listener = Self::create_listener(
				addr,
				config.network.reuse_port,
			)
			.expect("failed to create listener");

			let config_clone = config.clone();
			let engine_clone = engine.clone();
			let shutdown_clone = Arc::clone(&shutdown);
			let ws_handler = websocket_handler.clone();
			let http_handler = http_handler.clone();

			let handle = thread::Builder::new()
				.name(format!("reifydb-proto-{}", worker_id))
				.spawn(move || {
					let mut worker =
						super::worker::Worker::new(
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
			_phantom: PhantomData,
		}
	}

	fn get_protocol_names(
		websocket: &Option<WebSocketHandler>,
		http: &Option<HttpHandler>,
	) -> Vec<String> {
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

		let socket =
			Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

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
		// Signal all workers to stop
		self.shutdown.store(true, Ordering::Relaxed);

		// Wait for all workers to complete
		for handle in self.workers {
			let _ = handle.join();
		}
	}
}
