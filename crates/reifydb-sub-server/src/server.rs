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

use reifydb_core::interface::Transaction;
use reifydb_engine::StandardEngine;
use socket2::{Domain, Protocol, Socket, Type};

use crate::{config::ServerConfig, worker::Worker};

pub struct WebSocketServer<T: Transaction> {
	config: ServerConfig,
	workers: Vec<JoinHandle<()>>,
	shutdown: Arc<AtomicBool>,
	engine: StandardEngine<T>,
}

impl<T: Transaction> WebSocketServer<T> {
	pub fn new(config: ServerConfig, engine: StandardEngine<T>) -> Self {
		Self {
			config,
			workers: Vec::new(),
			shutdown: Arc::new(AtomicBool::new(false)),
			engine,
		}
	}

	pub fn start(&mut self) {
		let worker_count = self
			.config
			.workers
			.unwrap_or_else(|| num_cpus::get_physical());

		println!(
			"Starting WebSocket server: addr={} workers={} reuse_port={}",
			self.config.bind_addr,
			worker_count,
			self.config.reuse_port
		);

		let addrs: Vec<SocketAddr> = self
			.config
			.bind_addr
			.to_socket_addrs()
			.expect("invalid bind addr")
			.collect();
		let addr = *addrs.first().expect("no resolved addr");

		let mut handles = Vec::with_capacity(worker_count);

		for worker_id in 0..worker_count {
			let listener = self
				.build_listener(addr)
				.expect("failed to build listener");
			let config = self.config.clone();
			let shutdown = Arc::clone(&self.shutdown);
			let engine = self.engine.clone();

			let handle = thread::Builder::new()
				.name(format!("reifydb-ws-{}", worker_id))
				.spawn(move || {
					let mut worker = Worker::new(
						worker_id,
						worker_count,
						listener,
						config,
						shutdown,
						engine,
					);
					worker.run();
				})
				.expect("failed to spawn worker thread");

			handles.push(handle);
		}

		self.workers = handles;
		println!(
			"WebSocket server started with {} workers",
			worker_count
		);
	}

	pub fn stop(&mut self) {
		println!("Shutting down WebSocket server...");

		// Signal all workers to stop
		self.shutdown.store(true, Ordering::Relaxed);

		// Wait for all workers to complete
		while let Some(handle) = self.workers.pop() {
			if let Err(e) = handle.join() {
				eprintln!("Worker thread panicked: {:?}", e);
			}
		}

		println!("WebSocket server stopped");
	}

	fn build_listener(
		&self,
		addr: SocketAddr,
	) -> Result<TcpListener, Box<dyn std::error::Error>> {
		let domain = match addr {
			SocketAddr::V4(_) => Domain::IPV4,
			SocketAddr::V6(_) => Domain::IPV6,
		};

		let socket =
			Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

		if self.config.reuse_port {
			socket.set_reuse_port(true)?;
		}

		socket.set_reuse_address(true)?;
		socket.set_nonblocking(true)?;
		socket.bind(&addr.into())?;
		socket.listen(1024)?;

		Ok(socket.into())
	}
}

impl<T: Transaction> Drop for WebSocketServer<T> {
	fn drop(&mut self) {
		if !self.workers.is_empty() {
			self.stop();
		}
	}
}
