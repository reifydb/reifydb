// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod factory;
use std::{
	any::Any,
	net::SocketAddr,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

pub use factory::GrpcSubsystemFactory;
use reifydb_core::{Result, interface::Transaction};
use reifydb_engine::StandardEngine;
use reifydb_network::grpc::server::{GrpcConfig, GrpcServer};
use tokio::{sync::oneshot, task::JoinHandle};

use super::Subsystem;
use crate::{context::RuntimeProvider, health::HealthStatus};

pub struct GrpcSubsystem<T: Transaction> {
	/// The wrapped GrpcServer
	server: Option<GrpcServer<T>>,
	/// Whether the server is running
	running: Arc<AtomicBool>,
	/// Handle to the async task
	task_handle: Option<JoinHandle<()>>,
	/// Shared runtime provider
	runtime_provider: RuntimeProvider,
	/// Cached socket address (stored when server starts)
	socket_addr: Option<SocketAddr>,
}

impl<T: Transaction> GrpcSubsystem<T> {
	pub fn new(
		config: GrpcConfig,
		engine: StandardEngine<T>,
		runtime_provider: &RuntimeProvider,
	) -> Self {
		let grpc_server = GrpcServer::new(config, engine);
		Self {
			server: Some(grpc_server),
			running: Arc::new(AtomicBool::new(false)),
			task_handle: None,
			runtime_provider: runtime_provider.clone(),
			socket_addr: None,
		}
	}

	pub fn socket_addr(&self) -> Option<SocketAddr> {
		self.socket_addr
	}
}

impl<T: Transaction> Subsystem for GrpcSubsystem<T> {
	fn name(&self) -> &'static str {
		"Grpc"
	}

	fn start(&mut self) -> Result<()> {
		if self.running.load(Ordering::Relaxed) {
			return Ok(()); // Already running
		}

		if let Some(server) = self.server.take() {
			let running = Arc::clone(&self.running);
			let (addr_tx, addr_rx) = oneshot::channel();

			// Use shared runtime to spawn the server
			let handle = self.runtime_provider.spawn(async move {
				running.store(true, Ordering::Relaxed);
				println!(
					"[GrpcSubsystem] Starting gRPC server"
				);

				// Clone server to capture socket address before
				// serving
				let server_clone = server.clone();

				// Start a task that waits for the socket
				// address to be set
				let addr_task =
					tokio::spawn(async move {
						// Poll until socket address is
						// available (set during
						// serve())
						for _ in 0..50 {
							// Try for up to 500ms
							if let Some(addr) =
								server_clone
									.socket_addr(
									) {
								let _ = addr_tx.send(Some(addr));
								return;
							}
							tokio::time::sleep(std::time::Duration::from_millis(10)).await;
						}
						let _ = addr_tx.send(None);
					});

				// Start serving (this will set the socket
				// address)
				let serve_result = server.serve().await;
				addr_task.abort(); // Clean up address polling task

				if let Err(e) = serve_result {
					eprintln!(
						"[GrpcSubsystem] gRPC server error: {}",
						e
					);
				}

				running.store(false, Ordering::Relaxed);
				println!("[GrpcSubsystem] gRPC server stopped");
			});

			// Wait for the socket address from the async task
			if let Ok(addr) =
				self.runtime_provider.block_on(async {
					tokio::time::timeout(std::time::Duration::from_millis(1000), addr_rx).await
				}) {
				if let Ok(socket_addr) = addr {
					self.socket_addr = socket_addr;
				}
			}

			self.task_handle = Some(handle);
		}

		self.running.store(true, Ordering::Relaxed);
		Ok(())
	}

	fn stop(&mut self) -> Result<()> {
		if !self.running.load(Ordering::Relaxed) {
			return Ok(()); // Already stopped
		}

		self.running.store(false, Ordering::Relaxed);
		self.socket_addr = None;

		if let Some(handle) = self.task_handle.take() {
			handle.abort();
		}

		println!("[GrpcSubsystem] gRPC server stopped");
		Ok(())
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::Relaxed)
	}

	fn health_status(&self) -> HealthStatus {
		if self.is_running() {
			HealthStatus::Healthy
		} else {
			HealthStatus::Unknown
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}
}
