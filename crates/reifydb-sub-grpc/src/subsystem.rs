// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	any::Any,
	net::SocketAddr,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_core::{
	Result,
	interface::{
		Transaction,
		subsystem::{HealthStatus, Subsystem},
	},
};
use reifydb_engine::StandardEngine;
use reifydb_network::grpc::server::{GrpcConfig, GrpcServer};
use tokio::{runtime::Runtime, sync::oneshot, task::JoinHandle};

pub struct GrpcSubsystem<T: Transaction> {
	/// The wrapped GrpcServer
	server: Option<GrpcServer<T>>,
	/// Whether the server is running
	running: Arc<AtomicBool>,
	/// Handle to the async task
	task_handle: Option<JoinHandle<()>>,
	/// Cached socket address (stored when server starts)
	socket_addr: Option<SocketAddr>,
	/// Runtime for spawning tasks
	runtime: Option<Runtime>,
}

impl<T: Transaction> GrpcSubsystem<T> {
	pub fn new(config: GrpcConfig, engine: StandardEngine<T>) -> Self {
		let grpc_server = GrpcServer::new(config, engine);
		Self {
			server: Some(grpc_server),
			running: Arc::new(AtomicBool::new(false)),
			task_handle: None,
			socket_addr: None,
			runtime: None,
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

			// Get or create runtime
			let runtime = if let Ok(_handle) =
				tokio::runtime::Handle::try_current()
			{
				// Use existing runtime
				None
			} else {
				// Create new runtime
				Some(tokio::runtime::Runtime::new().expect(
					"Failed to create Tokio runtime",
				))
			};

			// Spawn the server task
			let handle = if let Some(rt) = &runtime {
				rt.spawn(async move {
					running.store(true, Ordering::Relaxed);
					println!(
						"[GrpcSubsystem] Starting gRPC server"
					);

					// Clone server to capture socket
					// address before serving
					let server_clone = server.clone();

					// Start a task that waits for the
					// socket address to be set
					let addr_task =
						tokio::spawn(async move {
							// Poll until socket
							// address is
							// available (set during
							// serve())
							for _ in 0..50 {
								// Try for up to
								// 500ms
								if let Some(
									addr,
								) = server_clone
									.socket_addr(
									) {
									let _ = addr_tx.send(Some(addr));
									return;
								}
								tokio::time::sleep(std::time::Duration::from_millis(10)).await;
							}
							let _ = addr_tx
								.send(None);
						});

					// Start serving (this will set the
					// socket address)
					let serve_result = server.serve().await;
					addr_task.abort(); // Clean up address polling task

					if let Err(e) = serve_result {
						println!(
							"[GrpcSubsystem] gRPC server error: {}",
							e
						);
					}

					running.store(false, Ordering::Relaxed);
					println!(
						"[GrpcSubsystem] gRPC server stopped"
					);
				})
			} else {
				tokio::spawn(async move {
					running.store(true, Ordering::Relaxed);
					println!(
						"[GrpcSubsystem] Starting gRPC server"
					);

					// Clone server to capture socket
					// address before serving
					let server_clone = server.clone();

					// Start a task that waits for the
					// socket address to be set
					let addr_task =
						tokio::spawn(async move {
							// Poll until socket
							// address is
							// available (set during
							// serve())
							for _ in 0..50 {
								// Try for up to
								// 500ms
								if let Some(
									addr,
								) = server_clone
									.socket_addr(
									) {
									let _ = addr_tx.send(Some(addr));
									return;
								}
								tokio::time::sleep(std::time::Duration::from_millis(10)).await;
							}
							let _ = addr_tx
								.send(None);
						});

					// Start serving (this will set the
					// socket address)
					let serve_result = server.serve().await;
					addr_task.abort(); // Clean up address polling task

					if let Err(e) = serve_result {
						println!(
							"[GrpcSubsystem] gRPC server error: {}",
							e
						);
					}

					running.store(false, Ordering::Relaxed);
					println!(
						"[GrpcSubsystem] gRPC server stopped"
					);
				})
			};

			// Store runtime if we created one
			self.runtime = runtime;

			// Wait for the socket address from the async task
			let block_on_result = if let Some(rt) = &self.runtime {
				rt.block_on(async {
					tokio::time::timeout(std::time::Duration::from_millis(1000), addr_rx).await
				})
			} else {
				tokio::runtime::Handle::current().block_on(async {
					tokio::time::timeout(std::time::Duration::from_millis(1000), addr_rx).await
				})
			};

			if let Ok(addr) = block_on_result {
				if let Ok(socket_addr) = addr {
					self.socket_addr = socket_addr;
				}
			}

			self.task_handle = Some(handle);
		}

		self.running.store(true, Ordering::Relaxed);
		Ok(())
	}

	fn shutdown(&mut self) -> Result<()> {
		if !self.running.load(Ordering::Relaxed) {
			return Ok(()); // Already stopped
		}

		self.running.store(false, Ordering::Relaxed);
		self.socket_addr = None;

		if let Some(handle) = self.task_handle.take() {
			handle.abort();
		}

		// Drop runtime if we created one
		self.runtime = None;

		Ok(())
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::Relaxed)
	}

	fn health_status(&self) -> HealthStatus {
		if !self.is_running() {
			return HealthStatus::Unknown;
		}

		HealthStatus::Healthy
	}

	fn as_any(&self) -> &dyn Any {
		self
	}

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}
}
