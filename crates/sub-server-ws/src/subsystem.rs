// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! WebSocket server subsystem implementing the ReifyDB Subsystem trait.
//!
//! This module provides `WsSubsystem` which manages the lifecycle of the
//! WebSocket server, including startup, connection tracking, and graceful shutdown.

use std::{
	any::Any,
	net::SocketAddr,
	sync::{
		Arc, RwLock,
		atomic::{AtomicBool, AtomicUsize, Ordering},
	},
};

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
use reifydb_sub_api::{HealthStatus, Subsystem};
use reifydb_sub_server::{AppState, SharedRuntime};
use tokio::{
	net::TcpListener,
	runtime::Handle,
	sync::{Semaphore, watch},
};

/// WebSocket server subsystem.
///
/// Manages a tokio-tungstenite WebSocket server with support for:
/// - Connection limiting via semaphore
/// - Graceful startup and shutdown
/// - Active connection tracking
/// - Health monitoring with connection count warnings
///
/// # Example
///
/// ```ignore
/// let runtime = SharedRuntime::new(4);
/// let state = AppState::new(engine, QueryConfig::default());
///
/// let mut ws = WsSubsystem::new(
///     "0.0.0.0:8091".to_string(),
///     state,
///     runtime.handle(),
/// );
///
/// ws.start()?;
/// // Server is now accepting connections
///
/// ws.shutdown()?;
/// // Server has gracefully stopped, connections drained
/// ```
pub struct WsSubsystem {
	/// Address to bind the server to.
	bind_addr: String,
	/// Actual bound address (available after start).
	actual_addr: RwLock<Option<SocketAddr>>,
	/// Shared application state.
	state: AppState,
	/// The shared runtime (kept alive to prevent premature shutdown).
	_runtime: Option<Arc<SharedRuntime>>,
	/// Handle to the tokio runtime.
	handle: Handle,
	/// Flag indicating if the server is running.
	running: Arc<AtomicBool>,
	/// Count of active connections.
	active_connections: Arc<AtomicUsize>,
	/// Channel to send shutdown signal.
	shutdown_tx: Option<watch::Sender<bool>>,
	/// Semaphore for connection limiting.
	connection_semaphore: Arc<Semaphore>,
}

impl WsSubsystem {
	/// Create a new WebSocket subsystem.
	///
	/// # Arguments
	///
	/// * `bind_addr` - Address and port to bind to (e.g., "0.0.0.0:8091")
	/// * `state` - Shared application state with engine and config
	/// * `handle` - Handle to the tokio runtime
	pub fn new(bind_addr: String, state: AppState, handle: Handle) -> Self {
		let max_connections = state.max_connections();
		Self {
			bind_addr,
			actual_addr: RwLock::new(None),
			state,
			_runtime: None,
			handle,
			running: Arc::new(AtomicBool::new(false)),
			active_connections: Arc::new(AtomicUsize::new(0)),
			shutdown_tx: None,
			connection_semaphore: Arc::new(Semaphore::new(max_connections)),
		}
	}

	/// Create a new WebSocket subsystem with an owned runtime.
	///
	/// This variant keeps the runtime alive for the lifetime of the subsystem.
	///
	/// # Arguments
	///
	/// * `bind_addr` - Address and port to bind to (e.g., "0.0.0.0:8091")
	/// * `state` - Shared application state with engine and config
	/// * `runtime` - Shared runtime (will be kept alive)
	pub fn with_runtime(bind_addr: String, state: AppState, runtime: Arc<SharedRuntime>) -> Self {
		let max_connections = state.max_connections();
		let handle = runtime.handle();
		Self {
			bind_addr,
			actual_addr: RwLock::new(None),
			state,
			_runtime: Some(runtime),
			handle,
			running: Arc::new(AtomicBool::new(false)),
			active_connections: Arc::new(AtomicUsize::new(0)),
			shutdown_tx: None,
			connection_semaphore: Arc::new(Semaphore::new(max_connections)),
		}
	}

	/// Get the bind address.
	pub fn bind_addr(&self) -> &str {
		&self.bind_addr
	}

	/// Get the actual bound address (available after start).
	pub fn local_addr(&self) -> Option<SocketAddr> {
		*self.actual_addr.read().unwrap()
	}

	/// Get the actual bound port (available after start).
	pub fn port(&self) -> Option<u16> {
		self.local_addr().map(|a| a.port())
	}

	/// Get the current number of active connections.
	pub fn active_connections(&self) -> usize {
		self.active_connections.load(Ordering::SeqCst)
	}
}

impl HasVersion for WsSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "websocket".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "WebSocket server subsystem for persistent query connections".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

impl Subsystem for WsSubsystem {
	fn name(&self) -> &'static str {
		"WebSocket"
	}

	fn start(&mut self) -> reifydb_core::Result<()> {
		// Idempotent: if already running, return success
		if self.running.load(Ordering::SeqCst) {
			return Ok(());
		}

		// Bind synchronously using std::net, then convert to tokio
		let addr = self.bind_addr.clone();
		let std_listener = std::net::TcpListener::bind(&addr).map_err(|e| {
			reifydb_core::error!(reifydb_core::diagnostic::internal::internal(format!(
				"Failed to bind WebSocket server to {}: {}",
				&addr, e
			)))
		})?;
		std_listener.set_nonblocking(true).map_err(|e| {
			reifydb_core::error!(reifydb_core::diagnostic::internal::internal(format!(
				"Failed to set non-blocking on {}: {}",
				&addr, e
			)))
		})?;

		let actual_addr = std_listener.local_addr().map_err(|e| {
			reifydb_core::error!(reifydb_core::diagnostic::internal::internal(format!(
				"Failed to get local address: {}",
				e
			)))
		})?;

		// Enter the runtime context to convert std listener to tokio
		let _guard = self.handle.enter();
		let listener = TcpListener::from_std(std_listener).map_err(|e| {
			reifydb_core::error!(reifydb_core::diagnostic::internal::internal(format!(
				"Failed to convert listener: {}",
				e
			)))
		})?;
		*self.actual_addr.write().unwrap() = Some(actual_addr);
		tracing::info!("WebSocket server bound to {}", actual_addr);

		let (tx, mut rx) = watch::channel(false);
		let state = self.state.clone();
		let running = self.running.clone();
		let active_connections = self.active_connections.clone();
		let semaphore = self.connection_semaphore.clone();

		self.handle.spawn(async move {
			running.store(true, Ordering::SeqCst);

			loop {
				tokio::select! {
					biased;

					// Check shutdown first
					result = rx.changed() => {
						if result.is_err() || *rx.borrow() {
							tracing::info!("WebSocket server shutting down");
							break;
						}
					}

					// Accept new connections
					accept = listener.accept() => {
						match accept {
							Ok((stream, peer)) => {
								// Try to acquire a permit (non-blocking)
								let permit = match semaphore.clone().try_acquire_owned() {
									Ok(p) => p,
									Err(_) => {
										tracing::warn!("Connection limit reached, rejecting {}", peer);
										// Connection will be dropped, closing it
										continue;
									}
								};

								let conn_state = state.clone();
								let shutdown_rx = rx.clone();
								let active = active_connections.clone();

								active.fetch_add(1, Ordering::SeqCst);
								tracing::debug!("Accepted connection from {}", peer);

								tokio::spawn(async move {
									crate::handler::handle_connection(stream, conn_state, shutdown_rx).await;
									active.fetch_sub(1, Ordering::SeqCst);
									drop(permit); // Release connection slot
								});
							}
							Err(e) => {
								tracing::warn!("Accept error: {}", e);
							}
						}
					}
				}
			}

			running.store(false, Ordering::SeqCst);
			tracing::info!("WebSocket server stopped");
		});

		self.shutdown_tx = Some(tx);
		Ok(())
	}

	fn shutdown(&mut self) -> reifydb_core::Result<()> {
		// Send shutdown signal
		if let Some(tx) = self.shutdown_tx.take() {
			let _ = tx.send(true);
		}

		// Wait for active connections to drain (with timeout)
		let active = self.active_connections.clone();
		let handle = self.handle.clone();

		handle.block_on(async {
			let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
			while active.load(Ordering::SeqCst) > 0 {
				if tokio::time::Instant::now() > deadline {
					tracing::warn!(
						"WebSocket shutdown timeout with {} connections still active",
						active.load(Ordering::SeqCst)
					);
					break;
				}
				tokio::time::sleep(std::time::Duration::from_millis(100)).await;
			}
			tracing::debug!("WebSocket server shutdown completed");
		});

		Ok(())
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::SeqCst)
	}

	fn health_status(&self) -> HealthStatus {
		if self.running.load(Ordering::SeqCst) {
			let active = self.active_connections.load(Ordering::SeqCst);
			let max = self.state.max_connections();

			// Warn if connections are at 90% capacity
			if active > max * 90 / 100 {
				HealthStatus::Warning {
					description: format!("High connection count: {}/{}", active, max),
				}
			} else {
				HealthStatus::Healthy
			}
		} else if self.shutdown_tx.is_some() {
			// Started but not yet running (startup in progress)
			HealthStatus::Warning {
				description: "Starting up".to_string(),
			}
		} else {
			HealthStatus::Failed {
				description: "Not running".to_string(),
			}
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}
}
