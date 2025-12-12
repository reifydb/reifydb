// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! HTTP server subsystem implementing the ReifyDB Subsystem trait.
//!
//! This module provides `HttpSubsystem` which manages the lifecycle of the
//! HTTP server, including startup, health monitoring, and graceful shutdown.

use std::any::Any;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use reifydb_core::interface::version::{HasVersion, SystemVersion, ComponentType};
use reifydb_sub_api::{HealthStatus, Subsystem};
use reifydb_sub_server::AppState;
use tokio::runtime::Handle;
use tokio::sync::oneshot;

/// HTTP server subsystem.
///
/// Manages an Axum-based HTTP server with support for:
/// - Graceful startup and shutdown
/// - Health monitoring
/// - Integration with shared tokio runtime
///
/// # Example
///
/// ```ignore
/// let runtime = SharedRuntime::new(4);
/// let state = AppState::new(engine, QueryConfig::default());
///
/// let mut http = HttpSubsystem::new(
///     "0.0.0.0:8090".to_string(),
///     state,
///     runtime.handle(),
/// );
///
/// http.start()?;
/// // Server is now accepting connections
///
/// http.shutdown()?;
/// // Server has gracefully stopped
/// ```
pub struct HttpSubsystem {
	/// Address to bind the server to.
	bind_addr: String,
	/// Actual bound address (available after start).
	actual_addr: RwLock<Option<SocketAddr>>,
	/// Shared application state.
	state: AppState,
	/// Handle to the tokio runtime.
	handle: Handle,
	/// Flag indicating if the server is running.
	running: Arc<AtomicBool>,
	/// Channel to send shutdown signal.
	shutdown_tx: Option<oneshot::Sender<()>>,
	/// Channel to receive shutdown completion.
	shutdown_complete_rx: Option<oneshot::Receiver<()>>,
}

impl HttpSubsystem {
	/// Create a new HTTP subsystem.
	///
	/// # Arguments
	///
	/// * `bind_addr` - Address and port to bind to (e.g., "0.0.0.0:8090")
	/// * `state` - Shared application state with engine and config
	/// * `handle` - Handle to the tokio runtime
	pub fn new(bind_addr: String, state: AppState, handle: Handle) -> Self {
		Self {
			bind_addr,
			actual_addr: RwLock::new(None),
			state,
			handle,
			running: Arc::new(AtomicBool::new(false)),
			shutdown_tx: None,
			shutdown_complete_rx: None,
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
}

impl HasVersion for HttpSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "http".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "HTTP server subsystem for query and command handling".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

impl Subsystem for HttpSubsystem {
	fn name(&self) -> &'static str {
		"Http"
	}

	fn start(&mut self) -> reifydb_core::Result<()> {
		// Idempotent: if already running, return success
		if self.running.load(Ordering::SeqCst) {
			return Ok(());
		}

		// Bind synchronously using std::net, then convert to tokio
		let addr = self.bind_addr.clone();
		let std_listener = std::net::TcpListener::bind(&addr)
			.map_err(|e| reifydb_core::error!(reifydb_core::diagnostic::network::bind_error(&addr, e)))?;
		std_listener.set_nonblocking(true)
			.map_err(|e| reifydb_core::error!(reifydb_core::diagnostic::network::bind_error(&addr, e)))?;

		let actual_addr = std_listener.local_addr()
			.map_err(|e| reifydb_core::error!(reifydb_core::diagnostic::network::local_addr_error(e)))?;

		// Enter the runtime context to convert std listener to tokio
		let _guard = self.handle.enter();
		let listener = tokio::net::TcpListener::from_std(std_listener)
			.map_err(|e| reifydb_core::error!(reifydb_core::diagnostic::network::bind_error(&addr, e)))?;
		*self.actual_addr.write().unwrap() = Some(actual_addr);
		tracing::info!("HTTP server bound to {}", actual_addr);

		let (shutdown_tx, shutdown_rx) = oneshot::channel();
		let (complete_tx, complete_rx) = oneshot::channel();

		let state = self.state.clone();
		let running = self.running.clone();

		self.handle.spawn(async move {
			// Mark as running
			running.store(true, Ordering::SeqCst);

			// Create router and serve
			let app = crate::routes::router(state);
			let server = axum::serve(listener, app).with_graceful_shutdown(async {
				shutdown_rx.await.ok();
				tracing::info!("HTTP server received shutdown signal");
			});

			// Run until shutdown
			if let Err(e) = server.await {
				tracing::error!("HTTP server error: {}", e);
			}

			// Mark as stopped
			running.store(false, Ordering::SeqCst);
			let _ = complete_tx.send(());
			tracing::info!("HTTP server stopped");
		});

		self.shutdown_tx = Some(shutdown_tx);
		self.shutdown_complete_rx = Some(complete_rx);
		Ok(())
	}

	fn shutdown(&mut self) -> reifydb_core::Result<()> {
		// Send shutdown signal
		if let Some(tx) = self.shutdown_tx.take() {
			let _ = tx.send(());
		}

		// Wait for graceful shutdown with timeout
		if let Some(rx) = self.shutdown_complete_rx.take() {
			let handle = self.handle.clone();
			handle.block_on(async {
				match tokio::time::timeout(std::time::Duration::from_secs(30), rx).await {
					Ok(_) => {
						tracing::debug!("HTTP server shutdown completed");
					}
					Err(_) => {
						tracing::warn!("HTTP server shutdown timed out");
					}
				}
			});
		}

		Ok(())
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::SeqCst)
	}

	fn health_status(&self) -> HealthStatus {
		if self.running.load(Ordering::SeqCst) {
			HealthStatus::Healthy
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