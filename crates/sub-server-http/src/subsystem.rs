// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! HTTP server subsystem implementing the ReifyDB Subsystem trait.
//!
//! This module provides `HttpSubsystem` which manages the lifecycle of the
//! HTTP server, including startup, health monitoring, and graceful shutdown.

use std::{
	any::Any,
	net::SocketAddr,
	sync::{
		Arc, RwLock,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_core::{
	error::diagnostic::subsystem::{address_unavailable, bind_failed},
	interface::version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_runtime::SharedRuntime;
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_sub_server::state::AppState;
use reifydb_type::error;
use tokio::{net::TcpListener, sync::oneshot};

use crate::routes::router;

/// HTTP server subsystem.
///
/// Manages an Axum-based HTTP server with support for:
/// - Graceful startup and shutdown
/// - Health monitoring
///
/// # Example
///
/// ```ignore
/// let state = AppState::new(pool, engine, QueryConfig::default());
///
/// let mut http = HttpSubsystem::new(
///     "0.0.0.0:8090".to_string(),
///     state,
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
	/// Flag indicating if the server is running.
	running: Arc<AtomicBool>,
	/// Channel to send shutdown signal.
	shutdown_tx: Option<oneshot::Sender<()>>,
	/// Channel to receive shutdown completion.
	shutdown_complete_rx: Option<oneshot::Receiver<()>>,
	/// Shared tokio runtime.
	runtime: SharedRuntime,
}

impl HttpSubsystem {
	/// Create a new HTTP subsystem.
	///
	/// # Arguments
	///
	/// * `bind_addr` - Address and port to bind to (e.g., "0.0.0.0:8090")
	/// * `state` - Shared application state with engine and config
	/// * `runtime` - Shared runtime
	pub fn new(bind_addr: String, state: AppState, runtime: SharedRuntime) -> Self {
		Self {
			bind_addr,
			actual_addr: RwLock::new(None),
			state,
			running: Arc::new(AtomicBool::new(false)),
			shutdown_tx: None,
			shutdown_complete_rx: None,
			runtime,
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
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
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

	fn start(&mut self) -> reifydb_type::Result<()> {
		// Idempotent: if already running, return success
		if self.running.load(Ordering::SeqCst) {
			return Ok(());
		}

		let addr = self.bind_addr.clone();
		let runtime = self.runtime.clone();
		let listener = runtime.block_on(TcpListener::bind(&addr)).map_err(|e| error!(bind_failed(&addr, e)))?;

		let actual_addr = listener.local_addr().map_err(|e| error!(address_unavailable(e)))?;
		*self.actual_addr.write().unwrap() = Some(actual_addr);
		tracing::info!("HTTP server bound to {}", actual_addr);

		let (shutdown_tx, shutdown_rx) = oneshot::channel();
		let (complete_tx, complete_rx) = oneshot::channel();

		let state = self.state.clone();
		let running = self.running.clone();
		let runtime = self.runtime.clone();

		runtime.spawn(async move {
			// Mark as running
			running.store(true, Ordering::SeqCst);

			// Create router and serve
			let app = router(state);
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

	fn shutdown(&mut self) -> reifydb_type::Result<()> {
		if let Some(tx) = self.shutdown_tx.take() {
			let _ = tx.send(());
		}
		if let Some(rx) = self.shutdown_complete_rx.take() {
			let _ = self.runtime.block_on(rx);
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
