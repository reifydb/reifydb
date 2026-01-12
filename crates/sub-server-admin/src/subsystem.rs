// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Admin server subsystem implementing the ReifyDB Subsystem trait.

use std::{
	any::Any,
	net::SocketAddr,
	sync::{
		Arc, RwLock,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_core::{
	diagnostic::subsystem::{address_unavailable, bind_failed, socket_config_failed},
	error,
	interface::version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_sub_api::{HealthStatus, Subsystem};
use reifydb_core::SharedRuntime;
use tokio::{net::TcpListener, sync::oneshot};

use crate::state::AdminState;

/// Admin server subsystem.
///
/// Manages an Axum-based admin HTTP server with support for:
/// - Graceful startup and shutdown
/// - Health monitoring
pub struct AdminSubsystem {
	/// Address to bind the server to.
	bind_addr: String,
	/// Actual bound address (available after start).
	actual_addr: RwLock<Option<SocketAddr>>,
	/// Shared application state.
	state: AdminState,
	/// Flag indicating if the server is running.
	running: Arc<AtomicBool>,
	/// Channel to send shutdown signal.
	shutdown_tx: Option<oneshot::Sender<()>>,
	/// Channel to receive shutdown completion.
	shutdown_complete_rx: Option<oneshot::Receiver<()>>,
	/// Shared tokio runtime.
	runtime: SharedRuntime,
}

impl AdminSubsystem {
	/// Create a new admin subsystem.
	///
	/// # Arguments
	///
	/// * `bind_addr` - Address and port to bind to (e.g., "127.0.0.1:9090")
	/// * `state` - Shared application state
	/// * `runtime` - Shared runtime
	pub fn new(bind_addr: String, state: AdminState, runtime: SharedRuntime) -> Self {
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

impl HasVersion for AdminSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "admin".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Admin server subsystem for web-based administration".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

impl Subsystem for AdminSubsystem {
	fn name(&self) -> &'static str {
		"Admin"
	}

	fn start(&mut self) -> reifydb_core::Result<()> {
		// Idempotent: if already running, return success
		if self.running.load(Ordering::SeqCst) {
			return Ok(());
		}

		// Bind synchronously using std::net, then convert to tokio
		let addr = self.bind_addr.clone();
		let std_listener = std::net::TcpListener::bind(&addr).map_err(|e| error!(bind_failed(&addr, e)))?;
		std_listener.set_nonblocking(true).map_err(|e| error!(socket_config_failed(e)))?;

		let actual_addr = std_listener.local_addr().map_err(|e| error!(address_unavailable(e)))?;

		// Convert std listener to tokio (we're already in async context)
		let listener = TcpListener::from_std(std_listener).map_err(|e| error!(socket_config_failed(e)))?;
		*self.actual_addr.write().unwrap() = Some(actual_addr);
		tracing::info!("Admin server bound to {}", actual_addr);

		let (shutdown_tx, shutdown_rx) = oneshot::channel();
		let (complete_tx, complete_rx) = oneshot::channel();

		let state = self.state.clone();
		let running = self.running.clone();
		let runtime = self.runtime.clone();

		runtime.spawn(async move {
			// Mark as running
			running.store(true, Ordering::SeqCst);

			// Create router and serve
			let app = crate::routes::router(state);
			let server = axum::serve(listener, app).with_graceful_shutdown(async {
				shutdown_rx.await.ok();
				tracing::info!("Admin server received shutdown signal");
			});

			// Run until shutdown
			if let Err(e) = server.await {
				tracing::error!("Admin server error: {}", e);
			}

			// Mark as stopped
			running.store(false, Ordering::SeqCst);
			let _ = complete_tx.send(());
			tracing::info!("Admin server stopped");
		});

		self.shutdown_tx = Some(shutdown_tx);
		self.shutdown_complete_rx = Some(complete_rx);
		Ok(())
	}

	fn shutdown(&mut self) -> reifydb_core::Result<()> {
		if let Some(tx) = self.shutdown_tx.take() {
			let _ = tx.send(());
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
