// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	any::Any,
	io,
	net::SocketAddr,
	sync::{
		Arc, RwLock,
		atomic::{AtomicBool, Ordering},
	},
};

use axum::{serve, serve::ListenerExt};
use reifydb_core::{
	error::CoreError,
	interface::version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_runtime::SharedRuntime;
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_sub_server::state::AppState;
use reifydb_type::Result;
use tokio::{net::TcpListener, sync::oneshot};
use tracing::{error, info, warn};

use crate::{routes::router, state::HttpServerState};

/// HTTP server subsystem.
///
/// Manages an Axum-based HTTP server with support for:
/// - Graceful startup and shutdown
/// - Health monitoring
///
/// # Example
///
/// ```ignore
/// let state = AppState::new(pool, engine, QueryConfig::default(), RequestInterceptorChain::empty());
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
	bind_addr: Option<String>,
	/// Address to bind the admin server to.
	admin_bind_addr: Option<String>,
	/// Actual bound address (available after start).
	actual_addr: RwLock<Option<SocketAddr>>,
	/// Actual bound address for admin server (available after start).
	admin_actual_addr: RwLock<Option<SocketAddr>>,
	/// Shared application state.
	state: AppState,
	/// Flag indicating if the server is running.
	running: Arc<AtomicBool>,
	/// Channel to send shutdown signal.
	shutdown_tx: Option<oneshot::Sender<()>>,
	/// Channel to receive shutdown completion.
	shutdown_complete_rx: Option<oneshot::Receiver<()>>,
	/// Channel to send admin shutdown signal.
	admin_shutdown_tx: Option<oneshot::Sender<()>>,
	/// Channel to receive admin shutdown completion.
	admin_shutdown_complete_rx: Option<oneshot::Receiver<()>>,
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
	pub fn new(
		bind_addr: Option<String>,
		admin_bind_addr: Option<String>,
		state: AppState,
		runtime: SharedRuntime,
	) -> Self {
		Self {
			bind_addr,
			admin_bind_addr,
			actual_addr: RwLock::new(None),
			admin_actual_addr: RwLock::new(None),
			state,
			running: Arc::new(AtomicBool::new(false)),
			shutdown_tx: None,
			shutdown_complete_rx: None,
			admin_shutdown_tx: None,
			admin_shutdown_complete_rx: None,
			runtime,
		}
	}

	/// Get the bind address.
	pub fn bind_addr(&self) -> Option<&str> {
		self.bind_addr.as_deref()
	}

	/// Get the actual bound address (available after start).
	pub fn local_addr(&self) -> Option<SocketAddr> {
		*self.actual_addr.read().unwrap()
	}

	/// Get the actual bound port (available after start).
	pub fn port(&self) -> Option<u16> {
		self.local_addr().map(|a| a.port())
	}

	/// Get the actual bound address for the admin server (available after start).
	pub fn admin_local_addr(&self) -> Option<SocketAddr> {
		*self.admin_actual_addr.read().unwrap()
	}

	/// Get the actual bound port for the admin server (available after start).
	pub fn admin_port(&self) -> Option<u16> {
		self.admin_local_addr().map(|a| a.port())
	}

	fn spawn_main_server(&mut self) -> Result<()> {
		let Some(addr) = self.bind_addr.clone() else {
			self.running.store(true, Ordering::SeqCst);
			return Ok(());
		};
		let listener = self.bind_listener(&addr)?;
		let actual_addr = local_addr_or_err(&listener)?;
		*self.actual_addr.write().unwrap() = Some(actual_addr);
		info!("HTTP server bound to {}", actual_addr);

		let (shutdown_tx, shutdown_rx) = oneshot::channel();
		let (complete_tx, complete_rx) = oneshot::channel();
		let server_state = HttpServerState::new(self.state.clone());
		let running = self.running.clone();
		self.runtime.spawn(async move {
			running.store(true, Ordering::SeqCst);
			let result = serve_http(listener, server_state, shutdown_rx, "HTTP server").await;
			if let Err(e) = result {
				error!("HTTP server error: {}", e);
			}
			running.store(false, Ordering::SeqCst);
			let _ = complete_tx.send(());
			info!("HTTP server stopped");
		});
		self.shutdown_tx = Some(shutdown_tx);
		self.shutdown_complete_rx = Some(complete_rx);
		Ok(())
	}

	fn spawn_admin_server(&mut self) -> Result<()> {
		let Some(admin_addr) = self.admin_bind_addr.clone() else {
			return Ok(());
		};
		let listener = self.bind_listener(&admin_addr)?;
		let actual_addr = local_addr_or_err(&listener)?;
		*self.admin_actual_addr.write().unwrap() = Some(actual_addr);
		info!("HTTP admin server bound to {}", actual_addr);

		let (admin_shutdown_tx, admin_shutdown_rx) = oneshot::channel();
		let (admin_complete_tx, admin_complete_rx) = oneshot::channel();
		let admin_config = self.state.config().clone().admin_enabled(true);
		let admin_app_state = self.state.clone_with_config(admin_config);
		let admin_server_state = HttpServerState::new(admin_app_state);
		self.runtime.spawn(async move {
			let result =
				serve_http(listener, admin_server_state, admin_shutdown_rx, "HTTP admin server").await;
			if let Err(e) = result {
				error!("HTTP admin server error: {}", e);
			}
			let _ = admin_complete_tx.send(());
			info!("HTTP admin server stopped");
		});
		self.admin_shutdown_tx = Some(admin_shutdown_tx);
		self.admin_shutdown_complete_rx = Some(admin_complete_rx);
		Ok(())
	}

	#[inline]
	fn bind_listener(&self, addr: &str) -> Result<TcpListener> {
		self.runtime.block_on(TcpListener::bind(addr)).map_err(|e| {
			CoreError::SubsystemBindFailed {
				addr: addr.to_string(),
				reason: e.to_string(),
			}
			.into()
		})
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

	fn start(&mut self) -> Result<()> {
		if self.running.load(Ordering::SeqCst) {
			return Ok(());
		}
		self.spawn_main_server()?;
		self.spawn_admin_server()?;
		Ok(())
	}

	fn shutdown(&mut self) -> Result<()> {
		// Shutdown admin server first
		if let Some(tx) = self.admin_shutdown_tx.take() {
			let _ = tx.send(());
		}
		if let Some(rx) = self.admin_shutdown_complete_rx.take() {
			let _ = self.runtime.block_on(rx);
		}
		// Then shutdown main server
		if let Some(tx) = self.shutdown_tx.take() {
			let _ = tx.send(());
		}
		if let Some(rx) = self.shutdown_complete_rx.take() {
			let _ = self.runtime.block_on(rx);
		}
		self.running.store(false, Ordering::SeqCst);
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

#[inline]
fn local_addr_or_err(listener: &TcpListener) -> Result<SocketAddr> {
	listener.local_addr().map_err(|e| {
		CoreError::SubsystemAddressUnavailable {
			reason: e.to_string(),
		}
		.into()
	})
}

async fn serve_http(
	listener: TcpListener,
	server_state: HttpServerState,
	shutdown_rx: oneshot::Receiver<()>,
	name: &'static str,
) -> io::Result<()> {
	let app = router(server_state);
	let listener = listener.tap_io(|tcp_stream| {
		if let Err(e) = tcp_stream.set_nodelay(true) {
			warn!("Failed to set TCP_NODELAY: {e}");
		}
	});
	serve(listener, app)
		.with_graceful_shutdown(async move {
			shutdown_rx.await.ok();
			info!("{} received shutdown signal", name);
		})
		.await
}
