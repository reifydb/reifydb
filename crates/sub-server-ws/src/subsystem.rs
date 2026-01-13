// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

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
	time::Duration,
};

use reifydb_core::{
	diagnostic::subsystem::{address_unavailable, bind_failed},
	error,
	interface::version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_sub_api::{HealthStatus, Subsystem};
use reifydb_core::SharedRuntime;
use reifydb_sub_server::{AppState, ResponseColumn, ResponseFrame};
use reifydb_type::Type;
use tokio::{
	net::TcpListener,
	select,
	sync::{Semaphore, watch},
	time::interval,
};
use tracing::info;

use crate::{handler::handle_connection, subscription::SubscriptionRegistry};

/// WebSocket server subsystem.
///
/// Manages a tokio-tungstenite WebSocket server with support for:
/// - Connection limiting via semaphore
/// - Graceful startup and shutdown
/// - Active connection tracking
/// - Health monitoring with connection count warnings
/// - Subscription push notifications
///
/// # Example
///
/// ```ignore
/// let state = AppState::new(pool, engine, QueryConfig::default());
///
/// let mut ws = WsSubsystem::new(
///     "0.0.0.0:8091".to_string(),
///     state,
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
	/// Flag indicating if the server is running.
	running: Arc<AtomicBool>,
	/// Count of active connections.
	active_connections: Arc<AtomicUsize>,
	/// Channel to send shutdown signal.
	shutdown_tx: Option<watch::Sender<bool>>,
	/// Semaphore for connection limiting.
	connection_semaphore: Arc<Semaphore>,
	/// Shared tokio runtime.
	runtime: SharedRuntime,
	/// Subscription registry for push notifications.
	registry: Arc<SubscriptionRegistry>,
}

impl WsSubsystem {
	/// Create a new WebSocket subsystem.
	///
	/// # Arguments
	///
	/// * `bind_addr` - Address and port to bind to (e.g., "0.0.0.0:8091")
	/// * `state` - Shared application state with engine and config
	/// * `runtime` - Shared runtime
	pub fn new(bind_addr: String, state: AppState, runtime: SharedRuntime) -> Self {
		let max_connections = state.max_connections();
		Self {
			bind_addr,
			actual_addr: RwLock::new(None),
			state,
			running: Arc::new(AtomicBool::new(false)),
			active_connections: Arc::new(AtomicUsize::new(0)),
			shutdown_tx: None,
			connection_semaphore: Arc::new(Semaphore::new(max_connections)),
			runtime,
			registry: Arc::new(SubscriptionRegistry::new()),
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

		let addr = self.bind_addr.clone();
		let runtime = self.runtime.clone();
		let listener = runtime.block_on(TcpListener::bind(&addr)).map_err(|e| error!(bind_failed(&addr, e)))?;

		let actual_addr = listener.local_addr().map_err(|e| error!(address_unavailable(e)))?;
		*self.actual_addr.write().unwrap() = Some(actual_addr);
		info!("WebSocket server bound to {}", actual_addr);

		let (tx, mut rx) = watch::channel(false);
		let state = self.state.clone();
		let running = self.running.clone();
		let active_connections = self.active_connections.clone();
		let semaphore = self.connection_semaphore.clone();
		let runtime = self.runtime.clone();
		let runtime_inner = runtime.clone();
		let registry = self.registry.clone();

		// Spawn the test push thread that broadcasts to all subscriptions every 2 seconds
		let push_registry = registry.clone();
		let mut push_shutdown_rx = rx.clone();
		runtime.spawn(async move {
			let mut tick = interval(Duration::from_secs(2));
			loop {
				select! {
					biased;

					result = push_shutdown_rx.changed() => {
						if result.is_err() || *push_shutdown_rx.borrow() {
							tracing::debug!("Push thread shutting down");
							break;
						}
					}

					_ = tick.tick() => {
						let frame = ResponseFrame {
							row_numbers: vec![0],
							columns: vec![ResponseColumn {
								name: "answer".to_string(),
								r#type: Type::Int8,
								data: vec!["42".to_string()],
							}],
						};
						push_registry.broadcast(frame).await;
					}
				}
			}
		});

		runtime.spawn(async move {
			running.store(true, Ordering::SeqCst);

			loop {
				select! {
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
								let conn_registry = registry.clone();
								let shutdown_rx = rx.clone();
								let active = active_connections.clone();
								let runtime_handle = runtime_inner.clone();

								active.fetch_add(1, Ordering::SeqCst);
								tracing::debug!("Accepted connection from {}", peer);

								runtime_handle.spawn(async move {
									handle_connection(stream, conn_state, conn_registry, shutdown_rx).await;
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
			info!("WebSocket server stopped");
		});

		self.shutdown_tx = Some(tx);
		Ok(())
	}

	fn shutdown(&mut self) -> reifydb_core::Result<()> {
		if let Some(tx) = self.shutdown_tx.take() {
			let _ = tx.send(true);
		}
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
