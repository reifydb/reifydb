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
	error::diagnostic::subsystem::{address_unavailable, bind_failed},
	interface::version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_runtime::SharedRuntime;
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_sub_server::state::AppState;
use reifydb_subscription::poller::SubscriptionPoller;
use reifydb_type::error;
use tokio::{
	net::TcpListener,
	select,
	sync::{Semaphore, oneshot, watch},
	time::interval,
};
use tracing::info;

use crate::{handler::handle_connection, subscription::registry::SubscriptionRegistry};

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
	/// Channel to receive shutdown completion.
	shutdown_complete_rx: Option<oneshot::Receiver<()>>,
	/// Semaphore for connection limiting.
	connection_semaphore: Arc<Semaphore>,
	/// Shared tokio runtime.
	runtime: SharedRuntime,
	/// Subscription registry for push notifications.
	registry: Arc<SubscriptionRegistry>,
	/// Subscription polling interval.
	poll_interval: Duration,
	/// Maximum rows to read per subscription per poll.
	poll_batch_size: usize,
}

impl WsSubsystem {
	/// Create a new WebSocket subsystem.
	///
	/// # Arguments
	///
	/// * `bind_addr` - Address and port to bind to (e.g., "0.0.0.0:8091")
	/// * `state` - Shared application state with engine and config
	/// * `runtime` - Shared runtime
	/// * `poll_interval` - Subscription polling interval
	/// * `poll_batch_size` - Maximum rows to read per subscription per poll
	pub fn new(
		bind_addr: String,
		state: AppState,
		runtime: SharedRuntime,
		poll_interval: Duration,
		poll_batch_size: usize,
	) -> Self {
		let max_connections = state.max_connections();
		Self {
			bind_addr,
			actual_addr: RwLock::new(None),
			state,
			running: Arc::new(AtomicBool::new(false)),
			active_connections: Arc::new(AtomicUsize::new(0)),
			shutdown_tx: None,
			shutdown_complete_rx: None,
			connection_semaphore: Arc::new(Semaphore::new(max_connections)),
			runtime,
			registry: Arc::new(SubscriptionRegistry::new()),
			poll_interval,
			poll_batch_size,
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
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
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
		info!("WebSocket server bound to {}", actual_addr);

		let (tx, mut rx) = watch::channel(false);
		let (complete_tx, complete_rx) = oneshot::channel();
		let state = self.state.clone();
		let running = self.running.clone();
		let active_connections = self.active_connections.clone();
		let semaphore = self.connection_semaphore.clone();
		let runtime = self.runtime.clone();
		let runtime_inner = runtime.clone();
		let registry = self.registry.clone();

		// Create subscription poller with configured values
		let poll_interval = self.poll_interval;
		let batch_size = self.poll_batch_size;
		let poller = Arc::new(SubscriptionPoller::new(batch_size));

		// Spawn the subscription poller thread
		let poller_clone = poller.clone();
		let poller_state = state.clone();
		let poller_registry = registry.clone();
		let mut poller_shutdown_rx = rx.clone();
		runtime.spawn(async move {
			let mut tick = interval(poll_interval);
			loop {
				select! {
					biased;

					result = poller_shutdown_rx.changed() => {
						if result.is_err() || *poller_shutdown_rx.borrow() {
							tracing::debug!("Subscription poller shutting down");
							break;
						}
					}

					_ = tick.tick() => {
						let p = poller_clone.clone();
					let e = poller_state.engine_clone();
					let s = poller_state.actor_system();
					let r = poller_registry.clone();
					let _ = tokio::task::spawn_blocking(move || {
						p.poll_all(&e, &s, r.as_ref());
					}).await;
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
								let conn_poller = poller.clone();
								let shutdown_rx = rx.clone();
								let active = active_connections.clone();
								let runtime_handle = runtime_inner.clone();

								active.fetch_add(1, Ordering::SeqCst);
								tracing::debug!("Accepted connection from {}", peer);

								runtime_handle.spawn(async move {
									handle_connection(stream, conn_state, conn_registry, conn_poller, shutdown_rx).await;
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
			let _ = complete_tx.send(());
			info!("WebSocket server stopped");
		});

		self.shutdown_tx = Some(tx);
		self.shutdown_complete_rx = Some(complete_rx);
		Ok(())
	}

	fn shutdown(&mut self) -> reifydb_type::Result<()> {
		if let Some(tx) = self.shutdown_tx.take() {
			let _ = tx.send(true);
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
