// SPDX-License-Identifier: Apache-2.0
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
	error::CoreError,
	interface::version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_runtime::SharedRuntime;
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_sub_server::state::AppState;
use reifydb_sub_subscription::{poller::StoreBackedPoller, store::SubscriptionStore};
use reifydb_type::{Result, error::Error};
use tokio::{
	net::TcpListener,
	select,
	sync::{Semaphore, oneshot, watch},
};
use tracing::{debug, info, warn};

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
/// let state = AppState::new(pool, engine, QueryConfig::default(), RequestInterceptorChain::empty());
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
	/// Count of active connections.
	active_connections: Arc<AtomicUsize>,
	/// Channel to send shutdown signal.
	shutdown_tx: Option<watch::Sender<bool>>,
	/// Channel to receive shutdown completion.
	shutdown_complete_rx: Option<oneshot::Receiver<()>>,
	/// Channel to receive admin shutdown completion.
	admin_shutdown_complete_rx: Option<oneshot::Receiver<()>>,
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
	/// In-memory subscription store (from IoC, if subscription subsystem is active).
	subscription_store: Option<Arc<SubscriptionStore>>,
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
		bind_addr: Option<String>,
		admin_bind_addr: Option<String>,
		state: AppState,
		runtime: SharedRuntime,
		poll_interval: Duration,
		poll_batch_size: usize,
		subscription_store: Option<Arc<SubscriptionStore>>,
	) -> Self {
		let max_connections = state.max_connections();
		Self {
			bind_addr,
			admin_bind_addr,
			actual_addr: RwLock::new(None),
			admin_actual_addr: RwLock::new(None),
			state,
			running: Arc::new(AtomicBool::new(false)),
			active_connections: Arc::new(AtomicUsize::new(0)),
			shutdown_tx: None,
			shutdown_complete_rx: None,
			admin_shutdown_complete_rx: None,
			connection_semaphore: Arc::new(Semaphore::new(max_connections)),
			runtime,
			registry: Arc::new(SubscriptionRegistry::new()),
			poll_interval,
			poll_batch_size,
			subscription_store,
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

	/// Get the current number of active connections.
	pub fn active_connections(&self) -> usize {
		self.active_connections.load(Ordering::SeqCst)
	}

	/// Get the actual bound address for the admin server (available after start).
	pub fn admin_local_addr(&self) -> Option<SocketAddr> {
		*self.admin_actual_addr.read().unwrap()
	}

	/// Get the actual bound port for the admin server (available after start).
	pub fn admin_port(&self) -> Option<u16> {
		self.admin_local_addr().map(|a| a.port())
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

	fn start(&mut self) -> Result<()> {
		// Idempotent: if already running, return success
		if self.running.load(Ordering::SeqCst) {
			return Ok(());
		}

		let runtime = self.runtime.clone();
		let state = self.state.clone();
		let registry = self.registry.clone();

		// Create shutdown watch channel (shared by main, admin, and poller)
		let (tx, rx) = watch::channel(false);

		// Create subscription poller with configured values
		let poll_interval = self.poll_interval;
		let batch_size = self.poll_batch_size;

		// Spawn store-backed subscription poller if store is available
		if let Some(ref store) = self.subscription_store {
			let poller = Arc::new(StoreBackedPoller::new(store.clone(), batch_size));
			let poller_registry = registry.clone();
			let poller_shutdown_rx = rx.clone();
			runtime.spawn(async move {
				poller.run_loop(poller_registry, poll_interval, poller_shutdown_rx).await;
			});
		}

		// Bind main listener if configured
		if let Some(addr) = &self.bind_addr {
			let addr = addr.clone();
			let listener = runtime.block_on(TcpListener::bind(&addr)).map_err(|e| {
				let err: Error = CoreError::SubsystemBindFailed {
					addr: addr.clone(),
					reason: e.to_string(),
				}
				.into();
				err
			})?;

			let actual_addr = listener.local_addr().map_err(|e| {
				let err: Error = CoreError::SubsystemAddressUnavailable {
					reason: e.to_string(),
				}
				.into();
				err
			})?;
			*self.actual_addr.write().unwrap() = Some(actual_addr);
			info!("WebSocket server bound to {}", actual_addr);

			let (complete_tx, complete_rx) = oneshot::channel();
			let running = self.running.clone();
			let active_connections = self.active_connections.clone();
			let semaphore = self.connection_semaphore.clone();
			let runtime_inner = runtime.clone();
			let mut shutdown_rx = rx;

			runtime.spawn(async move {
				running.store(true, Ordering::SeqCst);

				loop {
					select! {
						biased;

						// Check shutdown first
						result = shutdown_rx.changed() => {
							if result.is_err() || *shutdown_rx.borrow() {
								info!("WebSocket server shutting down");
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
											warn!("Connection limit reached, rejecting {}", peer);
											// Connection will be dropped, closing it
											continue;
										}
									};

									let conn_state = state.clone();
									let conn_registry = registry.clone();
										let shutdown_rx = shutdown_rx.clone();
									let active = active_connections.clone();
									let runtime_handle = runtime_inner.clone();

									active.fetch_add(1, Ordering::SeqCst);
									debug!("Accepted connection from {}", peer);

									runtime_handle.spawn(async move {
										handle_connection(stream, conn_state, conn_registry, shutdown_rx).await;
										active.fetch_sub(1, Ordering::SeqCst);
										drop(permit); // Release connection slot
									});
								}
								Err(e) => {
									warn!("Accept error: {}", e);
								}
							}
						}
					}
				}

				running.store(false, Ordering::SeqCst);
				let _ = complete_tx.send(());
				info!("WebSocket server stopped");
			});

			self.shutdown_complete_rx = Some(complete_rx);
		} else {
			// No main listener — mark running synchronously
			self.running.store(true, Ordering::SeqCst);
		}

		self.shutdown_tx = Some(tx);

		// Start admin listener if configured
		if let Some(admin_addr) = &self.admin_bind_addr {
			let admin_addr = admin_addr.clone();
			let runtime = self.runtime.clone();
			let admin_listener = runtime.block_on(TcpListener::bind(&admin_addr)).map_err(|e| {
				let err: Error = CoreError::SubsystemBindFailed {
					addr: admin_addr.clone(),
					reason: e.to_string(),
				}
				.into();
				err
			})?;

			let admin_actual_addr = admin_listener.local_addr().map_err(|e| {
				let err: Error = CoreError::SubsystemAddressUnavailable {
					reason: e.to_string(),
				}
				.into();
				err
			})?;
			*self.admin_actual_addr.write().unwrap() = Some(admin_actual_addr);
			info!("WebSocket admin server bound to {}", admin_actual_addr);

			let (admin_complete_tx, admin_complete_rx) = oneshot::channel();

			// Create admin state with admin_enabled = true, preserving interceptors
			let admin_config = self.state.config().clone().admin_enabled(true);
			let admin_state = self.state.clone_with_config(admin_config);

			// Share the same registry and poller
			let admin_registry = self.registry.clone();
			let admin_semaphore = self.connection_semaphore.clone();
			let admin_active = self.active_connections.clone();
			let mut admin_shutdown_rx = self.shutdown_tx.as_ref().unwrap().subscribe();
			let runtime_inner = runtime.clone();

			runtime.spawn(async move {
				loop {
					select! {
						biased;

						result = admin_shutdown_rx.changed() => {
							if result.is_err() || *admin_shutdown_rx.borrow() {
								info!("WebSocket admin server shutting down");
								break;
							}
						}

						accept = admin_listener.accept() => {
							match accept {
								Ok((stream, peer)) => {
									let permit = match admin_semaphore.clone().try_acquire_owned() {
										Ok(p) => p,
										Err(_) => {
											warn!("Connection limit reached on admin, rejecting {}", peer);
											continue;
										}
									};

									let conn_state = admin_state.clone();
									let conn_registry = admin_registry.clone();
									let shutdown_rx = admin_shutdown_rx.clone();
									let active = admin_active.clone();
									let runtime_handle = runtime_inner.clone();

									active.fetch_add(1, Ordering::SeqCst);
									debug!("Accepted admin connection from {}", peer);

									runtime_handle.spawn(async move {
										handle_connection(stream, conn_state, conn_registry, shutdown_rx).await;
										active.fetch_sub(1, Ordering::SeqCst);
										drop(permit);
									});
								}
								Err(e) => {
									warn!("Admin accept error: {}", e);
								}
							}
						}
					}
				}

				let _ = admin_complete_tx.send(());
				info!("WebSocket admin server stopped");
			});

			self.admin_shutdown_complete_rx = Some(admin_complete_rx);
		}

		Ok(())
	}

	fn shutdown(&mut self) -> Result<()> {
		// Signal shutdown (both main and admin listen on the same watch channel)
		if let Some(tx) = self.shutdown_tx.take() {
			let _ = tx.send(true);
		}
		// Wait for admin server to stop
		if let Some(rx) = self.admin_shutdown_complete_rx.take() {
			let _ = self.runtime.block_on(rx);
		}
		// Wait for main server to stop
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
