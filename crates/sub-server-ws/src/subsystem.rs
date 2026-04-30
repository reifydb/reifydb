// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	any::Any,
	io,
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
use reifydb_type::Result;
use tokio::{
	net::{TcpListener, TcpStream},
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

	#[inline]
	fn spawn_subscription_poller_if_configured(&self, shutdown_rx: watch::Receiver<bool>) {
		let Some(ref store) = self.subscription_store else {
			return;
		};
		let poller = Arc::new(StoreBackedPoller::new(store.clone(), self.poll_batch_size));
		let registry = self.registry.clone();
		let poll_interval = self.poll_interval;
		self.runtime.spawn(async move {
			poller.run_loop(registry, poll_interval, shutdown_rx).await;
		});
	}

	fn spawn_main_server(&mut self, shutdown_rx: watch::Receiver<bool>) -> Result<()> {
		let Some(addr) = self.bind_addr.clone() else {
			self.running.store(true, Ordering::SeqCst);
			return Ok(());
		};
		let listener = self.bind_listener(&addr)?;
		let actual_addr = local_addr_or_err(&listener)?;
		*self.actual_addr.write().unwrap() = Some(actual_addr);
		info!("WebSocket server bound to {}", actual_addr);

		let (complete_tx, complete_rx) = oneshot::channel();
		let running = self.running.clone();
		let state = self.state.clone();
		let registry = self.registry.clone();
		let semaphore = self.connection_semaphore.clone();
		let active_connections = self.active_connections.clone();
		let runtime_inner = self.runtime.clone();
		self.runtime.spawn(async move {
			running.store(true, Ordering::SeqCst);
			run_accept_loop(
				listener,
				state,
				registry,
				semaphore,
				active_connections,
				shutdown_rx,
				runtime_inner,
				"WebSocket server",
			)
			.await;
			running.store(false, Ordering::SeqCst);
			let _ = complete_tx.send(());
			info!("WebSocket server stopped");
		});
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
		info!("WebSocket admin server bound to {}", actual_addr);

		let (admin_complete_tx, admin_complete_rx) = oneshot::channel();
		let admin_config = self.state.config().clone().admin_enabled(true);
		let admin_state = self.state.clone_with_config(admin_config);
		let admin_registry = self.registry.clone();
		let admin_semaphore = self.connection_semaphore.clone();
		let admin_active = self.active_connections.clone();
		let admin_shutdown_rx = self.shutdown_tx.as_ref().unwrap().subscribe();
		let runtime_inner = self.runtime.clone();
		self.runtime.spawn(async move {
			run_accept_loop(
				listener,
				admin_state,
				admin_registry,
				admin_semaphore,
				admin_active,
				admin_shutdown_rx,
				runtime_inner,
				"WebSocket admin server",
			)
			.await;
			let _ = admin_complete_tx.send(());
			info!("WebSocket admin server stopped");
		});
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
		if self.running.load(Ordering::SeqCst) {
			return Ok(());
		}
		let (shutdown_tx, shutdown_rx) = watch::channel(false);
		self.spawn_subscription_poller_if_configured(shutdown_rx.clone());
		self.spawn_main_server(shutdown_rx)?;
		self.shutdown_tx = Some(shutdown_tx);
		self.spawn_admin_server()?;
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

#[inline]
fn local_addr_or_err(listener: &TcpListener) -> Result<SocketAddr> {
	listener.local_addr().map_err(|e| {
		CoreError::SubsystemAddressUnavailable {
			reason: e.to_string(),
		}
		.into()
	})
}

#[allow(clippy::too_many_arguments)]
async fn run_accept_loop(
	listener: TcpListener,
	state: AppState,
	registry: Arc<SubscriptionRegistry>,
	semaphore: Arc<Semaphore>,
	active_connections: Arc<AtomicUsize>,
	mut shutdown_rx: watch::Receiver<bool>,
	runtime: SharedRuntime,
	name: &'static str,
) {
	loop {
		select! {
			biased;
			result = shutdown_rx.changed() => {
				if result.is_err() || *shutdown_rx.borrow() {
					info!("{} shutting down", name);
					break;
				}
			}
			accept = listener.accept() => {
				handle_accept_result(
					accept,
					&state,
					&registry,
					&semaphore,
					&active_connections,
					&shutdown_rx,
					&runtime,
				);
			}
		}
	}
}

#[allow(clippy::too_many_arguments)]
fn handle_accept_result(
	accept: io::Result<(TcpStream, SocketAddr)>,
	state: &AppState,
	registry: &Arc<SubscriptionRegistry>,
	semaphore: &Arc<Semaphore>,
	active_connections: &Arc<AtomicUsize>,
	shutdown_rx: &watch::Receiver<bool>,
	runtime: &SharedRuntime,
) {
	let (stream, peer) = match accept {
		Ok(pair) => pair,
		Err(e) => {
			warn!("Accept error: {}", e);
			return;
		}
	};
	let Ok(permit) = semaphore.clone().try_acquire_owned() else {
		warn!("Connection limit reached, rejecting {}", peer);
		return;
	};
	let conn_state = state.clone();
	let conn_registry = registry.clone();
	let conn_shutdown_rx = shutdown_rx.clone();
	let active = active_connections.clone();
	active.fetch_add(1, Ordering::SeqCst);
	debug!("Accepted connection from {}", peer);
	runtime.spawn(async move {
		handle_connection(stream, conn_state, conn_registry, conn_shutdown_rx).await;
		active.fetch_sub(1, Ordering::SeqCst);
		drop(permit);
	});
}
