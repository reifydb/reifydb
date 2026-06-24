// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	io,
	net::SocketAddr,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use axum::{serve, serve::Listener};
use reifydb_core::{
	error::CoreError,
	interface::version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_runtime::{
	shutdown::Shutdown,
	sync::{mutex::Mutex, rwlock::RwLock},
};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_sub_server::{
	accept::{PermittedStream, accept_admitted},
	state::AppState,
};
use reifydb_value::Result;
use tokio::{
	net::TcpListener,
	runtime::Handle,
	sync::{Semaphore, oneshot},
};
use tracing::{error, info};

use crate::{routes::router, state::HttpServerState};

pub struct HttpSubsystem {
	bind_addr: Option<String>,

	admin_bind_addr: Option<String>,

	actual_addr: RwLock<Option<SocketAddr>>,

	admin_actual_addr: RwLock<Option<SocketAddr>>,

	state: AppState,

	running: Arc<AtomicBool>,

	shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,

	shutdown_complete_rx: Mutex<Option<oneshot::Receiver<()>>>,

	admin_shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,

	admin_shutdown_complete_rx: Mutex<Option<oneshot::Receiver<()>>>,

	connection_semaphore: Arc<Semaphore>,

	handle: Handle,
}

type ShutdownHandles = (oneshot::Sender<()>, oneshot::Receiver<()>);

impl HttpSubsystem {
	pub fn new(
		bind_addr: Option<String>,
		admin_bind_addr: Option<String>,
		state: AppState,
		handle: Handle,
	) -> Result<Self> {
		let connection_semaphore = Arc::new(Semaphore::new(state.max_connections()));
		let subsystem = Self {
			bind_addr,
			admin_bind_addr,
			actual_addr: RwLock::new(None),
			admin_actual_addr: RwLock::new(None),
			state,
			running: Arc::new(AtomicBool::new(false)),
			shutdown_tx: Mutex::new(None),
			shutdown_complete_rx: Mutex::new(None),
			admin_shutdown_tx: Mutex::new(None),
			admin_shutdown_complete_rx: Mutex::new(None),
			connection_semaphore,
			handle,
		};
		subsystem.spawn_main_server()?;
		subsystem.spawn_admin_server()?;
		Ok(subsystem)
	}

	pub fn bind_addr(&self) -> Option<&str> {
		self.bind_addr.as_deref()
	}

	pub fn local_addr(&self) -> Option<SocketAddr> {
		*self.actual_addr.read()
	}

	pub fn port(&self) -> Option<u16> {
		self.local_addr().map(|a| a.port())
	}

	pub fn admin_local_addr(&self) -> Option<SocketAddr> {
		*self.admin_actual_addr.read()
	}

	pub fn admin_port(&self) -> Option<u16> {
		self.admin_local_addr().map(|a| a.port())
	}

	fn spawn_main_server(&self) -> Result<()> {
		let Some(listener) = self.bind_main_listener()? else {
			return Ok(());
		};
		let (shutdown_tx, complete_rx) = self.spawn_main_serve_task(listener);
		self.store_main_shutdown_handles(shutdown_tx, complete_rx);
		Ok(())
	}

	#[inline]
	fn bind_main_listener(&self) -> Result<Option<TcpListener>> {
		let Some(addr) = self.bind_addr.clone() else {
			self.running.store(true, Ordering::SeqCst);
			return Ok(None);
		};
		let listener = self.bind_listener(&addr)?;
		let actual_addr = local_addr_or_err(&listener)?;
		*self.actual_addr.write() = Some(actual_addr);
		info!("HTTP server bound to {}", actual_addr);
		Ok(Some(listener))
	}

	#[inline]
	fn spawn_main_serve_task(&self, listener: TcpListener) -> ShutdownHandles {
		let (shutdown_tx, shutdown_rx) = oneshot::channel();
		let (complete_tx, complete_rx) = oneshot::channel();
		let server_state = HttpServerState::new(self.state.clone());
		let semaphore = self.connection_semaphore.clone();
		let running = self.running.clone();
		self.handle.spawn(async move {
			running.store(true, Ordering::SeqCst);
			let result = serve_http(listener, server_state, semaphore, shutdown_rx, "HTTP server").await;
			if let Err(e) = result {
				error!("HTTP server error: {}", e);
			}
			running.store(false, Ordering::SeqCst);
			let _ = complete_tx.send(());
			info!("HTTP server stopped");
		});
		(shutdown_tx, complete_rx)
	}

	#[inline]
	fn store_main_shutdown_handles(&self, shutdown_tx: oneshot::Sender<()>, complete_rx: oneshot::Receiver<()>) {
		*self.shutdown_tx.lock() = Some(shutdown_tx);
		*self.shutdown_complete_rx.lock() = Some(complete_rx);
	}

	fn spawn_admin_server(&self) -> Result<()> {
		let Some(listener) = self.bind_admin_listener()? else {
			return Ok(());
		};
		let (shutdown_tx, complete_rx) = self.spawn_admin_serve_task(listener);
		self.store_admin_shutdown_handles(shutdown_tx, complete_rx);
		Ok(())
	}

	#[inline]
	fn bind_admin_listener(&self) -> Result<Option<TcpListener>> {
		let Some(admin_addr) = self.admin_bind_addr.clone() else {
			return Ok(None);
		};
		let listener = self.bind_listener(&admin_addr)?;
		let actual_addr = local_addr_or_err(&listener)?;
		*self.admin_actual_addr.write() = Some(actual_addr);
		info!("HTTP admin server bound to {}", actual_addr);
		Ok(Some(listener))
	}

	#[inline]
	fn spawn_admin_serve_task(&self, listener: TcpListener) -> ShutdownHandles {
		let (admin_shutdown_tx, admin_shutdown_rx) = oneshot::channel();
		let (admin_complete_tx, admin_complete_rx) = oneshot::channel();
		let admin_config = self.state.config().clone().admin_enabled(true);
		let admin_app_state = self.state.clone_with_config(admin_config);
		let admin_server_state = HttpServerState::new(admin_app_state);
		let semaphore = self.connection_semaphore.clone();
		self.handle.spawn(async move {
			let result = serve_http(
				listener,
				admin_server_state,
				semaphore,
				admin_shutdown_rx,
				"HTTP admin server",
			)
			.await;
			if let Err(e) = result {
				error!("HTTP admin server error: {}", e);
			}
			let _ = admin_complete_tx.send(());
			info!("HTTP admin server stopped");
		});
		(admin_shutdown_tx, admin_complete_rx)
	}

	#[inline]
	fn store_admin_shutdown_handles(&self, shutdown_tx: oneshot::Sender<()>, complete_rx: oneshot::Receiver<()>) {
		*self.admin_shutdown_tx.lock() = Some(shutdown_tx);
		*self.admin_shutdown_complete_rx.lock() = Some(complete_rx);
	}

	#[inline]
	fn stop_admin_server(&self) {
		let admin_tx = self.admin_shutdown_tx.lock().take();
		if let Some(tx) = admin_tx {
			let _ = tx.send(());
		}
		let admin_rx = self.admin_shutdown_complete_rx.lock().take();
		if let Some(rx) = admin_rx {
			let _ = self.handle.block_on(rx);
		}
	}

	#[inline]
	fn stop_main_server(&self) {
		let main_tx = self.shutdown_tx.lock().take();
		if let Some(tx) = main_tx {
			let _ = tx.send(());
		}
		let main_rx = self.shutdown_complete_rx.lock().take();
		if let Some(rx) = main_rx {
			let _ = self.handle.block_on(rx);
		}
	}

	#[inline]
	fn bind_listener(&self, addr: &str) -> Result<TcpListener> {
		self.handle.block_on(TcpListener::bind(addr)).map_err(|e| {
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

impl Shutdown for HttpSubsystem {
	fn shutdown(&self) {
		self.stop_admin_server();
		self.stop_main_server();
		self.running.store(false, Ordering::SeqCst);
	}
}

impl Subsystem for HttpSubsystem {
	fn name(&self) -> &'static str {
		"Http"
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::SeqCst)
	}

	fn health_status(&self) -> HealthStatus {
		if self.running.load(Ordering::SeqCst) {
			HealthStatus::Healthy
		} else if self.shutdown_tx.lock().is_some() {
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
	semaphore: Arc<Semaphore>,
	shutdown_rx: oneshot::Receiver<()>,
	name: &'static str,
) -> io::Result<()> {
	let app = router(server_state);
	let listener = LimitedListener::new(listener, semaphore, name);
	serve(listener, app)
		.with_graceful_shutdown(async move {
			shutdown_rx.await.ok();
			info!("{} received shutdown signal", name);
		})
		.await
}

pub struct LimitedListener {
	listener: TcpListener,
	semaphore: Arc<Semaphore>,
	name: &'static str,
}

impl LimitedListener {
	pub fn new(listener: TcpListener, semaphore: Arc<Semaphore>, name: &'static str) -> Self {
		Self {
			listener,
			semaphore,
			name,
		}
	}
}

impl Listener for LimitedListener {
	type Io = PermittedStream;
	type Addr = SocketAddr;

	async fn accept(&mut self) -> (Self::Io, Self::Addr) {
		let (stream, permit, peer) = accept_admitted(&self.listener, &self.semaphore, self.name).await;
		(PermittedStream::new(stream, permit), peer)
	}

	fn local_addr(&self) -> io::Result<Self::Addr> {
		self.listener.local_addr()
	}
}
