// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	any::Any,
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
use reifydb_type::{Result, error::Error};
use tokio::{net::TcpListener, sync::oneshot};
use tracing::{error, info, warn};

use crate::{routes::router, state::AdminState};

pub struct AdminSubsystem {
	bind_addr: String,

	actual_addr: RwLock<Option<SocketAddr>>,

	state: AdminState,

	running: Arc<AtomicBool>,

	shutdown_tx: Option<oneshot::Sender<()>>,

	shutdown_complete_rx: Option<oneshot::Receiver<()>>,

	runtime: SharedRuntime,
}

impl AdminSubsystem {
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

	pub fn bind_addr(&self) -> &str {
		&self.bind_addr
	}

	pub fn local_addr(&self) -> Option<SocketAddr> {
		*self.actual_addr.read().unwrap()
	}

	pub fn port(&self) -> Option<u16> {
		self.local_addr().map(|a| a.port())
	}
}

impl HasVersion for AdminSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
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

	fn start(&mut self) -> Result<()> {
		if self.running.load(Ordering::SeqCst) {
			return Ok(());
		}

		let addr = self.bind_addr.clone();
		let runtime = self.runtime.clone();
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
		info!("Admin server bound to {}", actual_addr);

		let (shutdown_tx, shutdown_rx) = oneshot::channel();
		let (complete_tx, complete_rx) = oneshot::channel();

		let state = self.state.clone();
		let running = self.running.clone();
		let runtime = self.runtime.clone();

		runtime.spawn(async move {
			running.store(true, Ordering::SeqCst);

			let app = router(state);
			let listener = listener.tap_io(|tcp_stream| {
				if let Err(e) = tcp_stream.set_nodelay(true) {
					warn!("Failed to set TCP_NODELAY: {e}");
				}
			});
			let server = serve(listener, app).with_graceful_shutdown(async {
				shutdown_rx.await.ok();
				info!("Admin server received shutdown signal");
			});

			if let Err(e) = server.await {
				error!("Admin server error: {}", e);
			}

			running.store(false, Ordering::SeqCst);
			let _ = complete_tx.send(());
			info!("Admin server stopped");
		});

		self.shutdown_tx = Some(shutdown_tx);
		self.shutdown_complete_rx = Some(complete_rx);
		Ok(())
	}

	fn shutdown(&mut self) -> Result<()> {
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
