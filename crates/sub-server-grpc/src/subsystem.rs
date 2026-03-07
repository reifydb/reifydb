// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	any::Any,
	net::SocketAddr,
	sync::{
		Arc, RwLock,
		atomic::{AtomicBool, Ordering},
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
use reifydb_subscription::poller::SubscriptionPoller;
use reifydb_type::{Result, error::Error};
use tokio::{
	net::TcpListener,
	sync::{oneshot, watch},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing::{error, info};

use crate::{
	generated::reify_db_server::ReifyDbServer, service::ReifyDbService, subscription::GrpcSubscriptionRegistry,
};

pub struct GrpcSubsystem {
	bind_addr: String,
	actual_addr: RwLock<Option<SocketAddr>>,
	state: AppState,
	running: Arc<AtomicBool>,
	shutdown_tx: Option<oneshot::Sender<()>>,
	shutdown_complete_rx: Option<oneshot::Receiver<()>>,
	runtime: SharedRuntime,
	poll_interval: Duration,
	poll_batch_size: usize,
}

impl GrpcSubsystem {
	pub fn new(
		bind_addr: String,
		state: AppState,
		runtime: SharedRuntime,
		poll_interval: Duration,
		poll_batch_size: usize,
	) -> Self {
		Self {
			bind_addr,
			actual_addr: RwLock::new(None),
			state,
			running: Arc::new(AtomicBool::new(false)),
			shutdown_tx: None,
			shutdown_complete_rx: None,
			runtime,
			poll_interval,
			poll_batch_size,
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

impl HasVersion for GrpcSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "gRPC server subsystem for query and command handling".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

impl Subsystem for GrpcSubsystem {
	fn name(&self) -> &'static str {
		"Grpc"
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
		info!("gRPC server bound to {}", actual_addr);

		let (shutdown_tx, shutdown_rx) = oneshot::channel();
		let (complete_tx, complete_rx) = oneshot::channel();

		let state = self.state.clone();
		let running = self.running.clone();
		let runtime = self.runtime.clone();

		// Create subscription infrastructure
		let registry = Arc::new(GrpcSubscriptionRegistry::new());
		let poller = Arc::new(SubscriptionPoller::new(self.poll_batch_size));

		// Spawn the subscription poller task
		let poller_clone = poller.clone();
		let poller_state = state.clone();
		let poller_registry = registry.clone();
		let poll_interval = self.poll_interval;
		let (poller_stop_tx, poller_stop_rx) = watch::channel(false);
		runtime.spawn(async move {
			poller_clone
				.run_loop(
					poller_state.engine_clone(),
					poller_state.actor_system(),
					poller_registry,
					poll_interval,
					poller_stop_rx,
				)
				.await;
		});

		runtime.spawn(async move {
			running.store(true, Ordering::SeqCst);

			let service = ReifyDbService::new(state, registry, poller);
			let incoming = TcpListenerStream::new(listener);

			let result = Server::builder()
				.add_service(ReifyDbServer::new(service))
				.serve_with_incoming_shutdown(incoming, async {
					shutdown_rx.await.ok();
					info!("gRPC server received shutdown signal");
				})
				.await;

			// Stop the poller
			let _ = poller_stop_tx.send(true);

			if let Err(e) = result {
				error!("gRPC server error: {}", e);
			}

			running.store(false, Ordering::SeqCst);
			let _ = complete_tx.send(());
			info!("gRPC server stopped");
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
