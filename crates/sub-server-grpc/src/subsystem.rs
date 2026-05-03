// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	any::Any,
	net::SocketAddr,
	result::Result as StdResult,
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
use reifydb_sub_subscription::{poller::StoreBackedPoller, store::SubscriptionStore};
use reifydb_type::Result;
use tokio::{
	net::TcpListener,
	sync::{oneshot, watch},
};
use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
use tonic::transport::{Error as TonicError, Server};
use tracing::{error, info};

use crate::{
	generated::reify_db_server::ReifyDbServer, server_state::GrpcServerState, service::ReifyDbService,
	subscription::GrpcSubscriptionRegistry,
};

pub struct GrpcSubsystem {
	bind_addr: Option<String>,
	admin_bind_addr: Option<String>,
	actual_addr: RwLock<Option<SocketAddr>>,
	admin_actual_addr: RwLock<Option<SocketAddr>>,
	state: AppState,
	running: Arc<AtomicBool>,
	shutdown_tx: Option<oneshot::Sender<()>>,
	shutdown_complete_rx: Option<oneshot::Receiver<()>>,
	admin_shutdown_tx: Option<oneshot::Sender<()>>,
	admin_shutdown_complete_rx: Option<oneshot::Receiver<()>>,
	runtime: SharedRuntime,
	poll_interval: Duration,
	poll_batch_size: usize,
	registry: Option<Arc<GrpcSubscriptionRegistry>>,
	subscription_shutdown_tx: Option<watch::Sender<bool>>,
	poller_stop_tx: Option<watch::Sender<bool>>,
	subscription_store: Option<Arc<SubscriptionStore>>,
}

impl GrpcSubsystem {
	pub fn new(
		bind_addr: Option<String>,
		admin_bind_addr: Option<String>,
		state: AppState,
		runtime: SharedRuntime,
		poll_interval: Duration,
		poll_batch_size: usize,
		subscription_store: Option<Arc<SubscriptionStore>>,
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
			poll_interval,
			poll_batch_size,
			registry: None,
			subscription_shutdown_tx: None,
			poller_stop_tx: None,
			subscription_store,
		}
	}

	pub fn bind_addr(&self) -> Option<&str> {
		self.bind_addr.as_deref()
	}

	pub fn local_addr(&self) -> Option<SocketAddr> {
		*self.actual_addr.read().unwrap()
	}

	pub fn port(&self) -> Option<u16> {
		self.local_addr().map(|a| a.port())
	}

	pub fn admin_local_addr(&self) -> Option<SocketAddr> {
		*self.admin_actual_addr.read().unwrap()
	}

	pub fn admin_port(&self) -> Option<u16> {
		self.admin_local_addr().map(|a| a.port())
	}

	#[inline]
	fn init_subscription_registry(&mut self) -> Arc<GrpcSubscriptionRegistry> {
		let registry = Arc::new(GrpcSubscriptionRegistry::new());
		self.registry = Some(registry.clone());
		registry
	}

	#[inline]
	fn create_subscription_shutdown_channel(&mut self) -> watch::Receiver<bool> {
		let (sub_shutdown_tx, sub_shutdown_rx) = watch::channel(false);
		self.subscription_shutdown_tx = Some(sub_shutdown_tx);
		sub_shutdown_rx
	}

	#[inline]
	fn spawn_subscription_poller_if_configured(
		&self,
		registry: Arc<GrpcSubscriptionRegistry>,
		poller_stop_rx: watch::Receiver<bool>,
	) {
		let Some(ref store) = self.subscription_store else {
			return;
		};
		let poller = Arc::new(StoreBackedPoller::new(store.clone(), self.poll_batch_size));
		let poll_interval = self.poll_interval;
		self.runtime.spawn(async move {
			poller.run_loop(registry, poll_interval, poller_stop_rx).await;
		});
	}

	fn spawn_main_server(
		&mut self,
		registry: Arc<GrpcSubscriptionRegistry>,
		sub_shutdown_rx: watch::Receiver<bool>,
		poller_stop_tx: watch::Sender<bool>,
	) -> Result<()> {
		let Some(addr) = self.bind_addr.clone() else {
			self.running.store(true, Ordering::SeqCst);
			self.poller_stop_tx = Some(poller_stop_tx);
			return Ok(());
		};
		let listener = self.bind_listener(&addr)?;
		let actual_addr = local_addr_or_err(&listener)?;
		*self.actual_addr.write().unwrap() = Some(actual_addr);
		info!("gRPC server bound to {}", actual_addr);

		let (shutdown_tx, shutdown_rx) = oneshot::channel();
		let (complete_tx, complete_rx) = oneshot::channel();
		let state = self.state.clone();
		let running = self.running.clone();
		self.runtime.spawn(async move {
			running.store(true, Ordering::SeqCst);
			let server_state = GrpcServerState::new(state);
			let service = ReifyDbService::new(server_state, false, registry, sub_shutdown_rx);
			let result = serve_grpc(listener, service, shutdown_rx, "gRPC server").await;
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

	fn spawn_admin_server(&mut self) -> Result<()> {
		let Some(admin_addr) = self.admin_bind_addr.clone() else {
			return Ok(());
		};
		let listener = self.bind_listener(&admin_addr)?;
		let actual_addr = local_addr_or_err(&listener)?;
		*self.admin_actual_addr.write().unwrap() = Some(actual_addr);
		info!("gRPC admin server bound to {}", actual_addr);

		let (admin_shutdown_tx, admin_shutdown_rx) = oneshot::channel();
		let (admin_complete_tx, admin_complete_rx) = oneshot::channel();
		let admin_server_state = GrpcServerState::new(self.state.clone());
		let admin_registry = self.registry.as_ref().unwrap().clone();
		let admin_sub_shutdown_rx = self.subscription_shutdown_tx.as_ref().unwrap().subscribe();
		self.runtime.spawn(async move {
			let admin_service =
				ReifyDbService::new(admin_server_state, true, admin_registry, admin_sub_shutdown_rx);
			let result = serve_grpc(listener, admin_service, admin_shutdown_rx, "gRPC admin server").await;
			if let Err(e) = result {
				error!("gRPC admin server error: {}", e);
			}
			let _ = admin_complete_tx.send(());
			info!("gRPC admin server stopped");
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
		let registry = self.init_subscription_registry();
		let sub_shutdown_rx = self.create_subscription_shutdown_channel();
		let (poller_stop_tx, poller_stop_rx) = watch::channel(false);
		self.spawn_subscription_poller_if_configured(registry.clone(), poller_stop_rx);
		self.spawn_main_server(registry, sub_shutdown_rx, poller_stop_tx)?;
		self.spawn_admin_server()?;
		Ok(())
	}

	fn shutdown(&mut self) -> Result<()> {
		if let Some(registry) = self.registry.take() {
			registry.close_all();
		}

		if let Some(tx) = self.subscription_shutdown_tx.take() {
			let _ = tx.send(true);
		}

		if let Some(tx) = self.poller_stop_tx.take() {
			let _ = tx.send(true);
		}

		if let Some(tx) = self.admin_shutdown_tx.take() {
			let _ = tx.send(());
		}
		if let Some(rx) = self.admin_shutdown_complete_rx.take() {
			let _ = self.runtime.block_on(rx);
		}

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

async fn serve_grpc(
	listener: TcpListener,
	service: ReifyDbService,
	shutdown_rx: oneshot::Receiver<()>,
	name: &'static str,
) -> StdResult<(), TonicError> {
	let incoming = TcpListenerStream::new(listener).map(|result| {
		result.inspect(|stream| {
			let _ = stream.set_nodelay(true);
		})
	});
	Server::builder()
		.add_service(ReifyDbServer::new(service))
		.serve_with_incoming_shutdown(incoming, async {
			shutdown_rx.await.ok();
			info!("{} received shutdown signal", name);
		})
		.await
}
