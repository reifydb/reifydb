// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	net::SocketAddr,
	result::Result as StdResult,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_core::{
	error::CoreError,
	interface::version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_runtime::{
	shutdown::Shutdown,
	sync::{mutex::Mutex, rwlock::RwLock},
};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_sub_server::state::AppState;
use reifydb_sub_subscription::{poller::StoreBackedPoller, store::SubscriptionStore};
use reifydb_value::Result;
use tokio::{
	net::TcpListener,
	runtime::Handle,
	sync::{oneshot, watch},
};
use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
use tonic::transport::{Error as TonicError, Server};
use tracing::{error, info};

use crate::{
	generated::reify_db_server::ReifyDbServer, server_state::GrpcServerState, service::ReifyDbService,
	subscription::SubscriptionRegistry,
};

pub struct GrpcSubsystem {
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
	handle: Handle,
	poll_batch_size: usize,
	registry: Mutex<Option<Arc<SubscriptionRegistry>>>,
	subscription_shutdown_tx: Mutex<Option<watch::Sender<bool>>>,
	poller_stop_tx: Mutex<Option<watch::Sender<bool>>>,
	subscription_store: Option<Arc<SubscriptionStore>>,
}

impl GrpcSubsystem {
	pub fn new(
		bind_addr: Option<String>,
		admin_bind_addr: Option<String>,
		state: AppState,
		handle: Handle,
		poll_batch_size: usize,
		subscription_store: Option<Arc<SubscriptionStore>>,
	) -> Result<Self> {
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
			handle,
			poll_batch_size,
			registry: Mutex::new(None),
			subscription_shutdown_tx: Mutex::new(None),
			poller_stop_tx: Mutex::new(None),
			subscription_store,
		};

		let registry = subsystem.init_subscription_registry();
		let sub_shutdown_rx = subsystem.create_subscription_shutdown_channel();
		let (poller_stop_tx, poller_stop_rx) = watch::channel(false);
		subsystem.spawn_subscription_poller_if_configured(registry.clone(), poller_stop_rx);
		subsystem.spawn_main_server(registry, sub_shutdown_rx, poller_stop_tx)?;
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

	#[inline]
	fn init_subscription_registry(&self) -> Arc<SubscriptionRegistry> {
		let registry = Arc::new(SubscriptionRegistry::new(self.state.clock().clone()));
		*self.registry.lock() = Some(registry.clone());
		registry
	}

	#[inline]
	fn create_subscription_shutdown_channel(&self) -> watch::Receiver<bool> {
		let (sub_shutdown_tx, sub_shutdown_rx) = watch::channel(false);
		*self.subscription_shutdown_tx.lock() = Some(sub_shutdown_tx);
		sub_shutdown_rx
	}

	#[inline]
	fn spawn_subscription_poller_if_configured(
		&self,
		registry: Arc<SubscriptionRegistry>,
		poller_stop_rx: watch::Receiver<bool>,
	) {
		let Some(ref store) = self.subscription_store else {
			return;
		};
		let poller = Arc::new(StoreBackedPoller::new(store.clone(), self.poll_batch_size));
		self.handle.spawn(async move {
			poller.run_loop(registry, poller_stop_rx).await;
		});
	}

	fn spawn_main_server(
		&self,
		registry: Arc<SubscriptionRegistry>,
		sub_shutdown_rx: watch::Receiver<bool>,
		poller_stop_tx: watch::Sender<bool>,
	) -> Result<()> {
		let Some(addr) = self.bind_addr.clone() else {
			self.running.store(true, Ordering::SeqCst);
			*self.poller_stop_tx.lock() = Some(poller_stop_tx);
			return Ok(());
		};
		let listener = self.bind_listener(&addr)?;
		let actual_addr = local_addr_or_err(&listener)?;
		*self.actual_addr.write() = Some(actual_addr);
		info!("gRPC server bound to {}", actual_addr);

		let (shutdown_tx, shutdown_rx) = oneshot::channel();
		let (complete_tx, complete_rx) = oneshot::channel();
		let state = self.state.clone();
		let running = self.running.clone();
		self.handle.spawn(async move {
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
		*self.shutdown_tx.lock() = Some(shutdown_tx);
		*self.shutdown_complete_rx.lock() = Some(complete_rx);
		Ok(())
	}

	fn spawn_admin_server(&self) -> Result<()> {
		let Some(admin_addr) = self.admin_bind_addr.clone() else {
			return Ok(());
		};
		let listener = self.bind_listener(&admin_addr)?;
		let actual_addr = local_addr_or_err(&listener)?;
		*self.admin_actual_addr.write() = Some(actual_addr);
		info!("gRPC admin server bound to {}", actual_addr);

		let (admin_shutdown_tx, admin_shutdown_rx) = oneshot::channel();
		let (admin_complete_tx, admin_complete_rx) = oneshot::channel();
		let admin_server_state = GrpcServerState::new(self.state.clone());
		let admin_registry = self.registry.lock().as_ref().unwrap().clone();
		let admin_sub_shutdown_rx = self.subscription_shutdown_tx.lock().as_ref().unwrap().subscribe();
		self.handle.spawn(async move {
			let admin_service =
				ReifyDbService::new(admin_server_state, true, admin_registry, admin_sub_shutdown_rx);
			let result = serve_grpc(listener, admin_service, admin_shutdown_rx, "gRPC admin server").await;
			if let Err(e) = result {
				error!("gRPC admin server error: {}", e);
			}
			let _ = admin_complete_tx.send(());
			info!("gRPC admin server stopped");
		});
		*self.admin_shutdown_tx.lock() = Some(admin_shutdown_tx);
		*self.admin_shutdown_complete_rx.lock() = Some(admin_complete_rx);
		Ok(())
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

impl Shutdown for GrpcSubsystem {
	fn shutdown(&self) {
		self.close_registry();
		self.signal_subscription_and_poller_stop();
		self.shutdown_admin_server_and_wait();
		self.shutdown_main_server_and_wait();
		self.running.store(false, Ordering::SeqCst);
	}
}

impl GrpcSubsystem {
	#[inline]
	fn close_registry(&self) {
		if let Some(registry) = self.registry.lock().take() {
			registry.close_all();
		}
	}

	#[inline]
	fn signal_subscription_and_poller_stop(&self) {
		if let Some(tx) = self.subscription_shutdown_tx.lock().take() {
			let _ = tx.send(true);
		}
		if let Some(tx) = self.poller_stop_tx.lock().take() {
			let _ = tx.send(true);
		}
	}

	#[inline]
	fn shutdown_admin_server_and_wait(&self) {
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
	fn shutdown_main_server_and_wait(&self) {
		let main_tx = self.shutdown_tx.lock().take();
		if let Some(tx) = main_tx {
			let _ = tx.send(());
		}
		let main_rx = self.shutdown_complete_rx.lock().take();
		if let Some(rx) = main_rx {
			let _ = self.handle.block_on(rx);
		}
	}
}

impl Subsystem for GrpcSubsystem {
	fn name(&self) -> &'static str {
		"Grpc"
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
