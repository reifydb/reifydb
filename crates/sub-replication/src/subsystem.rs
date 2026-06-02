// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	net::SocketAddr,
	result::Result as StdResult,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

use reifydb_cdc::storage::CdcStore;
use reifydb_core::{
	error::CoreError,
	event::{EventBus, metric::CdcWrittenEvent},
	interface::version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	reifydb_assertions,
	shutdown::Shutdown,
	sync::{mutex::Mutex, rwlock::RwLock},
};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_value::{Result, error::Error};
use tokio::{
	net::TcpListener,
	runtime::Handle,
	sync::{Notify, oneshot, watch},
};
use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
use tonic::transport::{Error as TonicError, Server};
use tracing::{error, info, warn};

use crate::{
	builder::{PrimaryConfig, ReplicaConfig, ReplicationConfig},
	generated::reify_db_replication_server::ReifyDbReplicationServer,
	primary::{CdcNotifyListener, service::ReplicationService},
	replica::{applier::ReplicaApplier, client::ReplicationClient, watermark::ReplicaWatermark},
};

type PrimaryInputs = (CdcStore, EventBus, String, u64);
type PrimaryHandles = (oneshot::Sender<()>, oneshot::Receiver<()>, watch::Sender<bool>);
type ReplicaInputs = (StandardEngine, String, Duration, u64);
type ReplicaHandles = (watch::Sender<bool>, oneshot::Receiver<()>);

pub struct ReplicationSubsystem {
	config: ReplicationConfig,
	cdc_store: Option<CdcStore>,
	event_bus: Option<EventBus>,
	engine: Option<StandardEngine>,
	runtime: Handle,
	running: Arc<AtomicBool>,
	actual_addr: RwLock<Option<SocketAddr>>,

	shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
	shutdown_complete_rx: Mutex<Option<oneshot::Receiver<()>>>,
	stream_shutdown_tx: Mutex<Option<watch::Sender<bool>>>,

	replica_shutdown_tx: Mutex<Option<watch::Sender<bool>>>,
	replica_complete_rx: Mutex<Option<oneshot::Receiver<()>>>,
}

impl ReplicationSubsystem {
	pub fn primary(
		config: PrimaryConfig,
		cdc_store: CdcStore,
		event_bus: EventBus,
		runtime: Handle,
	) -> Result<Self> {
		let subsystem = Self {
			config: ReplicationConfig::Primary(config),
			cdc_store: Some(cdc_store),
			event_bus: Some(event_bus),
			engine: None,
			runtime,
			running: Arc::new(AtomicBool::new(false)),
			actual_addr: RwLock::new(None),
			shutdown_tx: Mutex::new(None),
			shutdown_complete_rx: Mutex::new(None),
			stream_shutdown_tx: Mutex::new(None),
			replica_shutdown_tx: Mutex::new(None),
			replica_complete_rx: Mutex::new(None),
		};
		subsystem.start_primary()?;
		Ok(subsystem)
	}

	pub fn replica(config: ReplicaConfig, engine: StandardEngine, runtime: Handle) -> Result<Self> {
		let subsystem = Self {
			config: ReplicationConfig::Replica(config),
			cdc_store: None,
			event_bus: None,
			engine: Some(engine),
			runtime,
			running: Arc::new(AtomicBool::new(false)),
			actual_addr: RwLock::new(None),
			shutdown_tx: Mutex::new(None),
			shutdown_complete_rx: Mutex::new(None),
			stream_shutdown_tx: Mutex::new(None),
			replica_shutdown_tx: Mutex::new(None),
			replica_complete_rx: Mutex::new(None),
		};
		subsystem.start_replica()?;
		Ok(subsystem)
	}

	pub fn local_addr(&self) -> Option<SocketAddr> {
		*self.actual_addr.read()
	}

	pub fn port(&self) -> Option<u16> {
		self.local_addr().map(|a| a.port())
	}

	fn start_primary(&self) -> Result<()> {
		let (cdc_store, event_bus, bind_addr, batch_size) = self.resolve_primary_inputs();

		let notify = register_cdc_notify_listener(&event_bus);
		let listener = self.bind_replication_listener(&bind_addr)?;
		self.record_bound_addr(&listener)?;

		let handles = self.spawn_primary_server(listener, cdc_store, notify, batch_size);
		self.stash_primary_handles(handles);
		Ok(())
	}

	#[inline]
	fn resolve_primary_inputs(&self) -> PrimaryInputs {
		let ReplicationConfig::Primary(config) = &self.config else {
			unreachable!()
		};
		let cdc_store = self.cdc_store.clone().expect("CdcStore required for primary mode");
		let event_bus = self.event_bus.clone().expect("EventBus required for primary mode");
		let bind_addr = config.bind_addr.clone().unwrap_or_else(|| "0.0.0.0:0".to_string());
		(cdc_store, event_bus, bind_addr, config.batch_size)
	}

	#[inline]
	fn spawn_primary_server(
		&self,
		listener: TcpListener,
		cdc_store: CdcStore,
		notify: Arc<Notify>,
		batch_size: u64,
	) -> PrimaryHandles {
		let (shutdown_tx, shutdown_rx) = oneshot::channel();
		let (complete_tx, complete_rx) = oneshot::channel();
		let (stream_shutdown_tx, stream_shutdown_rx) = watch::channel(false);
		let service = ReplicationService::new(cdc_store, notify, stream_shutdown_rx, batch_size);

		let running = self.running.clone();
		self.runtime.spawn(async move {
			running.store(true, Ordering::SeqCst);
			let result = serve_replication(listener, service, shutdown_rx).await;
			if let Err(e) = result {
				error!("Replication server error: {}", e);
			}
			running.store(false, Ordering::SeqCst);
			let _ = complete_tx.send(());
			info!("Replication server stopped");
		});
		(shutdown_tx, complete_rx, stream_shutdown_tx)
	}

	#[inline]
	fn stash_primary_handles(&self, handles: PrimaryHandles) {
		let (shutdown_tx, complete_rx, stream_shutdown_tx) = handles;
		*self.shutdown_tx.lock() = Some(shutdown_tx);
		*self.shutdown_complete_rx.lock() = Some(complete_rx);
		*self.stream_shutdown_tx.lock() = Some(stream_shutdown_tx);
	}

	#[inline]
	fn bind_replication_listener(&self, bind_addr: &str) -> Result<TcpListener> {
		self.runtime.block_on(TcpListener::bind(bind_addr)).map_err(|e| {
			CoreError::SubsystemBindFailed {
				addr: bind_addr.to_string(),
				reason: e.to_string(),
			}
			.into()
		})
	}

	#[inline]
	fn record_bound_addr(&self, listener: &TcpListener) -> Result<()> {
		let actual_addr = listener.local_addr().map_err(|e| -> Error {
			CoreError::SubsystemAddressUnavailable {
				reason: e.to_string(),
			}
			.into()
		})?;
		*self.actual_addr.write() = Some(actual_addr);
		info!("Replication server bound to {}", actual_addr);
		Ok(())
	}

	fn start_replica(&self) -> Result<()> {
		let (engine, primary_addr, reconnect_interval, batch_size) = self.resolve_replica_inputs();

		let client = self.build_replica_client(engine, primary_addr, reconnect_interval, batch_size);

		let handles = self.spawn_replica_client(client);
		self.stash_replica_handles(handles);
		self.running.store(true, Ordering::SeqCst);
		Ok(())
	}

	#[inline]
	fn resolve_replica_inputs(&self) -> ReplicaInputs {
		let ReplicationConfig::Replica(config) = &self.config else {
			unreachable!()
		};
		let engine = self.engine.clone().expect("StandardEngine required for replica mode");
		let primary_addr = config.primary_addr.clone().expect("primary_addr required for replica mode");
		(engine, primary_addr, config.reconnect_interval, config.batch_size)
	}

	#[inline]
	fn build_replica_client(
		&self,
		engine: StandardEngine,
		primary_addr: String,
		reconnect_interval: Duration,
		batch_size: u64,
	) -> ReplicationClient {
		let watermark = ReplicaWatermark::new();
		engine.ioc().register_service(watermark.clone());
		let applier = ReplicaApplier::new(engine.clone(), watermark);
		ReplicationClient::new(primary_addr, applier, reconnect_interval, batch_size)
	}

	#[inline]
	fn spawn_replica_client(&self, client: ReplicationClient) -> ReplicaHandles {
		let (shutdown_tx, shutdown_rx) = watch::channel(false);
		let (complete_tx, complete_rx) = oneshot::channel();
		let running = self.running.clone();

		self.runtime.spawn(async move {
			running.store(true, Ordering::SeqCst);
			client.run(shutdown_rx).await;
			running.store(false, Ordering::SeqCst);
			let _ = complete_tx.send(());
			info!("Replication client stopped");
		});
		(shutdown_tx, complete_rx)
	}

	#[inline]
	fn stash_replica_handles(&self, handles: ReplicaHandles) {
		let (shutdown_tx, complete_rx) = handles;
		*self.replica_shutdown_tx.lock() = Some(shutdown_tx);
		*self.replica_complete_rx.lock() = Some(complete_rx);
	}

	#[inline]
	fn shutdown_primary(&self) {
		reifydb_assertions! {
			let is_primary = matches!(self.config, ReplicationConfig::Primary(_));
			assert!(
				is_primary,
				"shutdown_primary acquires the primary shutdown channels (stream/server) but the config is \
				 the replica variant, so it would take the never-populated primary mutexes and silently skip \
				 stopping the running replica client task (is_primary={is_primary})"
			);
		}
		if let Some(tx) = self.stream_shutdown_tx.lock().take() {
			let _ = tx.send(true);
		}
		if let Some(tx) = self.shutdown_tx.lock().take() {
			let _ = tx.send(());
		}
		let rx = self.shutdown_complete_rx.lock().take();
		if let Some(rx) = rx {
			let _ = self.runtime.block_on(rx);
		}
	}

	#[inline]
	fn shutdown_replica(&self) {
		reifydb_assertions! {
			let is_replica = matches!(self.config, ReplicationConfig::Replica(_));
			assert!(
				is_replica,
				"shutdown_replica acquires the replica shutdown channels but the config is the primary \
				 variant, so it would take the never-populated replica mutexes and leave the running \
				 replication server task alive after shutdown returns (is_replica={is_replica})"
			);
		}
		if let Some(tx) = self.replica_shutdown_tx.lock().take() {
			let _ = tx.send(true);
		}
		let rx = self.replica_complete_rx.lock().take();
		if let Some(rx) = rx {
			let _ = self.runtime.block_on(rx);
		}
	}
}

#[inline]
fn register_cdc_notify_listener(event_bus: &EventBus) -> Arc<Notify> {
	let notify = Arc::new(Notify::new());
	event_bus.register::<CdcWrittenEvent, _>(CdcNotifyListener::new(notify.clone()));
	notify
}

async fn serve_replication(
	listener: TcpListener,
	service: ReplicationService,
	shutdown_rx: oneshot::Receiver<()>,
) -> StdResult<(), TonicError> {
	let incoming = TcpListenerStream::new(listener).map(|result| {
		if let Ok(ref stream) = result
			&& let Err(e) = stream.set_nodelay(true)
		{
			warn!("Failed to set TCP_NODELAY: {e}");
		}
		result
	});
	Server::builder()
		.add_service(ReifyDbReplicationServer::new(service))
		.serve_with_incoming_shutdown(incoming, async {
			shutdown_rx.await.ok();
			info!("Replication server received shutdown signal");
		})
		.await
}

impl HasVersion for ReplicationSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "CDC-based replication subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

impl Shutdown for ReplicationSubsystem {
	fn shutdown(&self) {
		match &self.config {
			ReplicationConfig::Primary(_) => self.shutdown_primary(),
			ReplicationConfig::Replica(_) => self.shutdown_replica(),
		}
		self.running.store(false, Ordering::SeqCst);
	}
}

impl Subsystem for ReplicationSubsystem {
	fn name(&self) -> &'static str {
		"Replication"
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::SeqCst)
	}

	fn health_status(&self) -> HealthStatus {
		if self.running.load(Ordering::SeqCst) {
			HealthStatus::Healthy
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
