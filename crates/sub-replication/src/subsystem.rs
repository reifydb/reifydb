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

use reifydb_cdc::storage::CdcStore;
use reifydb_core::{
	error::CoreError,
	event::{EventBus, metric::CdcWrittenEvent},
	interface::version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::SharedRuntime;
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_type::{Result, error::Error};
use tokio::{
	net::TcpListener,
	sync::{Notify, oneshot, watch},
};
use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
use tonic::transport::Server;
use tracing::{error, info, warn};

use crate::{
	builder::{PrimaryConfig, ReplicaConfig, ReplicationConfig},
	generated::reify_db_replication_server::ReifyDbReplicationServer,
	primary::{CdcNotifyListener, service::ReplicationService},
	replica::{applier::ReplicaApplier, client::ReplicationClient},
};

pub struct ReplicationSubsystem {
	config: ReplicationConfig,
	cdc_store: Option<CdcStore>,
	event_bus: Option<EventBus>,
	engine: Option<StandardEngine>,
	runtime: SharedRuntime,
	running: Arc<AtomicBool>,
	actual_addr: RwLock<Option<SocketAddr>>,
	// Primary mode
	shutdown_tx: Option<oneshot::Sender<()>>,
	shutdown_complete_rx: Option<oneshot::Receiver<()>>,
	stream_shutdown_tx: Option<watch::Sender<bool>>,
	// Replica mode
	replica_shutdown_tx: Option<watch::Sender<bool>>,
	replica_complete_rx: Option<oneshot::Receiver<()>>,
}

impl ReplicationSubsystem {
	pub fn primary(
		config: PrimaryConfig,
		cdc_store: CdcStore,
		event_bus: EventBus,
		runtime: SharedRuntime,
	) -> Self {
		Self {
			config: ReplicationConfig::Primary(config),
			cdc_store: Some(cdc_store),
			event_bus: Some(event_bus),
			engine: None,
			runtime,
			running: Arc::new(AtomicBool::new(false)),
			actual_addr: RwLock::new(None),
			shutdown_tx: None,
			shutdown_complete_rx: None,
			stream_shutdown_tx: None,
			replica_shutdown_tx: None,
			replica_complete_rx: None,
		}
	}

	pub fn replica(config: ReplicaConfig, engine: StandardEngine, runtime: SharedRuntime) -> Self {
		Self {
			config: ReplicationConfig::Replica(config),
			cdc_store: None,
			event_bus: None,
			engine: Some(engine),
			runtime,
			running: Arc::new(AtomicBool::new(false)),
			actual_addr: RwLock::new(None),
			shutdown_tx: None,
			shutdown_complete_rx: None,
			stream_shutdown_tx: None,
			replica_shutdown_tx: None,
			replica_complete_rx: None,
		}
	}

	pub fn local_addr(&self) -> Option<SocketAddr> {
		*self.actual_addr.read().unwrap()
	}

	pub fn port(&self) -> Option<u16> {
		self.local_addr().map(|a| a.port())
	}

	fn start_primary(&mut self) -> Result<()> {
		let ReplicationConfig::Primary(config) = &self.config else {
			unreachable!()
		};

		let cdc_store = self.cdc_store.clone().expect("CdcStore required for primary mode");
		let event_bus = self.event_bus.clone().expect("EventBus required for primary mode");
		let bind_addr = config.bind_addr.clone().unwrap_or_else(|| "0.0.0.0:0".to_string());
		let batch_size = config.batch_size;

		// Create notify and register EventBus listener for push-based replication
		let notify = Arc::new(Notify::new());
		event_bus.register::<CdcWrittenEvent, _>(CdcNotifyListener::new(notify.clone()));

		let listener = self.runtime.block_on(TcpListener::bind(&bind_addr)).map_err(|e| {
			let err: Error = CoreError::SubsystemBindFailed {
				addr: bind_addr.clone(),
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
		info!("Replication server bound to {}", actual_addr);

		let (shutdown_tx, shutdown_rx) = oneshot::channel();
		let (complete_tx, complete_rx) = oneshot::channel();
		let running = self.running.clone();

		let (stream_shutdown_tx, stream_shutdown_rx) = watch::channel(false);
		let service = ReplicationService::new(cdc_store, notify, stream_shutdown_rx, batch_size);

		self.runtime.spawn(async move {
			running.store(true, Ordering::SeqCst);

			let incoming = TcpListenerStream::new(listener).map(|result| {
				if let Ok(ref stream) = result
					&& let Err(e) = stream.set_nodelay(true)
				{
					warn!("Failed to set TCP_NODELAY: {e}");
				}
				result
			});

			let result = Server::builder()
				.add_service(ReifyDbReplicationServer::new(service))
				.serve_with_incoming_shutdown(incoming, async {
					shutdown_rx.await.ok();
					info!("Replication server received shutdown signal");
				})
				.await;

			if let Err(e) = result {
				error!("Replication server error: {}", e);
			}

			running.store(false, Ordering::SeqCst);
			let _ = complete_tx.send(());
			info!("Replication server stopped");
		});

		self.shutdown_tx = Some(shutdown_tx);
		self.shutdown_complete_rx = Some(complete_rx);
		self.stream_shutdown_tx = Some(stream_shutdown_tx);
		Ok(())
	}

	fn start_replica(&mut self) -> Result<()> {
		let ReplicationConfig::Replica(config) = &self.config else {
			unreachable!()
		};

		let engine = self.engine.clone().expect("StandardEngine required for replica mode");
		let primary_addr = config.primary_addr.clone().expect("primary_addr required for replica mode");
		let reconnect_interval = config.reconnect_interval;
		let batch_size = config.batch_size;

		let applier = ReplicaApplier::new(engine.clone());
		let client = ReplicationClient::new(primary_addr, applier, reconnect_interval, batch_size);

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

		self.replica_shutdown_tx = Some(shutdown_tx);
		self.replica_complete_rx = Some(complete_rx);
		self.running.store(true, Ordering::SeqCst);
		Ok(())
	}
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

impl Subsystem for ReplicationSubsystem {
	fn name(&self) -> &'static str {
		"Replication"
	}

	fn start(&mut self) -> Result<()> {
		if self.running.load(Ordering::SeqCst) {
			return Ok(());
		}

		match &self.config {
			ReplicationConfig::Primary(_) => self.start_primary(),
			ReplicationConfig::Replica(_) => self.start_replica(),
		}
	}

	fn shutdown(&mut self) -> Result<()> {
		match &self.config {
			ReplicationConfig::Primary(_) => {
				// Signal streaming tasks to exit first, so they drop their
				// channel senders and allow the gRPC server's graceful
				// shutdown to complete without waiting on open streams.
				if let Some(tx) = self.stream_shutdown_tx.take() {
					let _ = tx.send(true);
				}
				if let Some(tx) = self.shutdown_tx.take() {
					let _ = tx.send(());
				}
				if let Some(rx) = self.shutdown_complete_rx.take() {
					let _ = self.runtime.block_on(rx);
				}
			}
			ReplicationConfig::Replica(_) => {
				if let Some(tx) = self.replica_shutdown_tx.take() {
					let _ = tx.send(true);
				}
				if let Some(rx) = self.replica_complete_rx.take() {
					let _ = self.runtime.block_on(rx);
				}
			}
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
