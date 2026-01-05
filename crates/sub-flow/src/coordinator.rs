// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Coordinator that monitors CDC for flow creation and spawns flow consumers.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use reifydb_cdc::{CdcConsume, CdcConsumer, PollConsumer, PollConsumerConfig};
use reifydb_core::{
	CommitVersion, Result,
	interface::{Cdc, CdcChange, CdcConsumerId},
	key::{Key, KeyKind},
};
use reifydb_engine::{StandardCommandTransaction, StandardEngine};
use reifydb_rql::flow::load_flow;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::{
	FlowEngine, flow::FlowConsumer, provider::FlowChangeProvider, registry::FlowConsumerRegistry,
	tracker::PrimitiveVersionTracker,
};

/// Message broadcast to flow consumers when new versions are available.
#[derive(Clone, Debug)]
pub struct VersionBroadcast {
	/// The new version that is now safe to process.
	pub version: CommitVersion,
}

/// Coordinator that monitors CDC for flow creation and manages flow consumers.
pub struct Coordinator {
	engine: StandardEngine,
	flow_engine: Arc<FlowEngine>,
	registry: Arc<FlowConsumerRegistry>,
	primitive_tracker: Arc<PrimitiveVersionTracker>,
	consumer: Option<PollConsumer<CoordinatorConsumer>>,
	shutdown: CancellationToken,
	version_tx: broadcast::Sender<VersionBroadcast>,
	provider: Arc<FlowChangeProvider>,
}

/// Implementation of CDC consume logic for the coordinator.
struct CoordinatorConsumer {
	engine: StandardEngine,
	flow_engine: Arc<FlowEngine>,
	registry: Arc<FlowConsumerRegistry>,
	primitive_tracker: Arc<PrimitiveVersionTracker>,
	shutdown: CancellationToken,
	/// Broadcast channel for notifying consumers of new versions.
	version_tx: broadcast::Sender<VersionBroadcast>,
	/// Shared provider for decoded changes.
	provider: Arc<FlowChangeProvider>,
}

impl Coordinator {
	/// Create a new coordinator.
	pub fn new(
		engine: StandardEngine,
		flow_engine: Arc<FlowEngine>,
		registry: Arc<FlowConsumerRegistry>,
		primitive_tracker: Arc<PrimitiveVersionTracker>,
	) -> Self {
		let shutdown = CancellationToken::new();
		let (version_tx, _) = broadcast::channel(1024);
		let provider = Arc::new(FlowChangeProvider::new(engine.clone()));

		Self {
			engine,
			flow_engine,
			registry,
			primitive_tracker,
			consumer: None,
			shutdown,
			version_tx,
			provider,
		}
	}

	/// Start the coordinator.
	pub fn start(&mut self) -> Result<()> {
		debug!("starting flow coordinator");

		// Create consume implementation
		let consume_impl = CoordinatorConsumer {
			engine: self.engine.clone(),
			flow_engine: self.flow_engine.clone(),
			registry: self.registry.clone(),
			primitive_tracker: self.primitive_tracker.clone(),
			shutdown: self.shutdown.clone(),
			version_tx: self.version_tx.clone(),
			provider: Arc::clone(&self.provider),
		};

		// Configure consumer
		let consumer_id = CdcConsumerId::new("flow-coordinator");
		let config = PollConsumerConfig::new(consumer_id, Duration::from_millis(1), None);

		// Create and start poll consumer
		let mut consumer = PollConsumer::new(config, self.engine.clone(), consume_impl);
		consumer.start()?;

		self.consumer = Some(consumer);

		debug!("flow coordinator started");
		Ok(())
	}

	/// Shutdown the coordinator gracefully.
	pub async fn shutdown(&mut self, timeout: Duration) {
		debug!("shutting down flow coordinator");

		// Signal shutdown to all consumers
		self.shutdown.cancel();

		// Stop coordinator consumer
		if let Some(mut consumer) = self.consumer.take() {
			if let Err(e) = consumer.stop() {
				error!(error = %e, "failed to stop coordinator consumer");
			}
		}

		// Shutdown all flow consumers with timeout
		self.registry.shutdown_all(timeout).await;

		debug!("flow coordinator shutdown complete");
	}
}

#[async_trait]
impl CdcConsume for CoordinatorConsumer {
	async fn consume(&self, txn: &mut StandardCommandTransaction, cdcs: Vec<Cdc>) -> Result<()> {
		for cdc in cdcs {
			let version = cdc.version;

			// Track primitive versions from all CDC changes
			for change in &cdc.changes {
				if let Some(Key::Row(row_key)) = Key::decode(change.key()) {
					self.primitive_tracker.update(row_key.primitive, version).await;
				}
			}

			// Process flow creation events
			for change in &cdc.changes {
				// Check key kind first (fast path)
				if let Some(kind) = Key::kind(change.key()) {
					if kind == KeyKind::Flow {
						// Only process inserts (flow creation)
						if let CdcChange::Insert {
							key,
							..
						} = &change.change
						{
							// Decode to get FlowId
							if let Some(Key::Flow(flow_key)) = Key::decode(key) {
								let flow_id = flow_key.flow;

								// Check if not already spawned
								if !self.registry.contains(flow_id).await {
									debug!(
										flow_id = flow_id.0,
										"detected new flow"
									);

									// Load flow from catalog
									match load_flow(txn, flow_id).await {
										Ok(flow) => {
											// Register with flow engine (no
											// backfill)
											if let Err(e) = self
												.flow_engine
												.register(
													txn,
													flow.clone(),
												)
												.await
											{
												error!(
													flow_id = flow_id.0,
													error = %e,
													"failed to register flow with engine"
												);
												continue;
											}

											// Spawn flow consumer
											match FlowConsumer::spawn(
												flow_id,
												flow,
												self.engine.clone(),
												self.flow_engine
													.clone(),
												Arc::clone(
													&self.provider,
												),
												self.version_tx
													.subscribe(),
												self.shutdown
													.child_token(),
											) {
												Ok(consumer) => {
													self.registry.register(flow_id, consumer).await;
													info!(flow_id = flow_id.0, "spawned flow consumer");
												}
												Err(e) => {
													error!(
														flow_id = flow_id.0,
														error = %e,
														"failed to spawn flow consumer"
													);
												}
											}
										}
										Err(e) => {
											error!(
												flow_id = flow_id.0,
												error = %e,
												"failed to load flow"
											);
										}
									}
								}
							}
						}
					}
				}
			}

			// Broadcast the new version to all flow consumers
			let _ = self.version_tx.send(VersionBroadcast {
				version,
			});
		}

		Ok(())
	}
}
