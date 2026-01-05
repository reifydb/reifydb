// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Per-flow consumer that processes source changes via the FlowChangeProvider.
//!
//! Flow consumers listen for version broadcasts from the Coordinator and fetch
//! decoded changes from the shared FlowChangeProvider, filtering for their sources.

use std::{collections::HashSet, sync::Arc};

use broadcast::error::{RecvError, RecvError::Lagged};
use reifydb_cdc::CdcCheckpoint;
use reifydb_core::{
	CommitVersion, Result,
	interface::{CdcConsumerId, FlowId, PrimitiveId},
};
use reifydb_engine::StandardEngine;
use reifydb_rql::flow::{Flow, FlowNodeType};
use reifydb_sdk::FlowChangeOrigin;
use tokio::{select, sync::broadcast, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{FlowEngine, FlowTransaction, coordinator::VersionBroadcast, provider::FlowChangeProvider};

/// Per-flow consumer that processes CDC events for its sources.
///
/// Listens for version broadcasts from the Coordinator and fetches decoded
/// changes from the shared FlowChangeProvider. Each consumer filters changes
/// to only process those relevant to its source primitives.
pub struct FlowConsumer {
	flow_id: FlowId,
	sources: HashSet<PrimitiveId>,
	shutdown: CancellationToken,
	worker: Option<JoinHandle<()>>,
}

impl FlowConsumer {
	/// Spawn a new flow consumer for the given flow.
	///
	/// The consumer listens for version broadcasts and fetches decoded changes
	/// from the provider, filtering for its specific sources.
	pub fn spawn(
		flow_id: FlowId,
		flow: Flow,
		engine: StandardEngine,
		flow_engine: Arc<FlowEngine>,
		provider: Arc<FlowChangeProvider>,
		version_rx: broadcast::Receiver<VersionBroadcast>,
		shutdown: CancellationToken,
	) -> Result<Self> {
		// Extract source primitives from flow definition
		let sources = extract_sources(&flow);

		debug!(flow_id = flow_id.0, sources = sources.len(), "extracted flow sources");

		// Spawn worker task
		let worker = {
			let sources = sources.clone();
			let shutdown = shutdown.clone();

			tokio::spawn(Self::processing_loop(
				flow_id,
				sources,
				engine,
				flow_engine,
				provider,
				version_rx,
				shutdown,
			))
		};

		info!(flow_id = flow_id.0, "flow consumer started");

		Ok(Self {
			flow_id,
			sources,
			shutdown,
			worker: Some(worker),
		})
	}

	/// Processing loop that listens for version broadcasts.
	async fn processing_loop(
		flow_id: FlowId,
		sources: HashSet<PrimitiveId>,
		engine: StandardEngine,
		flow_engine: Arc<FlowEngine>,
		provider: Arc<FlowChangeProvider>,
		mut version_rx: broadcast::Receiver<VersionBroadcast>,
		shutdown: CancellationToken,
	) {
		// Get initial checkpoint
		let mut current_version = match Self::load_checkpoint(&engine, flow_id).await {
			Ok(v) => v,
			Err(e) => {
				error!(flow_id = flow_id.0, error = %e, "failed to load checkpoint, starting from 0");
				CommitVersion(0)
			}
		};

		debug!(flow_id = flow_id.0, version = current_version.0, "starting from checkpoint");

		let consumer_id = CdcConsumerId::new(&format!("flow-consumer-{}", flow_id.0));

		loop {
			select! {
				_ = shutdown.cancelled() => {
					debug!(flow_id = flow_id.0, "shutdown signal received");
					break;
				}

				result = version_rx.recv() => {
					match result {
						Ok(broadcast) => {
							let start_version = current_version.0 + 1;
							let end_version = broadcast.version.0.min(start_version + 99);

							if start_version > end_version {
								continue; // Nothing to process
							}

							// Single parent transaction for entire batch
							let mut txn = match engine.begin_command().await {
								Ok(t) => t,
								Err(e) => {
									error!(
										flow_id = flow_id.0,
										error = %e,
										"failed to begin transaction"
									);
									continue;
								}
							};
							let catalog = engine.catalog();

							let mut final_version = current_version;
							let mut flow_txn = FlowTransaction::new(&mut txn, CommitVersion(start_version), catalog.clone()).await;

							for version in start_version..=end_version {
								let version = CommitVersion(version);
								flow_txn.update_version(version);

								if let Err(e) = Self::process_version(
									flow_id,
									&sources,
									&mut flow_txn,
									&flow_engine,
									&provider,
									version,
								).await {
									error!(
										flow_id = flow_id.0,
										version = version.0,
										error = %e,
										"failed to process version"
									);
								}

								final_version = version;
							}

							flow_txn.commit(&mut txn).await.unwrap(); // should never happen

							if let Err(e) = CdcCheckpoint::persist(&mut txn, &consumer_id, final_version).await {
								error!(
									flow_id = flow_id.0,
									version = final_version.0,
									error = %e,
									"failed to persist checkpoint"
								);

								txn.rollback().ok();
								return;
							}

							if let Err(e) = txn.commit().await {
								error!(
									flow_id = flow_id.0,
									error = %e,
									"failed to commit transaction"
								);
								continue;
							}

							current_version = final_version;
						}
						Err(Lagged(skipped)) => {
							warn!(
								flow_id = flow_id.0,
								skipped = skipped,
								"version broadcast lagged"
							);
						}
						Err(RecvError::Closed) => {
							debug!(flow_id = flow_id.0, "broadcast channel closed");
							break;
						}
					}
				}
			}
		}

		debug!(flow_id = flow_id.0, "processing loop exited");
	}

	/// Process a single version by fetching from provider and applying changes.
	async fn process_version(
		flow_id: FlowId,
		sources: &HashSet<PrimitiveId>,
		flow_txn: &mut FlowTransaction,
		flow_engine: &FlowEngine,
		provider: &FlowChangeProvider,
		version: CommitVersion,
	) -> Result<()> {
		// Early return if no sources were affected at this version
		let Some(all_changes) = provider.get_changes(version, sources).await? else {
			return Ok(());
		};

		let relevant: Vec<_> = all_changes
			.iter()
			.filter(|change| match &change.origin {
				FlowChangeOrigin::External(source) => sources.contains(source),
				FlowChangeOrigin::Internal(_) => false,
			})
			.cloned()
			.collect();

		if relevant.is_empty() {
			return Ok(());
		}

		for change in relevant {
			flow_engine.process(flow_txn, change, flow_id).await?;
		}

		debug!(flow_id = flow_id.0, version = version.0, "processed version");
		Ok(())
	}

	/// Load the checkpoint for this flow consumer.
	async fn load_checkpoint(engine: &StandardEngine, flow_id: FlowId) -> Result<CommitVersion> {
		let consumer_id = CdcConsumerId::new(&format!("flow-consumer-{}", flow_id.0));
		let mut txn = engine.begin_query().await?;
		CdcCheckpoint::fetch(&mut txn, &consumer_id).await
	}

	/// Shutdown the flow consumer gracefully.
	pub async fn shutdown(mut self) -> Result<()> {
		debug!(flow_id = self.flow_id.0, "shutting down flow consumer");

		// Signal shutdown
		self.shutdown.cancel();

		// Wait for worker to finish
		if let Some(worker) = self.worker.take() {
			worker.abort();
		}

		info!(flow_id = self.flow_id.0, "flow consumer shutdown complete");
		Ok(())
	}

	/// Get the current checkpoint version for this flow consumer.
	///
	/// This represents how far the flow has processed in the CDC log.
	pub async fn current_version(&self, engine: &StandardEngine) -> Result<CommitVersion> {
		Self::load_checkpoint(engine, self.flow_id).await
	}

	/// Get the flow ID.
	pub fn flow_id(&self) -> FlowId {
		self.flow_id
	}

	/// Get the source primitives for this flow.
	pub fn sources(&self) -> &HashSet<PrimitiveId> {
		&self.sources
	}
}

/// Extract source primitive IDs from a flow definition.
fn extract_sources(flow: &Flow) -> HashSet<PrimitiveId> {
	flow.graph
		.nodes()
		.filter_map(|(_, node)| match &node.ty {
			FlowNodeType::SourceTable {
				table,
			} => Some(PrimitiveId::Table(*table)),
			FlowNodeType::SourceView {
				view,
			} => Some(PrimitiveId::View(*view)),
			FlowNodeType::SourceFlow {
				flow,
			} => Some(PrimitiveId::Flow(*flow)),
			_ => None,
		})
		.collect()
}
