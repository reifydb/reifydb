// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Per-flow consumer that processes source changes via the FlowChangeProvider.
//!
//! Flow consumers listen for version broadcasts from the Coordinator and fetch
//! decoded changes from the shared FlowChangeProvider, filtering for their sources.

use std::{collections::HashSet, sync::Arc, time::Instant};

use broadcast::error::{RecvError, RecvError::Lagged};
use reifydb_cdc::CdcCheckpoint;
use reifydb_core::{
	CommitVersion, Result,
	interface::{CdcConsumerId, FlowId, PrimitiveId},
};
use reifydb_engine::StandardEngine;
use reifydb_rql::flow::{Flow, FlowNodeType};
use reifydb_sdk::FlowChangeOrigin;
use reifydb_sub_server::SharedRuntime;
use tokio::{select, sync::broadcast, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{Span, debug, error, info, instrument, warn};

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
		runtime: SharedRuntime,
	) -> Result<Self> {
		// Extract source primitives from flow definition
		let sources = extract_sources(&flow);

		debug!(flow_id = flow_id.0, sources = sources.len(), "extracted flow sources");

		// Spawn worker task
		let worker = {
			let sources = sources.clone();
			let shutdown = shutdown.clone();

			runtime.spawn(Self::processing_loop(
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
		let mut current_version = match Self::load_checkpoint(&engine, flow_id) {
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
							let end_version = broadcast.version.0.min(start_version + 9);

							if start_version > end_version {
								continue; // Nothing to process
							}

							// Call instrumented batch processing function
							match Self::process_batch(
								flow_id,
								start_version,
								end_version,
								&sources,
								&engine,
								&flow_engine,
								&provider,
								&consumer_id,
								current_version,
							)
							.await
							{
								Ok(final_version) => {
									current_version = final_version;
								}
								Err(e) => {
									error!(
										flow_id = flow_id.0,
										start_version = start_version,
										end_version = end_version,
										error = %e,
										"failed to process batch"
									);
									// Continue processing next batch despite error
								}
							}
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
	#[instrument(name = "flow::consumer::process_version", level = "debug", skip_all, fields(
		flow_id = flow_id.0,
		version = version.0,
		source_count = sources.len(),
		relevant_changes = tracing::field::Empty,
		process_time_us = tracing::field::Empty
	))]
	async fn process_version(
		flow_id: FlowId,
		sources: &HashSet<PrimitiveId>,
		flow_txn: &mut FlowTransaction,
		flow_engine: &FlowEngine,
		provider: &FlowChangeProvider,
		version: CommitVersion,
	) -> Result<()> {
		let process_start = Instant::now();

		// Early return if no sources were affected at this version
		let Some(all_changes) = provider.get_changes(version, sources).await? else {
			Span::current().record("relevant_changes", 0);
			Span::current().record("process_time_us", process_start.elapsed().as_micros() as u64);
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

		Span::current().record("relevant_changes", relevant.len());

		if relevant.is_empty() {
			Span::current().record("process_time_us", process_start.elapsed().as_micros() as u64);
			return Ok(());
		}

		for change in relevant {
			flow_engine.process(flow_txn, change, flow_id)?;
		}

		Span::current().record("process_time_us", process_start.elapsed().as_micros() as u64);

		debug!(flow_id = flow_id.0, version = version.0, "processed version");
		Ok(())
	}

	/// Process a batch of versions
	#[instrument(name = "flow::batch", level = "info", skip_all, fields(
		flow_id = flow_id.0,
		start_version = start_version,
		end_version = end_version,
		batch_size = end_version - start_version + 1,
		versions_processed = tracing::field::Empty,
		txn_begin_ms = tracing::field::Empty,
		flow_txn_init_ms = tracing::field::Empty,
		processing_ms = tracing::field::Empty,
		flow_commit_ms = tracing::field::Empty,
		parent_commit_ms = tracing::field::Empty,
		total_ms = tracing::field::Empty
	))]
	async fn process_batch(
		flow_id: FlowId,
		start_version: u64,
		end_version: u64,
		sources: &HashSet<PrimitiveId>,
		engine: &StandardEngine,
		flow_engine: &FlowEngine,
		provider: &FlowChangeProvider,
		consumer_id: &CdcConsumerId,
		current_version: CommitVersion,
	) -> crate::Result<CommitVersion> {
		let batch_start = Instant::now();

		// Single parent transaction for entire batch
		let txn_begin_start = Instant::now();
		let mut txn = engine.begin_command()?;
		Span::current().record("txn_begin_us", txn_begin_start.elapsed().as_micros() as u64);

		let catalog = engine.catalog();

		let flow_txn_init_start = Instant::now();
		let mut flow_txn = FlowTransaction::new(&mut txn, CommitVersion(start_version), catalog.clone());
		Span::current().record("flow_txn_init_us", flow_txn_init_start.elapsed().as_micros() as u64);

		let processing_start = Instant::now();
		let mut final_version = current_version;
		let mut versions_processed = 0;

		for version in start_version..=end_version {
			let version = CommitVersion(version);
			flow_txn.update_version(version);

			Self::process_version(flow_id, sources, &mut flow_txn, flow_engine, provider, version).await?;

			final_version = version;
			versions_processed += 1;
		}
		Span::current().record("processing_us", processing_start.elapsed().as_micros() as u64);

		let flow_commit_start = Instant::now();
		flow_txn.commit(&mut txn)?;
		Span::current().record("flow_commit_us", flow_commit_start.elapsed().as_micros() as u64);

		CdcCheckpoint::persist(&mut txn, consumer_id, final_version)?;

		let parent_commit_start = Instant::now();
		txn.commit()?;
		Span::current().record("parent_commit_us", parent_commit_start.elapsed().as_micros() as u64);

		Span::current().record("versions_processed", versions_processed);
		Span::current().record("total_ms", batch_start.elapsed().as_millis() as u64);

		Ok(final_version)
	}

	/// Load the checkpoint for this flow consumer.
	fn load_checkpoint(engine: &StandardEngine, flow_id: FlowId) -> Result<CommitVersion> {
		let consumer_id = CdcConsumerId::new(&format!("flow-consumer-{}", flow_id.0));
		let mut txn = engine.begin_query()?;
		CdcCheckpoint::fetch(&mut txn, &consumer_id)
	}

	/// Shutdown the flow consumer gracefully.
	pub fn shutdown(mut self) -> Result<()> {
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
	pub fn current_version(&self, engine: &StandardEngine) -> Result<CommitVersion> {
		Self::load_checkpoint(engine, self.flow_id)
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
