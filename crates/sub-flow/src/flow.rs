// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Per-flow CDC consumer that processes source changes from earliest CDC version.

use std::{collections::HashSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use reifydb_cdc::{CdcCheckpoint, CdcConsume, CdcConsumer, PollConsumer, PollConsumerConfig};
use reifydb_core::{
	CommitVersion, Result,
	interface::{Cdc, CdcConsumerId, FlowId, PrimitiveId},
	key::Key,
};
use reifydb_engine::{StandardCommandTransaction, StandardEngine};
use reifydb_rql::flow::{Flow, FlowNodeType};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{FlowEngine, FlowTransaction, catalog::FlowCatalog, convert::convert_cdc_to_flow_change};

/// Per-flow consumer that processes CDC events for its sources.
pub struct FlowConsumer {
	flow_id: FlowId,
	sources: HashSet<PrimitiveId>,
	consumer: Option<PollConsumer<FlowConsumeImpl>>,
	shutdown: CancellationToken,
}

/// Implementation of CDC consume logic for a single flow.
struct FlowConsumeImpl {
	flow_id: FlowId,
	sources: HashSet<PrimitiveId>,
	engine: StandardEngine,
	flow_engine: Arc<FlowEngine>,
}

impl FlowConsumer {
	/// Spawn a new flow consumer for the given flow.
	///
	/// The consumer will start processing from CommitVersion(1) and catch up to present.
	/// Each flow consumer has its own unique consumer ID and maintains its own checkpoint.
	pub fn spawn(
		flow_id: FlowId,
		flow: Flow,
		engine: StandardEngine,
		flow_engine: Arc<FlowEngine>,
		shutdown: CancellationToken,
	) -> Result<Self> {
		// Extract source primitives from flow definition
		let sources = extract_sources(&flow);

		debug!(flow_id = flow_id.0, sources = sources.len(), "extracted flow sources");

		// Create consume implementation
		let consume_impl = FlowConsumeImpl {
			flow_id,
			sources: sources.clone(),
			engine: engine.clone(),
			flow_engine,
		};

		// Configure consumer
		let consumer_id = CdcConsumerId::new(&format!("flow-consumer-{}", flow_id.0));
		let config = PollConsumerConfig::new(consumer_id, Duration::from_millis(1), None);

		// Create and start poll consumer
		let mut consumer = PollConsumer::new(config, engine, consume_impl);
		consumer.start()?;

		info!(flow_id = flow_id.0, "flow consumer started");

		Ok(Self {
			flow_id,
			sources,
			consumer: Some(consumer),
			shutdown,
		})
	}

	/// Shutdown the flow consumer gracefully.
	pub async fn shutdown(mut self) -> Result<()> {
		debug!(flow_id = self.flow_id.0, "shutting down flow consumer");

		// Signal shutdown
		self.shutdown.cancel();

		// Stop poll consumer
		if let Some(mut consumer) = self.consumer.take() {
			consumer.stop()?;
		}

		info!(flow_id = self.flow_id.0, "flow consumer shutdown complete");
		Ok(())
	}

	/// Get the current checkpoint version for this flow consumer.
	///
	/// This represents how far the flow has processed in the CDC log.
	pub async fn current_version(&self, engine: &StandardEngine) -> Result<CommitVersion> {
		let consumer_id = CdcConsumerId::new(&format!("flow-consumer-{}", self.flow_id.0));
		let mut txn = engine.begin_query().await?;
		let version = CdcCheckpoint::fetch(&mut txn, &consumer_id).await?;
		Ok(version)
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

#[async_trait]
impl CdcConsume for FlowConsumeImpl {
	async fn consume(&self, txn: &mut StandardCommandTransaction, cdcs: Vec<Cdc>) -> Result<()> {
		let catalog_cache = FlowCatalog::new(self.flow_engine.inner.catalog.clone());

		for cdc in cdcs {
			let version = cdc.version;

			// Collect changes for this flow
			let mut flow_changes = Vec::new();

			// Create query transaction for row decoding at the CDC event's version
			let mut query_txn = self.engine.begin_query_at_version(version).await?;
			for cdc_change in &cdc.changes {
				// Only process Row keys (data events)
				if let Some(Key::Row(row_key)) = Key::decode(cdc_change.key()) {
					let source_id = row_key.primitive;
					let row_number = row_key.row;

					// Check if this source belongs to our flow
					if self.sources.contains(&source_id) {
						// Reuse existing conversion logic from convert
						match convert_cdc_to_flow_change(
							&mut query_txn,
							&catalog_cache,
							source_id,
							row_number,
							&cdc_change.change,
							version,
						)
						.await
						{
							Ok(change) => flow_changes.push(change),
							Err(e) => {
								warn!(
									flow_id = self.flow_id.0,
									source = ?source_id,
									row = row_number.0,
									error = %e,
									"failed to decode row"
								);
								continue;
							}
						}
					}
				}
			}

			// Drop query transaction before processing
			drop(query_txn);

			// Process batch if we have any changes
			if !flow_changes.is_empty() {
				let mut ft = FlowTransaction::new(txn, version, self.engine.catalog()).await;
				for change in flow_changes {
					self.flow_engine.process(&mut ft, change, self.flow_id).await?;
				}

				debug!(flow_id = self.flow_id.0, version = version.0, "processed flow changes");
			}
		}
		Ok(())
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
