// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Independent flow consumer implementing CdcConsume.

use std::{collections::HashSet, path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use reifydb_catalog::CatalogStore;
use reifydb_cdc::CdcConsume;
use reifydb_core::{
	CommitVersion, Result,
	interface::{Cdc, Engine, SourceId, WithEventBus},
};
use reifydb_engine::{StandardCommandTransaction, StandardEngine};
use reifydb_rql::flow::{Flow, FlowNodeType, load_flow};
use tokio::{
	sync::mpsc,
	task::JoinHandle,
	time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use crate::{
	FlowEngine, builder::OperatorFactory, coordinator::get_flow_version, dispatcher::dispatcher,
	registry::FlowRegistry,
};

/// Independent flow consumer with per-flow task architecture.
///
/// Each flow runs as an independent task with its own channel and version tracking.
/// This allows flows to process at their own pace without blocking each other.
pub struct IndependentFlowConsumer {
	/// Shutdown signal to signal dispatcher to exit.
	shutdown: CancellationToken,

	/// Channel sender for CDC events.
	cdc_tx: mpsc::UnboundedSender<Cdc>,

	/// Registry of active flows.
	registry: Arc<FlowRegistry>,

	/// Dispatcher task handle.
	#[allow(dead_code)]
	dispatcher_handle: JoinHandle<()>,
}

impl IndependentFlowConsumer {
	/// Create a new independent flow consumer.
	pub async fn new(
		engine: StandardEngine,
		operators: Vec<(String, OperatorFactory)>,
		operators_dir: Option<PathBuf>,
	) -> Result<Self> {
		// Load FFI operators eagerly so they're available in system.flow_operators
		// before any flows are created
		if let Some(ref dir) = operators_dir {
			if let Err(e) = FlowEngine::load_ffi_operators(dir, &engine.event_bus()) {
				warn!(error = %e, "failed to load FFI operators from {:?}", dir);
			}
		}

		let registry = Arc::new(FlowRegistry::new(engine.clone(), operators, operators_dir));
		let (cdc_tx, cdc_rx) = mpsc::unbounded_channel();
		let shutdown = CancellationToken::new();

		// Load existing flows from catalog and register them
		let existing_flows = load_all_flows(&engine).await?;
		info!(flow_count = existing_flows.len(), "loading existing flows");

		for (flow, sources) in existing_flows {
			let flow_id = flow.id;
			let persisted_version = get_flow_version(&engine, flow_id).await?.unwrap_or(CommitVersion(0));
			registry.register(flow, sources, persisted_version).await?;
			debug!(flow_id = flow_id.0, version = persisted_version.0, "registered existing flow");
		}

		// Spawn dispatcher task on the current runtime
		let dispatcher_handle = tokio::spawn(dispatcher(cdc_rx, registry.clone(), engine, shutdown.clone()));

		info!("independent flow consumer started");

		Ok(Self {
			shutdown,
			cdc_tx,
			registry,
			dispatcher_handle,
		})
	}

	/// Graceful shutdown with drain timeout.
	pub async fn shutdown(self, drain_timeout: Duration) {
		info!("initiating graceful shutdown");

		// Signal shutdown
		self.shutdown.cancel();

		let drain_deadline = Instant::now() + drain_timeout;

		let flow_ids = self.registry.flow_ids().await;
		info!(flow_count = flow_ids.len(), "draining flow tasks");

		for flow_id in flow_ids {
			if let Some(task) = self.registry.deregister(flow_id).await {
				let remaining = drain_deadline.saturating_duration_since(Instant::now());
				match timeout(remaining, task).await {
					Ok(Ok(())) => {
						debug!(flow_id = flow_id.0, "flow drained successfully");
					}
					Ok(Err(e)) => {
						warn!(flow_id = flow_id.0, error = ?e, "flow task panicked");
					}
					Err(_) => {
						warn!(flow_id = flow_id.0, "flow drain timed out");
					}
				}
			}
		}

		info!("shutdown complete");
	}
}

impl Drop for IndependentFlowConsumer {
	fn drop(&mut self) {
		// Cancel the shutdown token to signal dispatcher to exit.
		// This must happen before the runtime is dropped to avoid deadlock.
		self.shutdown.cancel();
		debug!("IndependentFlowConsumer shutdown signal sent");
	}
}

#[async_trait]
impl CdcConsume for IndependentFlowConsumer {
	async fn consume(&self, _txn: &mut StandardCommandTransaction, cdcs: Vec<Cdc>) -> Result<()> {
		if cdcs.is_empty() {
			return Ok(());
		}

		// Track the max version we're sending
		let max_version = cdcs.iter().map(|c| c.version).max().unwrap();
		let batch_count = cdcs.len();

		trace!(max_version = max_version.0, batch_count = batch_count, "forwarding CDC batches to dispatcher");

		// Forward to the dispatcher channel
		// Don't wait for completion - flows process asynchronously
		// The checkpoint is persisted by PollConsumer after this returns
		for cdc in cdcs {
			self.cdc_tx.send(cdc).map_err(|_| {
				reifydb_core::Error(reifydb_type::internal!("dispatcher channel closed"))
			})?;
		}

		trace!(max_version = max_version.0, "CDC batches forwarded to dispatcher");
		Ok(())
	}
}

/// Load all flows from catalog at startup.
async fn load_all_flows(engine: &StandardEngine) -> Result<Vec<(Flow, HashSet<reifydb_core::interface::SourceId>)>> {
	let mut txn = engine.begin_query().await?;

	let flow_defs = CatalogStore::list_flows_all(&mut txn).await?;
	let mut result = Vec::with_capacity(flow_defs.len());

	for flow_def in flow_defs {
		match load_flow(&mut txn, flow_def.id).await {
			Ok(flow) => {
				let sources = get_flow_sources(&flow);
				result.push((flow, sources));
			}
			Err(e) => {
				warn!(flow_id = flow_def.id.0, error = %e, "failed to load flow");
			}
		}
	}

	Ok(result)
}

/// Get the source tables/views this flow subscribes to.
fn get_flow_sources(flow: &Flow) -> HashSet<SourceId> {
	let mut sources = HashSet::new();

	for (_node_id, node) in flow.graph.nodes() {
		match &node.ty {
			FlowNodeType::SourceTable {
				table,
			} => {
				sources.insert(SourceId::Table(*table));
			}
			FlowNodeType::SourceView {
				view,
			} => {
				sources.insert(SourceId::View(*view));
			}
			FlowNodeType::SourceFlow {
				flow,
			} => {
				sources.insert(SourceId::Flow(*flow));
			}
			_ => {}
		}
	}

	sources
}
