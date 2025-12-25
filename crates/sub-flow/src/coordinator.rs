// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Per-flow coordinator task implementation.

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use mpsc::UnboundedReceiver;
use reifydb_core::{
	CommitVersion, Result,
	interface::{CommandTransaction, Engine, QueryTransaction, catalog::FlowId},
	key::FlowVersionKey,
	value::encoded::EncodedValues,
};
use reifydb_engine::{StandardCommandTransaction, StandardEngine};
use reifydb_flow_operator_sdk::FlowChange;
use reifydb_rql::flow::Flow;
use reifydb_type::{CowVec, diagnostic::flow::flow_version_corrupted};
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument, trace};

use crate::{FlowEngine, transaction::FlowTransaction};

/// Per-flow coordinator task.
///
/// This async function is the main loop for a single flow. It:
/// 1. Performs optional backfill on startup
/// 2. Receives batches from its channel
/// 3. Skips already-processed versions (exactly-once)
/// 4. Processes each batch through the flow's operator pipeline
/// 5. Atomically persists version with output writes
#[instrument(
	name = "coordinate_task",
	level = "info",
	skip(rx, flow, engine, version, flow_engine),
	fields(flow_id = flow_id.0, backfill = backfill_version.is_some())
)]
pub async fn coordinate_task(
	flow_id: FlowId,
	mut rx: UnboundedReceiver<Vec<FlowChange>>,
	flow: Flow,
	engine: StandardEngine,
	version: Arc<AtomicU64>,
	backfill_version: Option<CommitVersion>,
	flow_engine: FlowEngine,
) {
	info!("flow task started");

	// Register this flow with the engine
	{
		let mut txn = match engine.begin_command().await {
			Ok(txn) => txn,
			Err(e) => {
				error!("failed to begin transaction for flow registration: {}", e);
				return;
			}
		};

		// Perform backfill if requested (new flow)
		if let Some(target_version) = backfill_version {
			debug!(target_version = target_version.0, "performing backfill");
			if let Err(e) = flow_engine.register_with_backfill(&mut txn, flow.clone(), target_version).await
			{
				error!("backfill failed: {}", e);
				return;
			}

			// Persist the backfill version
			if let Err(e) = set_flow_version(&mut txn, flow_id, target_version).await {
				error!("failed to persist backfill version: {}", e);
				return;
			}

			if let Err(e) = txn.commit().await {
				error!("failed to commit backfill: {}", e);
				return;
			}

			// Update in-memory version
			version.store(target_version.0, Ordering::Release);
			info!(version = target_version.0, "backfill completed");
		} else {
			// Existing flow - register without backfill
			if let Err(e) = flow_engine.register_without_backfill(&mut txn, flow.clone()).await {
				error!("flow registration failed: {}", e);
				return;
			}

			if let Err(e) = txn.commit().await {
				error!("failed to commit flow registration: {}", e);
				return;
			}
		}
	}

	// Main processing loop
	while let Some(changes) = rx.recv().await {
		// Extract version from first change (all changes have same version)
		let batch_version = changes.first().expect("empty batch should not be sent").version;
		let current_version = version.load(Ordering::Acquire);

		// Skip already-processed versions (exactly-once semantics)
		if batch_version.0 <= current_version {
			trace!(
				batch_version = batch_version.0,
				current_version = current_version,
				"skipping already-processed batch"
			);
			continue;
		}

		// Process in blocking context (operators may block)
		let result = process_batch(&engine, &flow_engine, flow_id, &changes).await;

		match result {
			Ok(()) => {
				// Update in-memory version after successful commit
				version.store(batch_version.0, Ordering::Release);
				trace!(version = batch_version.0, "batch processed");
			}
			Err(e) => {
				// Fail-fast on any error
				error!(version = batch_version.0, error = %e, "batch processing failed");
				panic!("flow {} processing failed: {}", flow_id.0, e);
			}
		}
	}

	info!("flow task exiting (channel closed)");
}

/// Process a single batch through the flow engine.
async fn process_batch(
	engine: &StandardEngine,
	flow_engine: &FlowEngine,
	flow_id: FlowId,
	changes: &[FlowChange],
) -> Result<()> {
	let version = changes.first().expect("empty batch should not be sent").version;

	let mut txn = engine.begin_command().await?;

	let mut flow_txn = FlowTransaction::new(&txn, version).await;
	for change in changes {
		flow_engine.process(&mut flow_txn, change.clone(), flow_id).await?;
	}

	flow_txn.commit(&mut txn).await?;
	set_flow_version(&mut txn, flow_id, version).await?;

	txn.commit().await?;
	Ok(())
}

/// Persist the flow's processed version to catalog.
async fn set_flow_version(txn: &mut StandardCommandTransaction, flow_id: FlowId, version: CommitVersion) -> Result<()> {
	let key = FlowVersionKey::encoded(flow_id);
	let value = EncodedValues(CowVec::new(version.0.to_le_bytes().to_vec()));
	txn.set(&key, value).await
}

/// Read the flow's processed version from catalog.
#[allow(dead_code)]
pub async fn get_flow_version(engine: &StandardEngine, flow_id: FlowId) -> Result<Option<CommitVersion>> {
	let mut txn = engine.begin_query().await?;
	let key = FlowVersionKey::encoded(flow_id);

	match txn.get(&key).await? {
		Some(multi_values) => {
			let arr: [u8; 8] = multi_values.values.as_slice().try_into().map_err(|_| {
				reifydb_core::Error(flow_version_corrupted(flow_id.0, multi_values.values.len()))
			})?;
			Ok(Some(CommitVersion(u64::from_le_bytes(arr))))
		}
		None => Ok(None),
	}
}
