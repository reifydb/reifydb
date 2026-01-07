// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Flow coordinator that handles CDC consumption and orchestration.

use std::sync::Arc;

use reifydb_cdc::{CdcCheckpoint, CdcConsume};
use reifydb_core::{
	CommitVersion, Result,
	interface::{Cdc, CdcChange, FlowId},
	key::{Key, KeyKind},
};
use reifydb_engine::{StandardCommandTransaction, StandardEngine};
use reifydb_rql::flow::{Flow, load_flow};
use reifydb_sdk::FlowChange;
use tracing::{debug, warn};

use crate::{
	FlowEngine, catalog::FlowCatalog, convert::convert_cdc_to_flow_change, pool::FlowWorkerPool,
	tracker::PrimitiveVersionTracker, transaction::Pending, worker::Batch,
};

/// Flow coordinator that implements CDC consumption logic.
pub(crate) struct FlowCoordinator {
	pub(crate) engine: StandardEngine,
	flow_engine: Arc<FlowEngine>,
	tracker: Arc<PrimitiveVersionTracker>,
	catalog_cache: FlowCatalog,
	pub(crate) pool: FlowWorkerPool,
}

impl FlowCoordinator {
	/// Create a new flow coordinator.
	pub fn new(
		engine: StandardEngine,
		flow_engine: Arc<FlowEngine>,
		tracker: Arc<PrimitiveVersionTracker>,
		pool: FlowWorkerPool,
	) -> Self {
		let catalog_cache = FlowCatalog::new(engine.catalog());

		Self {
			engine,
			flow_engine,
			tracker,
			catalog_cache,
			pool,
		}
	}
}

impl CdcConsume for FlowCoordinator {
	fn consume(&self, txn: &mut StandardCommandTransaction, cdcs: Vec<Cdc>) -> Result<()> {
		let state_version = self.get_parent_snapshot_version(txn)?;

		// Capture latest version before consuming the vector
		let latest_version = cdcs.last().map(|c| c.version);

		let mut batches = Vec::new();
		for cdc in cdcs {
			let version = cdc.version;

			for change in &cdc.changes {
				if let Some(Key::Row(row_key)) = Key::decode(change.key()) {
					self.tracker.update(row_key.primitive, version);
				}
			}

			let new_flows = self.detect_new_flows(txn, &cdc)?;
			for (flow_id, _flow) in new_flows {
				self.backfill(flow_id, version)?;
			}

			let changes = self.decode_cdc(&cdc, version)?;
			batches.push(Batch {
				version,
				changes,
			});
		}

		let pending_writes = self.pool.process(batches, state_version)?;

		for (key, pending) in pending_writes.iter_sorted() {
			match pending {
				Pending::Set(value) => {
					txn.set(key, value.clone())?;
				}
				Pending::Remove => {
					txn.remove(key)?;
				}
			}
		}

		// Persist per-flow checkpoints for all registered flows
		if let Some(version) = latest_version {
			for flow_id in self.flow_engine.flow_ids() {
				CdcCheckpoint::persist(txn, &flow_id, version)?;
			}
		}

		Ok(())
	}
}

impl FlowCoordinator {
	/// Get the parent transaction's snapshot version for state reads.
	/// This version is constant for the entire CDC batch.
	pub(crate) fn get_parent_snapshot_version(&self, txn: &StandardCommandTransaction) -> Result<CommitVersion> {
		let query_txn = txn.multi.begin_query()?;
		Ok(query_txn.version())
	}

	/// Detect new flow registrations from CDC.
	fn detect_new_flows(&self, txn: &mut StandardCommandTransaction, cdc: &Cdc) -> Result<Vec<(FlowId, Flow)>> {
		let mut new_flows = Vec::new();

		for change in &cdc.changes {
			if let Some(kind) = Key::kind(change.key()) {
				if kind == KeyKind::Flow {
					if let CdcChange::Insert {
						key,
						..
					} = &change.change
					{
						if let Some(Key::Flow(flow_key)) = Key::decode(key) {
							let flow_id = flow_key.flow;

							// Check if not already registered
							if !self.flow_engine.inner.flows.read().contains_key(&flow_id) {
								let flow = load_flow(txn, flow_id)?;
								self.flow_engine.register(txn, flow.clone())?;
								debug!(flow_id = flow_id.0, "detected new flow");
								new_flows.push((flow_id, flow));
							}
						}
					}
				}
			}
		}

		Ok(new_flows)
	}

	pub(crate) fn decode_cdc(&self, cdc: &Cdc, version: CommitVersion) -> Result<Vec<FlowChange>> {
		let mut changes = Vec::new();

		let mut query_txn = self.engine.begin_query_at_version(version)?;

		for cdc_change in &cdc.changes {
			if let Some(Key::Row(row_key)) = Key::decode(cdc_change.key()) {
				let source_id = row_key.primitive;
				let row_number = row_key.row;

				// Skip Delete events with no pre-image
				if let CdcChange::Delete {
					pre: None,
					..
				} = &cdc_change.change
				{
					continue;
				}

				match convert_cdc_to_flow_change(
					&mut query_txn,
					&self.catalog_cache,
					source_id,
					row_number,
					&cdc_change.change,
					version,
				) {
					Ok(change) => changes.push(change),
					Err(e) => {
						warn!(
							source = ?source_id,
							row = row_number.0,
							error = %e,
							"failed to decode CDC change"
						);
					}
				}
			}
		}

		Ok(changes)
	}
}
