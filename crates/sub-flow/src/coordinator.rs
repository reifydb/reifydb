// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Flow coordinator that handles CDC consumption and orchestration.

use std::sync::Arc;

use crate::{
	catalog::FlowCatalog, convert, pool::FlowWorkerPool, tracker::PrimitiveVersionTracker, transaction::Pending,
	worker::Batch,
};
use reifydb_cdc::{CdcCheckpoint, CdcConsume};
use reifydb_core::interface::CdcChange;
use reifydb_core::{
	CommitVersion, Result,
	interface::Cdc,
	key::{Key, KeyKind},
};
use reifydb_engine::{StandardCommandTransaction, StandardEngine};
use tracing::debug;

/// Flow coordinator that implements CDC consumption logic.
pub(crate) struct FlowCoordinator {
	pub(crate) engine: StandardEngine,
	pub(crate) catalog: FlowCatalog,
	pub(crate) pool: FlowWorkerPool,
	tracker: Arc<PrimitiveVersionTracker>,
}

impl FlowCoordinator {
	/// Create a new flow coordinator.
	pub fn new(engine: StandardEngine, tracker: Arc<PrimitiveVersionTracker>, pool: FlowWorkerPool) -> Self {
		let catalog = FlowCatalog::new(engine.catalog());
		Self {
			engine,
			catalog,
			pool,
			tracker,
		}
	}
}

impl CdcConsume for FlowCoordinator {
	fn consume(&self, txn: &mut StandardCommandTransaction, cdcs: Vec<Cdc>) -> Result<()> {
		let state_version = self.get_parent_snapshot_version(txn)?;

		let latest_version = cdcs.last().map(|c| c.version);

		let mut batches = Vec::new();
		for cdc in cdcs {
			let version = cdc.version;

			for change in &cdc.changes {
				if let Some(Key::Row(row_key)) = Key::decode(change.key()) {
					self.tracker.update(row_key.primitive, version);
				}
			}

			self.handle_new_flows(txn, &cdc)?;

			let changes = convert::to_flow_change(&self.engine, &self.catalog, &cdc, version)?;
			batches.push(Batch {
				version,
				changes,
			});
		}

		let pending_writes = self.pool.submit(batches, state_version)?;

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

		if let Some(version) = latest_version {
			let registered = self.catalog.get_flow_ids();
			for flow_id in &registered {
				CdcCheckpoint::persist(txn, flow_id, version)?;
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
	fn handle_new_flows(&self, txn: &mut StandardCommandTransaction, cdc: &Cdc) -> Result<()> {
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

							let (flow, is_new) = self.catalog.get_or_load_flow(txn, flow_id)?;
							if is_new {
								self.pool.register_flow(flow.clone())?;

								debug!(flow_id = flow_id.0, "registered new flow");
								self.backfill(flow_id, cdc.version)?;
							}
						}
					}
				}
			}
		}

		Ok(())
	}
}
