// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Flow coordinator that handles CDC consumption and orchestration.

use std::cell::RefCell;
use std::cmp::min;
use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;

use crate::{
	catalog::FlowCatalog,
	convert,
	instruction::{FlowInstruction, WorkerBatch},
	pool::FlowWorkerPool,
	state::FlowStates,
	tracker::PrimitiveVersionTracker,
	transaction::Pending,
};
use reifydb_cdc::{CdcCheckpoint, CdcConsume};
use reifydb_core::interface::{CdcChange, FlowId, PrimitiveId};
use reifydb_core::{
	CommitVersion, Result,
	interface::Cdc,
	key::{Key, KeyKind},
};
use reifydb_engine::{StandardCommandTransaction, StandardEngine};
use reifydb_rql::flow::FlowGraphAnalyzer;
use reifydb_sdk::FlowChange;
use reifydb_sdk::FlowChangeOrigin::External;
use reifydb_transaction::cdc::CdcQueryTransaction;
use tracing::{debug, info};

/// Flow coordinator that implements CDC consumption logic.
pub(crate) struct FlowCoordinator {
	pub(crate) engine: StandardEngine,
	pub(crate) catalog: FlowCatalog,
	pub(crate) pool: FlowWorkerPool,
	tracker: Arc<PrimitiveVersionTracker>,
	/// Per-flow state tracking for routing and backfill management.
	pub(crate) states: RefCell<FlowStates>,
	/// Analyzer for source-to-flow mapping.
	pub(crate) analyzer: RefCell<FlowGraphAnalyzer>,
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
			states: RefCell::new(FlowStates::new()),
			analyzer: RefCell::new(FlowGraphAnalyzer::new()),
		}
	}
}

impl CdcConsume for FlowCoordinator {
	fn consume(&self, txn: &mut StandardCommandTransaction, cdcs: Vec<Cdc>) -> Result<()> {
		let state_version = self.get_parent_snapshot_version(txn)?;

		let latest_version = cdcs.last().map(|c| c.version);

		let mut all_changes = Vec::new();
		for cdc in &cdcs {
			let version = cdc.version;

			// Update tracker for lag calculation
			for change in &cdc.changes {
				if let Some(Key::Row(row_key)) = Key::decode(change.key()) {
					self.tracker.update(row_key.primitive, version);
				}
			}

			self.handle_new_flows(txn, cdc)?;

			// Convert CDC to flow changes
			let changes = convert::to_flow_change(&self.engine, &self.catalog, cdc, version)?;
			all_changes.extend(changes);
		}

		// Route changes to active flows and group by worker
		if let Some(to_version) = latest_version {
			let worker_batches = self.route_and_group_changes(&all_changes, to_version, state_version);

			// Submit targeted batches to workers
			if !worker_batches.is_empty() {
				let pending_writes = self.pool.submit(worker_batches)?;

				// Apply pending writes to transaction
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
			}

			for flow_id in self.states.borrow().active_flow_ids() {
				CdcCheckpoint::persist(txn, &flow_id, to_version)?;
			}

			self.advance_backfilling_flows(txn, to_version, state_version)?;
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

							let (flow, is_new) =
								self.catalog.get_or_load_flow(txn, flow_id)?;
							if is_new {
								self.pool.register_flow(flow.clone())?;
								self.analyzer.borrow_mut().add(flow.clone());
								self.states.borrow_mut().register_backfilling(flow_id);

								debug!(
									flow_id = flow_id.0,
									"registered new flow in backfilling status"
								);
							}
						}
					}
				}
			}
		}

		Ok(())
	}

	/// Filter CDC changes to only those relevant to a specific flow.
	///
	/// Uses the flow analyzer to determine which sources the flow depends on,
	/// then filters changes to only include those from subscribed sources.
	/// Maintains original CDC sequence order.
	fn filter_cdc_for_flow(&self, flow_id: FlowId, changes: &[FlowChange]) -> Vec<FlowChange> {
		let analyzer = self.analyzer.borrow();
		let dependency_graph = analyzer.get_dependency_graph();

		// Get all sources this flow depends on
		let mut flow_sources: std::collections::HashSet<PrimitiveId> = std::collections::HashSet::new();

		// Add table sources
		for (table_id, flow_ids) in &dependency_graph.source_tables {
			if flow_ids.contains(&flow_id) {
				flow_sources.insert(PrimitiveId::Table(*table_id));
			}
		}

		// Add view sources
		for (view_id, flow_ids) in &dependency_graph.source_views {
			if flow_ids.contains(&flow_id) {
				flow_sources.insert(PrimitiveId::View(*view_id));
			}
		}

		// Filter changes to only those from this flow's sources
		changes.iter()
			.filter(|change| {
				if let External(source) = change.origin {
					flow_sources.contains(&source)
				} else {
					// Internal changes are already scoped, keep them
					true
				}
			})
			.cloned()
			.collect()
	}

	/// Route CDC changes to flows and group by worker.
	///
	/// Returns a map of worker_id -> WorkerBatch containing only the
	/// changes relevant to each worker's assigned flows.
	fn route_and_group_changes(
		&self,
		changes: &[FlowChange],
		to_version: CommitVersion,
		state_version: CommitVersion,
	) -> HashMap<usize, WorkerBatch> {
		let states = self.states.borrow();
		let num_workers = self.pool.num_workers();
		let mut worker_batches: HashMap<usize, WorkerBatch> = HashMap::new();

		// Only process active flows (not backfilling)
		for flow_id in states.active_flow_ids() {
			let flow_changes = self.filter_cdc_for_flow(flow_id, changes);

			// Skip flows with no relevant changes
			if flow_changes.is_empty() {
				continue;
			}

			let worker_id = (flow_id.0 as usize) % num_workers;

			let batch = worker_batches.entry(worker_id).or_insert_with(|| WorkerBatch::new(state_version));

			batch.add_instruction(FlowInstruction::new(flow_id, to_version, flow_changes));
		}

		worker_batches
	}

	/// Advance backfilling flows by one chunk each.
	///
	/// This method processes backfilling flows incrementally, allowing them to
	/// gradually catch up to the current version without blocking the pipeline.
	fn advance_backfilling_flows(
		&self,
		txn: &mut StandardCommandTransaction,
		current_version: CommitVersion,
		state_version: CommitVersion,
	) -> Result<()> {
		const BACKFILL_CHUNK_SIZE: u64 = 1_000;

		let backfilling_flows: Vec<FlowId> = self.states.borrow().backfilling_flow_ids();

		for flow_id in backfilling_flows {
			// Get current checkpoint for this flow
			let from_version = {
				let mut query = self.engine.begin_query()?;
				CdcCheckpoint::fetch(&mut query, &flow_id).unwrap_or(CommitVersion(0))
			};

			// Check if already caught up
			if from_version >= current_version {
				if let Some(state) = self.states.borrow_mut().get_mut(&flow_id) {
					state.activate();
					state.update_checkpoint(current_version);
				}
				info!(flow_id = flow_id.0, "backfill complete, flow now active");
				continue;
			}

			// Calculate chunk range
			let to_version = CommitVersion(min(from_version.0 + BACKFILL_CHUNK_SIZE, current_version.0));

			// Fetch CDC for this chunk
			let cdc_txn = txn.begin_cdc_query()?;
			let batch = cdc_txn.range(Bound::Excluded(from_version), Bound::Included(to_version))?;

			if batch.items.is_empty() {
				// No CDC in this range, advance checkpoint
				CdcCheckpoint::persist(txn, &flow_id, to_version)?;
				{
					let mut states = self.states.borrow_mut();
					if let Some(state) = states.get_mut(&flow_id) {
						state.update_checkpoint(to_version);
					}
				}
				continue;
			}

			// Convert CDC to flow changes
			let mut chunk_changes = Vec::new();
			for cdc in &batch.items {
				let changes = convert::to_flow_change(&self.engine, &self.catalog, cdc, cdc.version)?;
				chunk_changes.extend(changes);
			}

			// Filter to only changes relevant to this flow
			let flow_changes = self.filter_cdc_for_flow(flow_id, &chunk_changes);

			if flow_changes.is_empty() {
				// CDC exists but no relevant changes for this flow, advance checkpoint
				CdcCheckpoint::persist(txn, &flow_id, to_version)?;
				if let Some(state) = self.states.borrow_mut().get_mut(&flow_id) {
					state.update_checkpoint(to_version);
				}
				continue;
			}

			// Create instruction and send to worker
			let instruction = FlowInstruction::new(flow_id, to_version, flow_changes);
			let worker_id = (flow_id.0 as usize) % self.pool.num_workers();

			let mut worker_batch = WorkerBatch::new(state_version);
			worker_batch.add_instruction(instruction);

			let pending_writes = self.pool.submit_to_worker(worker_id, worker_batch)?;

			// Apply pending writes
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

			// Update checkpoint
			CdcCheckpoint::persist(txn, &flow_id, to_version)?;
			if let Some(state) = self.states.borrow_mut().get_mut(&flow_id) {
				state.update_checkpoint(to_version);
			}

			debug!(
				flow_id = flow_id.0,
				from = from_version.0,
				to = to_version.0,
				"advanced backfilling flow by one chunk"
			);

			// Check if now caught up
			if to_version >= current_version {
				if let Some(state) = self.states.borrow_mut().get_mut(&flow_id) {
					state.activate();
				}
				info!(flow_id = flow_id.0, "backfill complete, flow now active");
			}
		}

		Ok(())
	}
}
