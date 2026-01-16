// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Backfill logic for flows that need to catch up to the current version.

use std::{cmp::min, ops::Bound};

use reifydb_cdc::consume::checkpoint::CdcCheckpoint;
use reifydb_core::{common::CommitVersion, interface::cdc::CdcBatch};
use reifydb_transaction::standard::command::StandardCommandTransaction;
use reifydb_type::Result;
use tracing::{Span, debug, info, instrument};

use crate::{
	convert,
	coordinator::FlowCoordinator,
	instruction::{FlowInstruction, WorkerBatch},
	transaction::pending::Pending,
};

impl FlowCoordinator {
	/// Advance backfilling flows by one chunk each.
	///
	/// This method processes backfilling flows incrementally, allowing them to
	/// gradually catch up to the current version without blocking the pipeline.
	#[instrument(name = "flow::coordinator::advance_backfill", level = "debug", skip(self, txn), fields(
		backfilling = tracing::field::Empty,
		processed = tracing::field::Empty,
		elapsed_us = tracing::field::Empty
	))]
	pub(crate) fn advance_backfilling_flows(
		&self,
		txn: &mut StandardCommandTransaction,
		current_version: CommitVersion,
		state_version: CommitVersion,
	) -> Result<()> {
		let start = std::time::Instant::now();
		const BACKFILL_CHUNK_SIZE: u64 = 1_000;

		let backfilling_flows: Vec<_> = self.states.borrow().backfilling_flow_ids();
		Span::current().record("backfilling", backfilling_flows.len());
		let mut processed = 0u32;

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

			// Fetch CDC for this chunk from storage
			let batch = self
				.cdc_store
				.read_range(
					Bound::Excluded(from_version),
					Bound::Included(to_version),
					BACKFILL_CHUNK_SIZE,
				)
				.unwrap_or_else(|e| {
					tracing::warn!(error = %e, "Failed to read CDC range for backfill");
					CdcBatch::empty()
				});

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

			processed += 1;
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

		Span::current().record("processed", processed);
		Span::current().record("elapsed_us", start.elapsed().as_micros() as u64);
		Ok(())
	}
}
