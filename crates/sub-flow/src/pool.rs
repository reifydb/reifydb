// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Worker pool for parallel flow processing across N workers.

use std::sync::Arc;

use reifydb_catalog::Catalog;
use reifydb_core::{CommitVersion, Error, Result};
use reifydb_engine::StandardEngine;
use reifydb_type::internal;
use tracing::debug;

use crate::{
	FlowEngine,
	transaction::PendingWrites,
	worker::{Batch, FlowWorker},
};

/// Pool of N flow workers for parallel flow processing.
pub(crate) struct FlowWorkerPool {
	workers: Vec<FlowWorker>,
}

impl FlowWorkerPool {
	/// Create a new worker pool with N workers.
	///
	/// Each worker gets a unique ID (0..N-1) and processes flows assigned via hash partitioning.
	/// Workers share the FlowEngine (Arc-cloned) and each runs in its own OS thread.
	pub fn new(num_workers: usize, flow_engine: Arc<FlowEngine>, engine: StandardEngine, catalog: Catalog) -> Self {
		debug!(num_workers, "creating flow worker pool");

		let workers = (0..num_workers)
			.map(|worker_id| {
				FlowWorker::new(
					worker_id,
					num_workers,
					flow_engine.clone(),
					engine.clone(),
					catalog.clone(),
				)
			})
			.collect();

		Self {
			workers,
		}
	}

	/// Process versioned batches of decoded changes across all workers.
	///
	/// Broadcasts batches to all workers, each processing only their assigned flows.
	/// Aggregates PendingWrites from all workers with keyspace overlap validation.
	pub fn process(&self, batches: Vec<Batch>, state_version: CommitVersion) -> Result<PendingWrites> {
		// Broadcast batches to all workers in parallel
		let mut results = Vec::with_capacity(self.workers.len());

		for worker in &self.workers {
			let result = worker.process(batches.clone(), state_version)?;
			results.push(result);
		}

		// Aggregate results with keyspace validation
		self.aggregate_pending_writes(results)
	}

	/// Aggregate PendingWrites from multiple workers with keyspace overlap detection.
	///
	/// Returns error if any key exists in multiple worker write sets (indicates bug).
	fn aggregate_pending_writes(&self, writes: Vec<PendingWrites>) -> Result<PendingWrites> {
		let mut combined = PendingWrites::new();

		for pending in writes {
			for (key, value) in pending.iter_sorted() {
				// Validate no keyspace overlap between workers
				if combined.contains_key(&key) {
					return Err(Error(internal!(
						"keyspace overlap detected during worker aggregation: {}",
						reifydb_type::util::hex::encode(key.as_ref())
					)));
				}

				// Safe to merge - disjoint keyspaces
				match value {
					crate::transaction::Pending::Set(v) => {
						combined.insert(key.clone(), v.clone());
					}
					crate::transaction::Pending::Remove => {
						combined.remove(key.clone());
					}
				}
			}
		}

		Ok(combined)
	}

	/// Stop all workers in the pool.
	pub fn stop(&mut self) {
		for worker in &mut self.workers {
			worker.stop();
		}
	}
}

impl Drop for FlowWorkerPool {
	fn drop(&mut self) {
		self.stop();
	}
}
