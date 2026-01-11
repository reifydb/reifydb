// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Worker pool for parallel flow processing across N workers.

use crate::{FlowEngine, instruction::WorkerBatch, transaction::PendingWrites, worker::FlowWorker};
use reifydb_catalog::Catalog;
use reifydb_core::{Error, Result};
use reifydb_engine::StandardEngine;
use reifydb_type::internal;
use reifydb_type::util::hex::encode;
use std::collections::HashMap;
use tracing::debug;

/// Pool of N flow workers for parallel flow processing.
pub(crate) struct FlowWorkerPool {
	workers: Vec<FlowWorker>,
}

impl FlowWorkerPool {
	/// Returns the number of workers in the pool.
	pub fn num_workers(&self) -> usize {
		self.workers.len()
	}
}

impl FlowWorkerPool {
	/// Create a new worker pool with N workers.
	///
	/// Each worker gets a unique ID (0..N-1) and processes flows assigned via hash partitioning.
	/// Each worker gets its own FlowEngine instance created by the factory function.
	pub fn new<F, Fac>(num_workers: usize, factory_builder: F, engine: StandardEngine, catalog: Catalog) -> Self
	where
		F: Fn() -> Fac + Send + 'static,
		Fac: FnOnce() -> FlowEngine + Send + 'static,
	{
		debug!(num_workers, "creating flow worker pool");

		let workers = (0..num_workers)
			.map(|_| {
				let worker_factory = factory_builder();
				FlowWorker::new(worker_factory, engine.clone(), catalog.clone())
			})
			.collect();

		Self {
			workers,
		}
	}

	/// Register a flow in the assigned worker's FlowEngine.
	///
	/// Uses hash partitioning to assign flow to specific worker: (flow_id % num_workers)
	pub fn register_flow(&self, flow: reifydb_rql::flow::FlowDag) -> Result<()> {
		let flow_id = flow.id;
		let worker_id = (flow_id.0 as usize) % self.workers.len();

		self.workers[worker_id].register_flow(flow)?;
		Ok(())
	}

	pub fn submit_to_worker(&self, worker_id: usize, batch: WorkerBatch) -> Result<PendingWrites> {
		if worker_id >= self.workers.len() {
			return Err(Error(internal!("Invalid worker_id: {}", worker_id)));
		}

		self.workers[worker_id].process(batch)
	}

	pub fn submit(&self, batches: HashMap<usize, WorkerBatch>) -> Result<PendingWrites> {
		let mut results = Vec::with_capacity(batches.len());

		for (worker_id, batch) in batches {
			if worker_id >= self.workers.len() {
				return Err(Error(internal!("Invalid worker_id: {}", worker_id)));
			}

			results.push(self.workers[worker_id].process(batch)?);
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
						encode(key.as_ref())
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
