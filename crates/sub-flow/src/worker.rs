// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Flow worker that handles flow processing logic.

use std::{
	mem::take,
	sync::Arc,
	thread::{JoinHandle, spawn},
};

use crossbeam_channel::{Receiver, Sender};
use reifydb_catalog::Catalog;
use reifydb_core::{CommitVersion, Error, Result};
use reifydb_engine::StandardEngine;
use reifydb_sdk::FlowChange;
use reifydb_type::internal;
use tracing::error;

use crate::{
	FlowEngine,
	transaction::{FlowTransaction, PendingWrites},
};

/// A batch of changes that all belong to the same CDC version.
#[derive(Clone)]
pub(crate) struct Batch {
	pub version: CommitVersion,
	pub changes: Vec<FlowChange>,
}

/// Message types for worker communication.
enum WorkerRequest {
	ProcessVersionedBatches {
		batches: Vec<Batch>,
		state_version: CommitVersion,
		response: Sender<WorkerResponse>,
	},
	Stop,
}

enum WorkerResponse {
	Success(PendingWrites),
	Error(String),
}

/// Flow processing worker running in its own OS thread.
pub(crate) struct FlowWorker {
	tx: Sender<WorkerRequest>,
	handle: Option<JoinHandle<()>>,
}

impl FlowWorker {
	/// Create a new flow worker with its own OS thread.
	pub fn new(
		worker_id: usize,
		num_workers: usize,
		flow_engine: Arc<FlowEngine>,
		engine: StandardEngine,
		catalog: Catalog,
	) -> Self {
		let (tx, rx) = crossbeam_channel::unbounded();

		let thread_handle = spawn(move || {
			Self::worker_thread(rx, flow_engine, engine, catalog, worker_id, num_workers);
		});

		Self {
			tx,
			handle: Some(thread_handle),
		}
	}

	/// Process versioned batches of decoded changes for all flows.
	///
	/// Each batch contains changes from a single CDC version. The worker processes
	/// each version sequentially with correct snapshot isolation, accumulating
	/// pending writes across all versions.
	pub fn process(&self, batches: Vec<Batch>, state_version: CommitVersion) -> Result<PendingWrites> {
		let (resp_tx, resp_rx) = crossbeam_channel::bounded(1);

		self.tx.send(WorkerRequest::ProcessVersionedBatches {
			batches,
			state_version,
			response: resp_tx,
		})
		.map_err(|_| Error(internal!("Worker thread died")))?;

		match resp_rx.recv().map_err(|_| Error(internal!("Worker response error")))? {
			WorkerResponse::Success(pending) => Ok(pending),
			WorkerResponse::Error(e) => Err(Error(internal!("{}", e))),
		}
	}

	/// Stop the worker thread.
	pub fn stop(&mut self) {
		self.tx.send(WorkerRequest::Stop).ok();
		if let Some(handle) = self.handle.take() {
			handle.join().ok();
		}
	}

	/// Worker thread main loop.
	fn worker_thread(
		rx: Receiver<WorkerRequest>,
		flow_engine: Arc<FlowEngine>,
		engine: StandardEngine,
		catalog: Catalog,
		worker_id: usize,
		num_workers: usize,
	) {
		while let Ok(req) = rx.recv() {
			match req {
				WorkerRequest::ProcessVersionedBatches {
					batches,
					state_version,
					response,
				} => {
					let result = Self::process_versioned_batches_impl(
						&flow_engine,
						&engine,
						&catalog,
						batches,
						state_version,
						worker_id,
						num_workers,
					);

					let resp = match result {
						Ok(pending) => WorkerResponse::Success(pending),
						Err(e) => WorkerResponse::Error(e.to_string()),
					};
					response.send(resp).ok();
				}
				WorkerRequest::Stop => {
					break;
				}
			}
		}
	}

	/// Process versioned batches of changes and return accumulated pending writes.
	///
	/// Each batch contains changes from a single CDC version. This method processes
	/// each version sequentially with correct snapshot isolation, accumulating pending
	/// writes across all versions to maintain state continuity for stateful operators.
	///
	/// Only processes flows assigned to this worker based on hash partitioning.
	fn process_versioned_batches_impl(
		flow_engine: &Arc<FlowEngine>,
		engine: &StandardEngine,
		catalog: &Catalog,
		batches: Vec<Batch>,
		state_version: CommitVersion,
		worker_id: usize,
		num_workers: usize,
	) -> Result<PendingWrites> {
		let mut pending = PendingWrites::new();

		// Process each version group sequentially
		for batch in batches {
			let primitive_version = batch.version;

			// Create query transactions at appropriate versions
			let primitive_query = engine.multi().begin_query_at_version(primitive_version)?;
			let state_query = engine.multi().begin_query_at_version(state_version)?;

			// Create FlowTransaction with accumulated pending from previous versions
			let mut flow_txn = FlowTransaction {
				version: primitive_version,
				pending,
				primitive_query,
				state_query,
				catalog: catalog.clone(),
			};

			// Process all changes at this version
			let flow_ids = flow_engine.flow_ids();
			for flow_id in flow_ids {
				// Only process flows assigned to this worker via hash partitioning
				if (flow_id.0 as usize) % num_workers != worker_id {
					continue;
				}

				for change in &batch.changes {
					if let Err(e) = flow_engine.process(&mut flow_txn, change.clone(), flow_id) {
						error!(flow_id = flow_id.0, error = %e, "failed to process flow");
					}
				}
			}

			// Extract accumulated pending for next iteration
			pending = take(&mut flow_txn.pending);
		}

		Ok(pending)
	}
}

impl Drop for FlowWorker {
	fn drop(&mut self) {
		self.stop();
	}
}
