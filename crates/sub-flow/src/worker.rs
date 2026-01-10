// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Flow worker that handles flow processing logic.

use std::{
	collections::HashMap,
	mem::take,
	thread::{JoinHandle, spawn},
};
use crate::{
	FlowEngine,
	transaction::{FlowTransaction, PendingWrites},
};
use WorkerRequest::Process;
use crossbeam_channel::{Receiver, Sender, bounded};
use reifydb_catalog::Catalog;
use reifydb_cdc::CdcCheckpoint;
use reifydb_core::{CommitVersion, Error, Result, interface::FlowId};
use reifydb_engine::StandardEngine;
use reifydb_rql::flow::FlowDag;
use reifydb_sdk::FlowChange;
use reifydb_type::internal;
use tracing::error;

/// A batch of changes that all belong to the same CDC version.
#[derive(Clone)]
pub(crate) struct Batch {
	pub version: CommitVersion,
	pub changes: Vec<FlowChange>,
}

/// Message types for worker communication.
enum WorkerRequest {
	Process {
		batches: Vec<Batch>,
		state_version: CommitVersion,
		response: Sender<WorkerResponse>,
	},
	Register {
		flow: FlowDag,
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
	pub fn new<F>(
		worker_id: usize,
		num_workers: usize,
		engine_factory: F,
		engine: StandardEngine,
		catalog: Catalog,
	) -> Self
	where
		F: FnOnce() -> FlowEngine + Send + 'static,
	{
		let (tx, rx) = crossbeam_channel::unbounded();

		let thread_handle = spawn(move || {
			// Create FlowEngine INSIDE worker thread (Rc is thread-local)
			let flow_engine = engine_factory();
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
		let (response, rx) = bounded(1);

		self.tx.send(Process {
			batches,
			state_version,
			response,
		})
		.map_err(|_| Error(internal!("Worker thread died")))?;

		match rx.recv().map_err(|_| Error(internal!("Worker response error")))? {
			WorkerResponse::Success(pending) => Ok(pending),
			WorkerResponse::Error(e) => Err(Error(internal!("{}", e))),
		}
	}

	/// Register a flow in this worker's FlowEngine.
	pub fn register_flow(&self, flow: FlowDag) -> Result<()> {
		let (resp_tx, resp_rx) = bounded(1);

		self.tx.send(WorkerRequest::Register {
			flow,
			response: resp_tx,
		})
		.map_err(|_| Error(internal!("Worker thread died")))?;

		match resp_rx.recv().map_err(|_| Error(internal!("Worker response error")))? {
			WorkerResponse::Success(_) => Ok(()),
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
		mut flow_engine: FlowEngine,
		engine: StandardEngine,
		catalog: Catalog,
		worker_id: usize,
		num_workers: usize,
	) {
		while let Ok(req) = rx.recv() {
			match req {
				Process {
					batches,
					state_version,
					response,
				} => {
					let result = Self::process_request(
						&mut flow_engine,
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
				WorkerRequest::Register {
					flow,
					response,
				} => {
					let result = engine
						.begin_command()
						.and_then(|mut txn| flow_engine.register(&mut txn, flow));

					let resp = match result {
						Ok(_) => WorkerResponse::Success(PendingWrites::new()),
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

	fn process_request(
		flow_engine: &mut FlowEngine,
		engine: &StandardEngine,
		catalog: &Catalog,
		batches: Vec<Batch>,
		state_version: CommitVersion,
		worker_id: usize,
		num_workers: usize,
	) -> Result<PendingWrites> {
		let mut pending = PendingWrites::new();

		// Load checkpoints for all flows this worker manages
		let mut flow_checkpoints: HashMap<FlowId, CommitVersion> = HashMap::new();
		{
			let mut query_txn = engine.begin_query()?;
			for flow_id in flow_engine.flow_ids() {
				let checkpoint = CdcCheckpoint::fetch(&mut query_txn, &flow_id)
					.unwrap_or(CommitVersion(0));
				flow_checkpoints.insert(flow_id, checkpoint);
			}
		}

		// Process each version group sequentially
		for batch in batches {
			let primitive_version = batch.version;

			let primitive_query = engine.multi().begin_query_at_version(primitive_version)?;
			let state_query = engine.multi().begin_query_at_version(state_version)?;

			let mut txn = FlowTransaction {
				version: primitive_version,
				pending,
				primitive_query,
				state_query,
				catalog: catalog.clone(),
			};

			for flow_id in flow_engine.flow_ids() {
				if (flow_id.0 as usize) % num_workers != worker_id {
					continue;
				}

				// Skip this batch if flow has already processed it
				let checkpoint = flow_checkpoints.get(&flow_id).copied().unwrap_or(CommitVersion(0));
				if batch.version <= checkpoint {
					println!(
						"[WORKER DEBUG] Skipping batch v{} for flow {} (checkpoint={})",
						batch.version.0,
						flow_id.0,
						checkpoint.0
					);
					continue;
				}

				println!(
					"[WORKER DEBUG] Processing batch v{} for flow {} (checkpoint={})",
					batch.version.0,
					flow_id.0,
					checkpoint.0
				);

				for change in &batch.changes {
					if let Err(e) = flow_engine.process(&mut txn, change.clone(), flow_id) {
						error!(flow_id = flow_id.0, error = %e, "failed to process flow");
					}
				}
			}

			// Extract accumulated pending for next iteration
			pending = take(&mut txn.pending);
		}

		Ok(pending)
	}
}

impl Drop for FlowWorker {
	fn drop(&mut self) {
		self.stop();
	}
}
