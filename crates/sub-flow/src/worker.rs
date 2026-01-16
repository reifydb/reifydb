// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Flow worker that handles flow processing logic.

use std::{
	mem::take,
	thread::{JoinHandle, spawn},
};

use WorkerRequest::Process;
use crossbeam_channel::{Receiver, Sender, bounded};
use reifydb_catalog::catalog::Catalog;
use reifydb_engine::engine::StandardEngine;
use reifydb_rql::flow::flow::FlowDag;
use reifydb_type::{Result, error::Error, internal};
use tracing::{Span, error, instrument};

use crate::{
	FlowEngine,
	instruction::WorkerBatch,
	transaction::{FlowTransaction, pending::PendingWrites},
};

/// Message types for worker communication.
enum WorkerRequest {
	Process {
		batch: WorkerBatch,
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
	pub fn new<F>(engine_factory: F, engine: StandardEngine, catalog: Catalog) -> Self
	where
		F: FnOnce() -> FlowEngine + Send + 'static,
	{
		let (tx, rx) = crossbeam_channel::unbounded();

		let thread_handle = spawn(move || {
			let flow_engine = engine_factory();
			Self::worker_thread(rx, flow_engine, engine, catalog);
		});

		Self {
			tx,
			handle: Some(thread_handle),
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

	/// Process a targeted batch with flow-specific instructions.
	///
	/// Unlike `process`, this method receives pre-filtered changes for specific flows,
	/// eliminating the need for worker-side filtering.
	pub fn process(&self, batch: WorkerBatch) -> Result<PendingWrites> {
		let (response, rx) = bounded(1);

		self.tx.send(Process {
			batch,
			response,
		})
		.map_err(|_| Error(internal!("Worker thread died")))?;

		match rx.recv().map_err(|_| Error(internal!("Worker response error")))? {
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
		mut flow_engine: FlowEngine,
		engine: StandardEngine,
		catalog: Catalog,
	) {
		while let Ok(req) = rx.recv() {
			match req {
				Process {
					batch,
					response,
				} => {
					let result = Self::process_request(&mut flow_engine, &engine, &catalog, batch);

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

	#[instrument(name = "flow::worker::process", level = "debug", skip(flow_engine, engine, catalog, batch), fields(
		instructions = batch.instructions.len(),
		total_changes = tracing::field::Empty
	))]
	fn process_request(
		flow_engine: &mut FlowEngine,
		engine: &StandardEngine,
		catalog: &Catalog,
		batch: WorkerBatch,
	) -> Result<PendingWrites> {
		let total_changes: usize = batch.instructions.iter().map(|i| i.changes.len()).sum();
		Span::current().record("total_changes", total_changes);

		let mut pending = PendingWrites::new();

		// Process each flow instruction
		for instruction in batch.instructions {
			let flow_id = instruction.flow_id;

			// Skip if instruction has no changes
			if instruction.changes.is_empty() {
				continue;
			}

			// Use the to_version as the primitive version for snapshot isolation
			let primitive_version = instruction.to_version;

			let primitive_query = engine.multi().begin_query_at_version(primitive_version)?;
			let state_query = engine.multi().begin_query_at_version(batch.state_version)?;

			let mut txn = FlowTransaction {
				version: primitive_version,
				pending,
				primitive_query,
				state_query,
				catalog: catalog.clone(),
			};

			// Process all changes for this flow
			for change in &instruction.changes {
				if let Err(e) = flow_engine.process(&mut txn, change.clone(), flow_id) {
					error!(flow_id = flow_id.0, error = %e, "failed to process flow");
				}
			}

			// Extract accumulated pending for next instruction
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
