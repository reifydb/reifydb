// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Pool actor that coordinates multiple flow worker actors.
//!
//! This module provides an actor-based implementation of the worker pool:
//! - [`PoolActor`]: Supervises N FlowActors and routes work to them
//! - [`PoolMsg`]: Messages the pool can receive (RegisterFlow, Submit, SubmitToWorker)
//! - [`PoolResponse`]: Response sent back through reply channels

use std::collections::HashMap;

use crossbeam_channel::{Sender, bounded};
use reifydb_core::internal;
use reifydb_rql::flow::flow::FlowDag;
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::config::ActorConfig,
		traits::{Actor, Flow},
	},
	clock::Clock,
};
use reifydb_type::util::hex::encode;
use tracing::{Span, instrument};

use crate::{
	actor::{FlowMsg, FlowResponse},
	instruction::WorkerBatch,
	transaction::pending::{Pending, PendingWrites},
};

/// Messages for the pool actor
pub enum PoolMsg {
	/// Register a new flow (routes to appropriate worker)
	RegisterFlow {
		flow: FlowDag,
		reply: Sender<PoolResponse>,
	},
	/// Submit batches to multiple workers
	Submit {
		batches: HashMap<usize, WorkerBatch>,
		reply: Sender<PoolResponse>,
	},
	/// Submit to a specific worker
	SubmitToWorker {
		worker_id: usize,
		batch: WorkerBatch,
		reply: Sender<PoolResponse>,
	},
}

/// Response from the pool actor
pub enum PoolResponse {
	/// Operation succeeded with pending writes
	Success(PendingWrites),
	/// Registration succeeded
	RegisterSuccess,
	/// Operation failed with error message
	Error(String),
}

/// Pool actor - supervises worker actors and routes work.
pub struct PoolActor {
	worker_refs: Vec<ActorRef<FlowMsg>>,
	clock: Clock,
}

impl PoolActor {
	pub fn new(worker_refs: Vec<ActorRef<FlowMsg>>, clock: Clock) -> Self {
		Self {
			worker_refs,
			clock,
		}
	}
}

/// Actor state - minimal since worker refs are in the actor itself
pub struct PoolState;

impl Actor for PoolActor {
	type State = PoolState;
	type Message = PoolMsg;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		PoolState
	}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Flow {
		match msg {
			PoolMsg::RegisterFlow {
				flow,
				reply,
			} => {
				let resp = self.handle_register_flow(flow);
				let _ = reply.send(resp);
			}
			PoolMsg::Submit {
				batches,
				reply,
			} => {
				let resp = self.handle_submit(batches);
				let _ = reply.send(resp);
			}
			PoolMsg::SubmitToWorker {
				worker_id,
				batch,
				reply,
			} => {
				let resp = self.handle_submit_to_worker(worker_id, batch);
				let _ = reply.send(resp);
			}
		}
		Flow::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(0) // unbounded
	}
}

impl PoolActor {
	/// Handle RegisterFlow by routing to the appropriate worker.
	fn handle_register_flow(&self, flow: FlowDag) -> PoolResponse {
		let flow_id = flow.id;
		let worker_id = (flow_id.0 as usize) % self.worker_refs.len();

		let (reply_tx, reply_rx) = bounded(1);

		if self.worker_refs[worker_id]
			.send(FlowMsg::Register {
				flow,
				reply: reply_tx,
			})
			.is_err()
		{
			return PoolResponse::Error(format!("Worker {} stopped", worker_id));
		}

		match reply_rx.recv() {
			Ok(FlowResponse::Success(_)) => PoolResponse::RegisterSuccess,
			Ok(FlowResponse::Error(e)) => PoolResponse::Error(e),
			Err(_) => PoolResponse::Error(format!("Worker {} response error", worker_id)),
		}
	}

	/// Handle Submit by sending to workers and aggregating results.
	#[instrument(name = "flow::pool_actor::submit", level = "debug", skip(self, batches), fields(
		batches = batches.len(),
		instructions = tracing::field::Empty,
		elapsed_us = tracing::field::Empty
	))]
	fn handle_submit(&self, batches: HashMap<usize, WorkerBatch>) -> PoolResponse {
		let start = self.clock.instant();
		let total_instructions: usize = batches.values().map(|b| b.instructions.len()).sum();
		Span::current().record("instructions", total_instructions);

		// Send to workers and collect reply channels
		let mut pending_replies: Vec<(usize, crossbeam_channel::Receiver<FlowResponse>)> =
			Vec::with_capacity(batches.len());

		for (worker_id, batch) in batches {
			if worker_id >= self.worker_refs.len() {
				return PoolResponse::Error(internal!("Invalid worker_id: {}", worker_id).to_string());
			}

			let (reply_tx, reply_rx) = bounded(1);

			if self.worker_refs[worker_id]
				.send(FlowMsg::Process {
					batch,
					reply: reply_tx,
				})
				.is_err()
			{
				return PoolResponse::Error(format!("Worker {} stopped", worker_id));
			}

			pending_replies.push((worker_id, reply_rx));
		}

		// Collect all results
		let mut results = Vec::with_capacity(pending_replies.len());
		for (worker_id, reply_rx) in pending_replies {
			match reply_rx.recv() {
				Ok(FlowResponse::Success(pending)) => results.push(pending),
				Ok(FlowResponse::Error(e)) => {
					return PoolResponse::Error(format!("Worker {} error: {}", worker_id, e));
				}
				Err(_) => return PoolResponse::Error(format!("Worker {} response error", worker_id)),
			}
		}

		// Aggregate results
		match self.aggregate_pending_writes(results) {
			Ok(combined) => {
				Span::current().record("elapsed_us", start.elapsed().as_micros() as u64);
				PoolResponse::Success(combined)
			}
			Err(e) => PoolResponse::Error(e),
		}
	}

	/// Handle SubmitToWorker by forwarding to specific worker.
	fn handle_submit_to_worker(&self, worker_id: usize, batch: WorkerBatch) -> PoolResponse {
		if worker_id >= self.worker_refs.len() {
			return PoolResponse::Error(internal!("Invalid worker_id: {}", worker_id).to_string());
		}

		let (reply_tx, reply_rx) = bounded(1);

		if self.worker_refs[worker_id]
			.send(FlowMsg::Process {
				batch,
				reply: reply_tx,
			})
			.is_err()
		{
			return PoolResponse::Error(format!("Worker {} stopped", worker_id));
		}

		match reply_rx.recv() {
			Ok(FlowResponse::Success(pending)) => PoolResponse::Success(pending),
			Ok(FlowResponse::Error(e)) => PoolResponse::Error(e),
			Err(_) => PoolResponse::Error(format!("Worker {} response error", worker_id)),
		}
	}

	/// Aggregate PendingWrites from multiple workers with keyspace overlap detection.
	fn aggregate_pending_writes(&self, writes: Vec<PendingWrites>) -> Result<PendingWrites, String> {
		let mut combined = PendingWrites::new();

		for pending in writes {
			for (key, value) in pending.iter_sorted() {
				// Validate no keyspace overlap between workers
				if combined.contains_key(&key) {
					return Err(internal!(
						"keyspace overlap detected during worker aggregation: {}",
						encode(key.as_ref())
					)
					.to_string());
				}

				// Safe to merge - disjoint keyspaces
				match value {
					Pending::Set(v) => {
						combined.insert(key.clone(), v.clone());
					}
					Pending::Remove => {
						combined.remove(key.clone());
					}
				}
			}
		}

		Ok(combined)
	}
}
