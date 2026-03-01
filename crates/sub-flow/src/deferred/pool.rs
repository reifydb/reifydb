// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Pool actor that coordinates multiple flow worker actors.
//!
//! This module provides an actor-based implementation of the worker pool:
//! - [`PoolActor`]: Supervises N FlowActors and routes work to them
//! - [`PoolMsg`]: Messages the pool can receive (RegisterFlow, Submit, SubmitToWorker, WorkerReply)
//! - [`PoolResponse`]: Response sent back through callbacks

use std::{collections::HashMap, mem::replace};

use reifydb_core::internal;
use reifydb_rql::flow::flow::FlowDag;
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::ActorConfig,
		traits::{Actor, Directive},
	},
	clock::{Clock, Instant},
};
use reifydb_type::util::hex::encode;
use tracing::{Span, field, instrument};

use super::{
	instruction::WorkerBatch,
	worker::{FlowMsg, FlowResponse},
};
use crate::transaction::pending::{Pending, PendingWrite};

/// Messages for the pool actor
pub enum PoolMsg {
	/// Register a new flow (routes to appropriate worker)
	RegisterFlow {
		flow: FlowDag,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
	},
	/// Submit batches to multiple workers
	Submit {
		batches: HashMap<usize, WorkerBatch>,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
	},
	/// Submit to a specific worker
	SubmitToWorker {
		worker_id: usize,
		batch: WorkerBatch,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
	},
	/// Async reply from a FlowActor worker
	WorkerReply {
		worker_id: usize,
		response: FlowResponse,
	},
}

/// Response from the pool actor
pub enum PoolResponse {
	/// Operation succeeded with pending writes and view changes
	Success {
		pending: Pending,
	},
	/// Registration succeeded
	RegisterSuccess,
	/// Operation failed with error message
	Error(String),
}

/// Phase of the pool actor state machine
enum Phase {
	/// Idle, ready for new work
	Idle,
	/// Waiting for multiple workers to reply
	WaitingForWorkers {
		pending_count: usize,
		results: Vec<Pending>,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
		started_at: Instant,
	},
	/// Waiting for a single worker to reply
	WaitingForSingleWorker {
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
		is_register: bool,
	},
}

/// Pool actor - supervises worker actors and routes work.
pub struct PoolActor {
	refs: Vec<ActorRef<FlowMsg>>,
	clock: Clock,
}

impl PoolActor {
	pub fn new(refs: Vec<ActorRef<FlowMsg>>, clock: Clock) -> Self {
		Self {
			refs,
			clock,
		}
	}
}

/// Actor state
pub struct PoolState {
	phase: Phase,
}

impl Actor for PoolActor {
	type State = PoolState;
	type Message = PoolMsg;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		PoolState {
			phase: Phase::Idle,
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			PoolMsg::RegisterFlow {
				flow,
				reply,
			} => {
				if !matches!(state.phase, Phase::Idle) {
					(reply)(PoolResponse::Error("Pool actor is busy".to_string()));
					return Directive::Continue;
				}

				let flow_id = flow.id;
				let worker_id = (flow_id.0 as usize) % self.refs.len();

				let self_ref = ctx.self_ref().clone();
				let callback: Box<dyn FnOnce(FlowResponse) + Send> = Box::new(move |resp| {
					let _ = self_ref.send(PoolMsg::WorkerReply {
						worker_id,
						response: resp,
					});
				});

				if self.refs[worker_id]
					.send(FlowMsg::Register {
						flow,
						reply: callback,
					})
					.is_err()
				{
					reply(PoolResponse::Error(format!("Worker {} stopped", worker_id)));
					return Directive::Continue;
				}

				state.phase = Phase::WaitingForSingleWorker {
					reply,
					is_register: true,
				};
			}
			PoolMsg::Submit {
				batches,
				reply,
			} => {
				if !matches!(state.phase, Phase::Idle) {
					reply(PoolResponse::Error("Pool actor is busy".to_string()));
					return Directive::Continue;
				}

				self.handle_submit_async(state, ctx, batches, reply);
			}
			PoolMsg::SubmitToWorker {
				worker_id,
				batch,
				reply,
			} => {
				if !matches!(state.phase, Phase::Idle) {
					(reply)(PoolResponse::Error("Pool actor is busy".to_string()));
					return Directive::Continue;
				}

				if worker_id >= self.refs.len() {
					(reply)(PoolResponse::Error(
						internal!("Invalid worker_id: {}", worker_id).to_string(),
					));
					return Directive::Continue;
				}

				let self_ref = ctx.self_ref().clone();
				let callback: Box<dyn FnOnce(FlowResponse) + Send> = Box::new(move |resp| {
					let _ = self_ref.send(PoolMsg::WorkerReply {
						worker_id,
						response: resp,
					});
				});

				if self.refs[worker_id]
					.send(FlowMsg::Process {
						batch,
						reply: callback,
					})
					.is_err()
				{
					reply(PoolResponse::Error(format!("Worker {} stopped", worker_id)));
					return Directive::Continue;
				}

				state.phase = Phase::WaitingForSingleWorker {
					reply,
					is_register: false,
				};
			}
			PoolMsg::WorkerReply {
				worker_id,
				response,
			} => {
				self.handle_worker_reply(state, worker_id, response);
			}
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}

impl PoolActor {
	/// Handle Submit by sending to workers asynchronously.
	#[instrument(name = "flow::pool::submit", level = "debug", skip(self, state, ctx, batches, reply), fields(
		batches = batches.len(),
		instructions = field::Empty,
		elapsed_us = field::Empty
	))]
	fn handle_submit_async(
		&self,
		state: &mut PoolState,
		ctx: &Context<PoolMsg>,
		batches: HashMap<usize, WorkerBatch>,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
	) {
		let start = self.clock.instant();
		let total_instructions: usize = batches.values().map(|b| b.instructions.len()).sum();
		Span::current().record("instructions", total_instructions);

		let batch_count = batches.len();

		for (worker_id, batch) in batches {
			if worker_id >= self.refs.len() {
				(reply)(PoolResponse::Error(internal!("Invalid worker_id: {}", worker_id).to_string()));
				return;
			}

			let self_ref = ctx.self_ref().clone();
			let callback: Box<dyn FnOnce(FlowResponse) + Send> = Box::new(move |resp| {
				let _ = self_ref.send(PoolMsg::WorkerReply {
					worker_id,
					response: resp,
				});
			});

			if self.refs[worker_id]
				.send(FlowMsg::Process {
					batch,
					reply: callback,
				})
				.is_err()
			{
				(reply)(PoolResponse::Error(format!("Worker {} stopped", worker_id)));
				return;
			}
		}

		state.phase = Phase::WaitingForWorkers {
			pending_count: batch_count,
			results: Vec::with_capacity(batch_count),
			reply,
			started_at: start,
		};
	}

	/// Handle a WorkerReply message based on current phase.
	fn handle_worker_reply(&self, state: &mut PoolState, worker_id: usize, response: FlowResponse) {
		let phase = replace(&mut state.phase, Phase::Idle);

		match phase {
			Phase::WaitingForSingleWorker {
				reply: original_reply,
				is_register,
			} => {
				let resp = match response {
					FlowResponse::Success {
						pending,
					} => {
						if is_register {
							PoolResponse::RegisterSuccess
						} else {
							PoolResponse::Success {
								pending,
							}
						}
					}
					FlowResponse::Error(e) => PoolResponse::Error(e),
				};
				(original_reply)(resp);
				// state.phase is already Idle
			}
			Phase::WaitingForWorkers {
				mut pending_count,
				mut results,
				reply: original_reply,
				started_at: start,
			} => {
				match response {
					FlowResponse::Success {
						pending,
					} => {
						results.push(pending);
						pending_count -= 1;

						if pending_count == 0 {
							// All workers done — aggregate and reply
							match self.aggregate_pending_writes(results) {
								Ok(combined) => {
									Span::current().record(
										"elapsed_us",
										start.elapsed().as_micros() as u64,
									);
									(original_reply)(PoolResponse::Success {
										pending: combined,
									});
								}
								Err(e) => {
									(original_reply)(PoolResponse::Error(e));
								}
							}
							// state.phase is already Idle
						} else {
							// Still waiting for more workers
							state.phase = Phase::WaitingForWorkers {
								pending_count,
								results,
								reply: original_reply,
								started_at: start,
							};
						}
					}
					FlowResponse::Error(e) => {
						// On first error, reply immediately with error
						(original_reply)(PoolResponse::Error(format!(
							"Worker {} error: {}",
							worker_id, e
						)));
						// state.phase is already Idle — remaining replies will hit the Idle
						// branch below
					}
				}
			}
			Phase::Idle => {
				// Stale reply from a previous errored batch — ignore
			}
		}
	}

	/// Aggregate Pending from multiple workers with keyspace overlap detection.
	fn aggregate_pending_writes(&self, writes: Vec<Pending>) -> Result<Pending, String> {
		let mut combined = Pending::new();

		for mut pending in writes {
			combined.extend_view_changes(pending.take_view_changes());
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
					PendingWrite::Set(v) => {
						combined.insert(key.clone(), v.clone());
					}
					PendingWrite::Remove => {
						combined.remove(key.clone());
					}
				}
			}
		}

		Ok(combined)
	}
}
