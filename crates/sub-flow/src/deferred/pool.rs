// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Pool actor that coordinates multiple flow worker actors.
//!
//! This module provides an actor-based implementation of the worker pool:
//! - [`PoolActor`]: Supervises N FlowActors and routes work to them
//! - [`FlowPoolMessage`]: Messages the pool can receive (RegisterFlow, Submit, SubmitToWorker, WorkerReply)
//! - [`PoolResponse`]: Response sent back through callbacks

use std::{collections::BTreeMap, mem::replace};

use reifydb_core::{
	actors::{
		flow::{FlowMessage, FlowPoolMessage, FlowResponse, PoolResponse, WorkerBatch},
		pending::{Pending, PendingWrite},
	},
	common::CommitVersion,
	encoded::shape::RowShape,
	interface::{catalog::flow::FlowId, change::Change},
	internal,
};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::ActorConfig,
		traits::{Actor, Directive},
	},
	context::clock::{Clock, Instant},
};
use reifydb_type::{util::hex::encode, value::datetime::DateTime};
use tracing::{Span, field, instrument};

/// Phase of the pool actor state machine
enum Phase {
	/// Idle, ready for new work
	Idle,
	/// Waiting for multiple workers to reply
	WaitingForWorkers {
		pending_count: usize,
		results: Vec<Pending>,
		pending_shapes: Vec<RowShape>,
		view_changes: Vec<Change>,
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
	refs: Vec<ActorRef<FlowMessage>>,
	clock: Clock,
}

impl PoolActor {
	pub fn new(refs: Vec<ActorRef<FlowMessage>>, clock: Clock) -> Self {
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
	type Message = FlowPoolMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		PoolState {
			phase: Phase::Idle,
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			FlowPoolMessage::RegisterFlow {
				flow_id,
				reply,
			} => {
				if !matches!(state.phase, Phase::Idle) {
					(reply)(PoolResponse::Error("Pool actor is busy".to_string()));
					return Directive::Continue;
				}

				let worker_id = (flow_id.0 as usize) % self.refs.len();

				let self_ref = ctx.self_ref().clone();
				let callback: Box<dyn FnOnce(FlowResponse) + Send> = Box::new(move |resp| {
					let _ = self_ref.send(FlowPoolMessage::WorkerReply {
						worker_id,
						response: resp,
					});
				});

				if self.refs[worker_id]
					.send(FlowMessage::Register {
						flow_id,
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
			FlowPoolMessage::Submit {
				batches,
				reply,
			} => {
				if !matches!(state.phase, Phase::Idle) {
					reply(PoolResponse::Error("Pool actor is busy".to_string()));
					return Directive::Continue;
				}

				self.handle_submit_async(state, ctx, batches, reply);
			}
			FlowPoolMessage::SubmitToWorker {
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
					let _ = self_ref.send(FlowPoolMessage::WorkerReply {
						worker_id,
						response: resp,
					});
				});

				if self.refs[worker_id]
					.send(FlowMessage::Process {
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
			FlowPoolMessage::Rebalance {
				assignments,
				reply,
			} => {
				if !matches!(state.phase, Phase::Idle) {
					(reply)(PoolResponse::Error("Pool actor is busy".to_string()));
					return Directive::Continue;
				}

				self.handle_rebalance_async(state, ctx, assignments, reply);
			}
			FlowPoolMessage::Tick {
				ticks,
				timestamp,
				state_version,
				reply,
			} => {
				if !matches!(state.phase, Phase::Idle) {
					(reply)(PoolResponse::Error("Pool actor is busy".to_string()));
					return Directive::Continue;
				}

				self.handle_tick_async(state, ctx, ticks, timestamp, state_version, reply);
			}
			FlowPoolMessage::WorkerReply {
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
		ctx: &Context<FlowPoolMessage>,
		batches: BTreeMap<usize, WorkerBatch>,
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
				let _ = self_ref.send(FlowPoolMessage::WorkerReply {
					worker_id,
					response: resp,
				});
			});

			if self.refs[worker_id]
				.send(FlowMessage::Process {
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
			pending_shapes: Vec::new(),
			view_changes: Vec::new(),
			reply,
			started_at: start,
		};
	}

	/// Handle Rebalance by clearing all workers and re-registering assigned flows.
	fn handle_rebalance_async(
		&self,
		state: &mut PoolState,
		ctx: &Context<FlowPoolMessage>,
		assignments: BTreeMap<usize, Vec<FlowId>>,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
	) {
		let worker_count = self.refs.len();

		// Send Rebalance to every worker. Workers not in the assignments map
		// receive an empty list (which just clears them).
		for worker_id in 0..worker_count {
			let flow_ids = assignments.get(&worker_id).cloned().unwrap_or_default();

			let self_ref = ctx.self_ref().clone();
			let callback: Box<dyn FnOnce(FlowResponse) + Send> = Box::new(move |resp| {
				let _ = self_ref.send(FlowPoolMessage::WorkerReply {
					worker_id,
					response: resp,
				});
			});

			if self.refs[worker_id]
				.send(FlowMessage::Rebalance {
					flow_ids,
					reply: callback,
				})
				.is_err()
			{
				(reply)(PoolResponse::Error(format!("Worker {} stopped", worker_id)));
				return;
			}
		}

		state.phase = Phase::WaitingForWorkers {
			pending_count: worker_count,
			results: Vec::new(),
			pending_shapes: Vec::new(),
			view_changes: Vec::new(),
			reply,
			started_at: self.clock.instant(),
		};
	}

	/// Handle Tick by sending to workers asynchronously.
	fn handle_tick_async(
		&self,
		state: &mut PoolState,
		ctx: &Context<FlowPoolMessage>,
		ticks: BTreeMap<usize, Vec<FlowId>>,
		timestamp: DateTime,
		state_version: CommitVersion,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
	) {
		let tick_count = ticks.len();

		for (worker_id, flow_ids) in ticks {
			if worker_id >= self.refs.len() {
				(reply)(PoolResponse::Error(internal!("Invalid worker_id: {}", worker_id).to_string()));
				return;
			}

			let self_ref = ctx.self_ref().clone();
			let callback: Box<dyn FnOnce(FlowResponse) + Send> = Box::new(move |resp| {
				let _ = self_ref.send(FlowPoolMessage::WorkerReply {
					worker_id,
					response: resp,
				});
			});

			if self.refs[worker_id]
				.send(FlowMessage::Tick {
					flow_ids,
					timestamp,
					state_version,
					reply: callback,
				})
				.is_err()
			{
				(reply)(PoolResponse::Error(format!("Worker {} stopped", worker_id)));
				return;
			}
		}

		state.phase = Phase::WaitingForWorkers {
			pending_count: tick_count,
			results: Vec::with_capacity(tick_count),
			pending_shapes: Vec::new(),
			view_changes: Vec::new(),
			reply,
			started_at: self.clock.instant(),
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
						pending_shapes,
						view_changes,
					} => {
						if is_register {
							PoolResponse::RegisterSuccess
						} else {
							PoolResponse::Success {
								pending,
								pending_shapes,
								view_changes,
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
				mut pending_shapes,
				mut view_changes,
				reply: original_reply,
				started_at: start,
			} => {
				match response {
					FlowResponse::Success {
						pending,
						pending_shapes: worker_pending_shapes,
						view_changes: worker_view_changes,
					} => {
						results.push(pending);
						pending_shapes.extend(worker_pending_shapes);
						view_changes.extend(worker_view_changes);
						pending_count -= 1;

						if pending_count == 0 {
							// All workers done - aggregate and reply
							match self.aggregate_pending_writes(results) {
								Ok(combined) => {
									Span::current().record(
										"elapsed_us",
										start.elapsed().as_micros() as u64,
									);
									(original_reply)(PoolResponse::Success {
										pending: combined,
										pending_shapes,
										view_changes,
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
								pending_shapes,
								view_changes,
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
						// state.phase is already Idle - remaining replies will hit the Idle
						// branch below
					}
				}
			}
			Phase::Idle => {
				// Stale reply from a previous errored batch - ignore
			}
		}
	}

	/// Aggregate Pending from multiple workers with keyspace overlap detection.
	fn aggregate_pending_writes(&self, writes: Vec<Pending>) -> Result<Pending, String> {
		let mut combined = Pending::new();

		for pending in writes {
			for (key, value) in pending.iter_sorted() {
				// Validate no keyspace overlap between workers
				if combined.contains_key(key) {
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
