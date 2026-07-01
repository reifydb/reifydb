// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	mem::replace,
	panic::{AssertUnwindSafe, catch_unwind},
	process,
	sync::Arc,
};

use reifydb_core::{
	actors::{
		flow::{FlowMessage, FlowPoolMessage, FlowResponse, PoolResponse, WorkerBatch},
		pending::{Pending, PendingWrite},
	},
	common::CommitVersion,
	encoded::shape::RowShape,
	interface::{
		catalog::{flow::FlowId, shape::ShapeId},
		change::Change,
	},
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
use reifydb_value::{error::Error, util::hex::encode, value::datetime::DateTime};
use tracing::{Span, error, field, instrument};

use crate::error::FlowDispatchError;

enum Phase {
	Idle,

	WaitingForWorkers {
		pending_count: usize,
		results: Vec<Pending>,
		pending_shapes: Vec<RowShape>,
		view_changes: Vec<Change>,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
		started_at: Instant,
	},

	WaitingForSingleWorker {
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
		is_register: bool,
	},
}

#[inline]
fn reject_if_busy(
	state: &PoolState,
	reply: Box<dyn FnOnce(PoolResponse) + Send>,
) -> Option<Box<dyn FnOnce(PoolResponse) + Send>> {
	if matches!(state.phase, Phase::Idle) {
		Some(reply)
	} else {
		(reply)(PoolResponse::Error(FlowDispatchError::PoolBusy.into()));
		None
	}
}

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
		catch_unwind(AssertUnwindSafe(|| {
			match msg {
				FlowPoolMessage::RegisterFlow {
					flow_id,
					reply,
				} => {
					let Some(reply) = reject_if_busy(state, reply) else {
						return Directive::Continue;
					};
					self.handle_register_flow(state, ctx, flow_id, reply);
				}
				FlowPoolMessage::Submit {
					batches,
					reply,
				} => {
					let Some(reply) = reject_if_busy(state, reply) else {
						return Directive::Continue;
					};
					self.handle_submit_async(state, ctx, batches, reply);
				}
				FlowPoolMessage::Broadcast {
					state_version,
					to_version,
					changes,
					index,
					active,
					reply,
				} => {
					let Some(reply) = reject_if_busy(state, reply) else {
						return Directive::Continue;
					};
					self.handle_broadcast(
						state,
						ctx,
						state_version,
						to_version,
						changes,
						index,
						active,
						reply,
					);
				}
				FlowPoolMessage::SubmitToWorker {
					worker_id,
					batch,
					reply,
				} => {
					let Some(reply) = reject_if_busy(state, reply) else {
						return Directive::Continue;
					};
					self.handle_submit_to_worker(state, ctx, worker_id, batch, reply);
				}
				FlowPoolMessage::Rebalance {
					assignments,
					reply,
				} => {
					let Some(reply) = reject_if_busy(state, reply) else {
						return Directive::Continue;
					};
					self.handle_rebalance_async(state, ctx, assignments, reply);
				}
				FlowPoolMessage::Tick {
					ticks,
					timestamp,
					state_version,
					reply,
				} => {
					let Some(reply) = reject_if_busy(state, reply) else {
						return Directive::Continue;
					};
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
		}))
		.unwrap_or_else(|_| {
			error!("panic in flow pool actor, aborting");
			process::abort()
		})
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}

impl PoolActor {
	#[inline]
	fn handle_register_flow(
		&self,
		state: &mut PoolState,
		ctx: &Context<FlowPoolMessage>,
		flow_id: FlowId,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
	) {
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
			reply(PoolResponse::Error(
				FlowDispatchError::WorkerStopped {
					worker_id,
				}
				.into(),
			));
			return;
		}

		state.phase = Phase::WaitingForSingleWorker {
			reply,
			is_register: true,
		};
	}

	#[inline]
	fn handle_submit_to_worker(
		&self,
		state: &mut PoolState,
		ctx: &Context<FlowPoolMessage>,
		worker_id: usize,
		batch: WorkerBatch,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
	) {
		if worker_id >= self.refs.len() {
			(reply)(PoolResponse::Error(
				FlowDispatchError::InvalidWorkerId {
					worker_id,
					num_workers: self.refs.len(),
				}
				.into(),
			));
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
			reply(PoolResponse::Error(
				FlowDispatchError::WorkerStopped {
					worker_id,
				}
				.into(),
			));
			return;
		}

		state.phase = Phase::WaitingForSingleWorker {
			reply,
			is_register: false,
		};
	}

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
				(reply)(PoolResponse::Error(
					FlowDispatchError::InvalidWorkerId {
						worker_id,
						num_workers: self.refs.len(),
					}
					.into(),
				));
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
				(reply)(PoolResponse::Error(
					FlowDispatchError::WorkerStopped {
						worker_id,
					}
					.into(),
				));
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

	#[allow(clippy::too_many_arguments)]
	fn handle_broadcast(
		&self,
		state: &mut PoolState,
		ctx: &Context<FlowPoolMessage>,
		state_version: CommitVersion,
		to_version: CommitVersion,
		changes: Arc<Vec<Change>>,
		index: Arc<HashMap<ShapeId, Vec<FlowId>>>,
		active: Arc<Vec<FlowId>>,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
	) {
		let start = self.clock.instant();
		let worker_count = self.refs.len();

		for worker_id in 0..worker_count {
			let self_ref = ctx.self_ref().clone();
			let callback: Box<dyn FnOnce(FlowResponse) + Send> = Box::new(move |resp| {
				let _ = self_ref.send(FlowPoolMessage::WorkerReply {
					worker_id,
					response: resp,
				});
			});

			if self.refs[worker_id]
				.send(FlowMessage::Dispatch {
					state_version,
					to_version,
					changes: changes.clone(),
					index: index.clone(),
					active: active.clone(),
					reply: callback,
				})
				.is_err()
			{
				(reply)(PoolResponse::Error(
					FlowDispatchError::WorkerStopped {
						worker_id,
					}
					.into(),
				));
				return;
			}
		}

		state.phase = Phase::WaitingForWorkers {
			pending_count: worker_count,
			results: Vec::with_capacity(worker_count),
			pending_shapes: Vec::new(),
			view_changes: Vec::new(),
			reply,
			started_at: start,
		};
	}

	fn handle_rebalance_async(
		&self,
		state: &mut PoolState,
		ctx: &Context<FlowPoolMessage>,
		assignments: BTreeMap<usize, Vec<FlowId>>,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
	) {
		let worker_count = self.refs.len();

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
				(reply)(PoolResponse::Error(
					FlowDispatchError::WorkerStopped {
						worker_id,
					}
					.into(),
				));
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
				(reply)(PoolResponse::Error(
					FlowDispatchError::InvalidWorkerId {
						worker_id,
						num_workers: self.refs.len(),
					}
					.into(),
				));
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
				(reply)(PoolResponse::Error(
					FlowDispatchError::WorkerStopped {
						worker_id,
					}
					.into(),
				));
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

	fn handle_worker_reply(&self, state: &mut PoolState, worker_id: usize, response: FlowResponse) {
		let phase = replace(&mut state.phase, Phase::Idle);
		match phase {
			Phase::WaitingForSingleWorker {
				reply,
				is_register,
			} => self.complete_single_worker_reply(reply, is_register, response),
			Phase::WaitingForWorkers {
				pending_count,
				results,
				pending_shapes,
				view_changes,
				reply,
				started_at,
			} => self.aggregate_worker_reply(
				state,
				worker_id,
				response,
				pending_count,
				results,
				pending_shapes,
				view_changes,
				reply,
				started_at,
			),
			Phase::Idle => {}
		}
	}

	#[inline]
	fn complete_single_worker_reply(
		&self,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
		is_register: bool,
		response: FlowResponse,
	) {
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
		(reply)(resp);
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn aggregate_worker_reply(
		&self,
		state: &mut PoolState,
		worker_id: usize,
		response: FlowResponse,
		mut pending_count: usize,
		mut results: Vec<Pending>,
		mut pending_shapes: Vec<RowShape>,
		mut view_changes: Vec<Change>,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
		started_at: Instant,
	) {
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
					match self.aggregate_pending_writes(results) {
						Ok(combined) => {
							Span::current().record(
								"elapsed_us",
								started_at.elapsed().as_micros() as u64,
							);
							(reply)(PoolResponse::Success {
								pending: combined,
								pending_shapes,
								view_changes,
							});
						}
						Err(e) => {
							(reply)(PoolResponse::Error(e));
						}
					}
				} else {
					state.phase = Phase::WaitingForWorkers {
						pending_count,
						results,
						pending_shapes,
						view_changes,
						reply,
						started_at,
					};
				}
			}
			FlowResponse::Error(e) => {
				(reply)(PoolResponse::Error(
					FlowDispatchError::WorkerFailed {
						worker_id,
						cause: e,
					}
					.into(),
				));
			}
		}
	}

	fn aggregate_pending_writes(&self, writes: Vec<Pending>) -> Result<Pending, Error> {
		let mut combined = Pending::new();

		for pending in writes {
			for (key, value) in pending.iter_sorted() {
				if combined.contains_key(key) {
					return Err(FlowDispatchError::KeyspaceOverlap {
						key: encode(key.as_ref()),
					}
					.into());
				}

				match value {
					PendingWrite::Set(v) => {
						combined.insert(key.clone(), v.clone());
					}
					PendingWrite::Remove => {
						combined.remove(key.clone());
					}
					PendingWrite::Drop => {
						combined.drop_key(key.clone());
					}
				}
			}
		}

		Ok(combined)
	}
}
