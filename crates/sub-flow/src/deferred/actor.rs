// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeSet,
	panic::{AssertUnwindSafe, catch_unwind},
	process,
	sync::Arc,
};

use reifydb_cdc::storage::CdcStore;
use reifydb_codec::encoded::shape::RowShape;
use reifydb_core::{
	actors::{flow::FlowActorMessage, pending::Pending},
	common::CommitVersion,
	interface::{
		WithEventBus,
		catalog::{
			config::{ConfigKey, GetConfig},
			flow::FlowId,
			shape::ShapeId,
		},
		cdc::Cdc,
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_rql::flow::flow::FlowDag;
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::ActorConfig,
		traits::{Actor, Directive},
	},
	context::{RuntimeContext, clock::Clock},
};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration, identity::IdentityId},
};
use tracing::{error, warn};

use crate::{
	builder::CustomOperators,
	deferred::{
		committer::{CommitterMessage, FlowSlice, SliceCommitReply, TickCommitReply},
		health::FlowHealthRegistry,
		overlay::FlowWriteOverlay,
		slice::{SliceComputer, SliceConfig, SliceCursor, SliceStep},
		tracker::FlowPositionTracker,
	},
	engine::FlowEngineInner,
	transaction::allocators::FlowAllocators,
};

pub struct FlowActorParams {
	pub engine: StandardEngine,
	pub committer: ActorRef<CommitterMessage>,
	pub cdc_store: CdcStore,
	pub custom_operators: CustomOperators,
	pub allocators: FlowAllocators,
	pub clock: Clock,
	pub health: FlowHealthRegistry,
	pub flow_tracker: FlowPositionTracker,
	pub flow: FlowDag,
	pub source_shapes: Arc<BTreeSet<ShapeId>>,
	pub cursor: CommitVersion,
	pub chunk_size: u64,
	pub checkpoint_lag: u64,
	pub retry_limit: u32,
	pub retry_backoff: Duration,
}

pub struct FlowActor {
	engine: StandardEngine,
	committer: ActorRef<CommitterMessage>,
	cdc_store: CdcStore,
	custom_operators: CustomOperators,
	allocators: FlowAllocators,
	clock: Clock,
	health: FlowHealthRegistry,
	flow_tracker: FlowPositionTracker,
	flow: FlowDag,
	flow_id: FlowId,
	ticks_enabled: bool,
	computer: SliceComputer,
	config: SliceConfig,
	retry_limit: u32,
	retry_backoff: Duration,
	initial_source_shapes: Arc<BTreeSet<ShapeId>>,
	initial_cursor: CommitVersion,
}

pub struct FlowActorState {
	flow_engine: FlowEngineInner,
	source_shapes: Arc<BTreeSet<ShapeId>>,
	cursor: CommitVersion,
	durable_cursor: CommitVersion,
	committing: bool,
	wake_pending: bool,
	poisoned: bool,
	retry_count: u32,
	overlay: FlowWriteOverlay,
}

impl FlowActor {
	pub fn new(params: FlowActorParams) -> Self {
		let flow_id = params.flow.id;
		let ticks_enabled = params.flow.ticks();
		Self {
			computer: SliceComputer::new(params.engine.clone()),
			config: SliceConfig {
				chunk_size: params.chunk_size,
				checkpoint_lag: params.checkpoint_lag,
			},
			engine: params.engine,
			committer: params.committer,
			cdc_store: params.cdc_store,
			custom_operators: params.custom_operators,
			allocators: params.allocators,
			clock: params.clock,
			health: params.health,
			flow_tracker: params.flow_tracker,
			flow: params.flow,
			flow_id,
			ticks_enabled,
			retry_limit: params.retry_limit,
			retry_backoff: params.retry_backoff,
			initial_source_shapes: params.source_shapes,
			initial_cursor: params.cursor,
		}
	}

	fn tick_interval(&self) -> Duration {
		self.engine.catalog().get_config_duration(ConfigKey::FlowTick)
	}

	fn poison(&self, state: &mut FlowActorState, reason: String) {
		error!(flow_id = self.flow_id.0, reason = %reason, "poisoning flow");
		self.health.mark_poisoned(self.flow_id, reason);
		state.poisoned = true;
	}

	fn publish_position(&self, cursor: CommitVersion) {
		self.flow_tracker.update(self.flow_id, cursor);
	}

	fn retry_or_poison(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>, reason: String) {
		if state.retry_count >= self.retry_limit {
			self.poison(state, reason);
			return;
		}
		state.retry_count += 1;
		let backoff = self.retry_backoff * (1i64 << state.retry_count.min(16));
		warn!(
			flow_id = self.flow_id.0,
			attempt = state.retry_count,
			reason = %reason,
			"flow error, retrying after backoff"
		);
		ctx.schedule_once(backoff, || FlowActorMessage::Drain);
	}

	fn build_flow_engine(&self) -> FlowEngineInner {
		FlowEngineInner::new(
			self.engine.catalog(),
			self.engine.executor(),
			self.engine.event_bus().clone(),
			RuntimeContext::with_clock(self.clock.clone()),
			self.custom_operators.clone(),
			self.allocators.clone(),
		)
	}

	fn register_flow(&self, flow_engine: &mut FlowEngineInner) -> Result<()> {
		let mut txn = self.engine.begin_command(IdentityId::system())?;
		flow_engine.register(&mut txn, self.flow.clone())?;
		txn.rollback()?;
		Ok(())
	}

	fn on_drain(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>) {
		if state.poisoned || state.committing {
			return;
		}
		let step = self.computer.step(
			&mut state.flow_engine,
			&self.cdc_store,
			SliceCursor {
				flow_id: self.flow_id,
				source_shapes: &state.source_shapes,
				cursor: state.cursor,
				durable_cursor: state.durable_cursor,
			},
			&self.config,
			&mut state.overlay,
		);
		match step {
			Ok(SliceStep::Idle) => {
				state.retry_count = 0;
			}
			Ok(SliceStep::Skip {
				advance_to,
				more,
			}) => {
				state.retry_count = 0;
				state.cursor = advance_to;
				self.publish_position(advance_to);
				if more {
					let _ = ctx.self_ref().send(FlowActorMessage::Drain);
				}
			}
			Ok(SliceStep::Commit {
				slice,
				advance_to,
				more,
			}) => {
				self.dispatch_commit(state, ctx, slice, advance_to, more);
			}
			Err(e) => {
				self.retry_or_poison(state, ctx, format!("flow step failed: {e}"));
			}
		}
	}

	fn on_ingest(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		cdcs: Arc<Vec<Cdc>>,
		covers_from: CommitVersion,
		up_to: CommitVersion,
	) {
		if state.poisoned {
			return;
		}
		if state.committing {
			state.wake_pending = true;
			return;
		}
		if state.cursor >= up_to {
			return;
		}
		if state.cursor < covers_from {
			let _ = ctx.self_ref().send(FlowActorMessage::Drain);
			return;
		}

		let step = self.computer.step_pushed(
			&mut state.flow_engine,
			cdcs.as_slice(),
			SliceCursor {
				flow_id: self.flow_id,
				source_shapes: &state.source_shapes,
				cursor: state.cursor,
				durable_cursor: state.durable_cursor,
			},
			&self.config,
			&mut state.overlay,
		);
		match step {
			Ok(SliceStep::Idle) => {
				state.retry_count = 0;
			}
			Ok(SliceStep::Skip {
				advance_to,
				more,
			}) => {
				state.retry_count = 0;
				state.cursor = advance_to;
				self.publish_position(advance_to);
				if more {
					let _ = ctx.self_ref().send(FlowActorMessage::Drain);
				}
			}
			Ok(SliceStep::Commit {
				slice,
				advance_to,
				more,
			}) => {
				self.dispatch_commit(state, ctx, slice, advance_to, more);
			}
			Err(e) => {
				self.retry_or_poison(state, ctx, format!("flow ingest failed: {e}"));
			}
		}
	}

	fn dispatch_commit(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		slice: FlowSlice,
		advance_to: CommitVersion,
		more: bool,
	) {
		state.committing = true;
		let self_ref = ctx.self_ref().clone();
		let reply: SliceCommitReply = Box::new(move |result| {
			let (result, committed) = match result {
				Ok(committed) => (Ok(()), Some(committed)),
				Err(e) => (Err(e), None),
			};
			let _ = self_ref.send(FlowActorMessage::CommitDone {
				advance_to,
				more,
				result,
				committed,
			});
		});
		if self.committer
			.send(CommitterMessage::Slice {
				slice,
				reply,
			})
			.is_err()
		{
			state.committing = false;
			self.poison(state, "committer stopped".to_string());
		}
	}

	fn on_commit_done(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		advance_to: CommitVersion,
		more: bool,
		result: Result<()>,
		committed: Option<(CommitVersion, Pending)>,
	) {
		state.committing = false;
		if let Some((commit_version, pending)) = committed {
			state.overlay.promote(commit_version, pending);
		}
		match result {
			Ok(()) => {
				state.retry_count = 0;
				state.cursor = advance_to;
				state.durable_cursor = advance_to;
				self.publish_position(advance_to);
				if more || state.wake_pending {
					state.wake_pending = false;
					let _ = ctx.self_ref().send(FlowActorMessage::Drain);
				}
			}
			Err(e) => {
				self.retry_or_poison(state, ctx, format!("slice commit failed: {e}"));
			}
		}
	}

	fn on_tick(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>) {
		if self.ticks_enabled && !state.poisoned && !state.committing {
			let timestamp = DateTime::from_timestamp_millis(self.clock.now_millis()).unwrap();
			match self.computer.tick(&mut state.flow_engine, self.flow_id, timestamp) {
				Ok((pending, pending_shapes)) => {
					let has_output =
						pending.iter_sorted().next().is_some() || !pending_shapes.is_empty();
					if has_output {
						self.dispatch_tick_commit(state, ctx, pending, pending_shapes);
					}
				}
				Err(e) => {
					warn!(flow_id = self.flow_id.0, error = %e, "flow tick failed");
				}
			}
		}

		ctx.schedule_once(self.tick_interval(), || FlowActorMessage::Tick);

		if !state.poisoned && !state.committing {
			let _ = ctx.self_ref().send(FlowActorMessage::Drain);
		}
	}

	fn dispatch_tick_commit(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		pending: Pending,
		pending_shapes: Vec<RowShape>,
	) {
		state.committing = true;
		let self_ref = ctx.self_ref().clone();
		let advance_to = state.cursor;
		let reply: TickCommitReply = Box::new(move |committed| {
			let _ = self_ref.send(FlowActorMessage::CommitDone {
				advance_to,
				more: false,
				result: Ok(()),
				committed,
			});
		});
		if self.committer
			.send(CommitterMessage::Tick {
				pending,
				pending_shapes,
				reply,
			})
			.is_err()
		{
			state.committing = false;
			self.poison(state, "committer stopped".to_string());
		}
	}

	fn on_stop(&self, delete_checkpoint: bool) {
		if delete_checkpoint {
			let mut slice = FlowSlice::empty();
			slice.checkpoint_deletes.push(self.flow_id);
			let reply: SliceCommitReply = Box::new(|_| {});
			let _ = self.committer.send(CommitterMessage::Slice {
				slice,
				reply,
			});
		}
	}
}

impl Actor for FlowActor {
	type State = FlowActorState;
	type Message = FlowActorMessage;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		let mut flow_engine = self.build_flow_engine();
		let poisoned = match self.register_flow(&mut flow_engine) {
			Ok(()) => false,
			Err(e) => {
				error!(flow_id = self.flow_id.0, error = %e, "failed to register flow, poisoning");
				self.health.mark_poisoned(self.flow_id, format!("registration failed: {e}"));
				true
			}
		};

		self.publish_position(self.initial_cursor);

		ctx.schedule_once(self.tick_interval(), || FlowActorMessage::Tick);
		if !poisoned {
			let _ = ctx.self_ref().send(FlowActorMessage::Drain);
		}

		FlowActorState {
			flow_engine,
			source_shapes: self.initial_source_shapes.clone(),
			cursor: self.initial_cursor,
			durable_cursor: self.initial_cursor,
			committing: false,
			wake_pending: false,
			poisoned,
			retry_count: 0,
			overlay: FlowWriteOverlay::new(),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		let directive = catch_unwind(AssertUnwindSafe(|| match msg {
			FlowActorMessage::Drain => {
				self.on_drain(state, ctx);
				Directive::Continue
			}
			FlowActorMessage::Wake => {
				if !state.poisoned {
					if state.committing {
						state.wake_pending = true;
					} else {
						let _ = ctx.self_ref().send(FlowActorMessage::Drain);
					}
				}
				Directive::Continue
			}
			FlowActorMessage::Ingest {
				cdcs,
				covers_from,
				up_to,
			} => {
				self.on_ingest(state, ctx, cdcs, covers_from, up_to);
				Directive::Continue
			}
			FlowActorMessage::Tick => {
				self.on_tick(state, ctx);
				Directive::Continue
			}
			FlowActorMessage::UpdateSources {
				source_shapes,
			} => {
				state.source_shapes = source_shapes;
				if !state.poisoned && !state.committing {
					let _ = ctx.self_ref().send(FlowActorMessage::Drain);
				}
				Directive::Continue
			}
			FlowActorMessage::CommitDone {
				advance_to,
				more,
				result,
				committed,
			} => {
				self.on_commit_done(state, ctx, advance_to, more, result, committed);
				Directive::Continue
			}
			FlowActorMessage::Stop {
				delete_checkpoint,
				reply,
			} => {
				self.on_stop(delete_checkpoint);
				(reply)();
				Directive::Stop
			}
		}));

		directive.unwrap_or_else(|_| {
			error!(flow_id = self.flow_id.0, "panic in flow actor, aborting");
			process::abort()
		})
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}
