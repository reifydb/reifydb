// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeSet,
	mem::take,
	panic::{AssertUnwindSafe, catch_unwind},
	process,
	sync::Arc,
	time::Instant,
};

use reifydb_cdc::consume::backlog::{BacklogPull, FlowBacklog};
use reifydb_codec::encoded::shape::RowShape;
use reifydb_core::{
	actors::{flow::FlowActorMessage, pending::Pending},
	common::CommitVersion,
	interface::{
		WithEventBus,
		catalog::{
			config::{ConfigKey, GetConfig},
			flow::FlowId,
			object::ObjectId,
		},
		cdc::Cdc,
		change::Change,
	},
	state::budget::OperatorStateBudgetHandle,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_flow::transaction::substrate::{FlowSubstrate, apply_operator_state};
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
	byte_size::ByteSize,
	reifydb_assertions,
	value::{datetime::DateTime, duration::Duration, identity::IdentityId},
};
use tracing::{error, info, warn};

use crate::{
	builder::CustomOperators,
	deferred::{
		committer::{CommitterMessage, FlowSlice, SliceCommitReply, TickCommitReply},
		frontier::ControlFrontier,
		health::FlowHealthRegistry,
		loader::{LoaderMessage, LoaderReply},
		overlay::FlowWriteOverlay,
		slice::{SliceComputer, SliceConfig, SliceCursor, SliceStep},
		snapshot::{FlowSnapshotLoad, FlowSnapshots},
		tracker::FlowPositionTracker,
	},
	engine::FlowEngineInner,
	operator::metrics::OperatorSampleRegistry,
};

pub struct FlowActorParams {
	pub engine: StandardEngine,
	pub committer: ActorRef<CommitterMessage>,
	pub backlog: FlowBacklog,
	pub loader: ActorRef<LoaderMessage>,
	pub control: ControlFrontier,
	pub custom_operators: CustomOperators,
	pub substrate: FlowSubstrate,
	pub operator_samples: OperatorSampleRegistry,
	pub state_budget: OperatorStateBudgetHandle,
	pub clock: Clock,
	pub health: FlowHealthRegistry,
	pub flow_tracker: FlowPositionTracker,
	pub flow: FlowDag,
	pub source_objects: Arc<BTreeSet<ObjectId>>,
	pub cursor: CommitVersion,
	pub pull_batch_bytes: ByteSize,
	pub load_batch_bytes: ByteSize,
	pub checkpoint_lag: u64,
	pub checkpoint_max_age: Duration,
	pub retry_limit: u32,
	pub retry_backoff: Duration,
	pub snapshots: Option<FlowSnapshots>,
	pub snapshot_load: FlowSnapshotLoad,
}

pub struct FlowActor {
	engine: StandardEngine,
	committer: ActorRef<CommitterMessage>,
	backlog: FlowBacklog,
	loader: ActorRef<LoaderMessage>,
	control: ControlFrontier,
	custom_operators: CustomOperators,
	substrate: FlowSubstrate,
	operator_samples: OperatorSampleRegistry,
	state_budget: OperatorStateBudgetHandle,
	clock: Clock,
	health: FlowHealthRegistry,
	flow_tracker: FlowPositionTracker,
	flow: FlowDag,
	flow_id: FlowId,
	ticks_enabled: bool,
	computer: SliceComputer,
	config: SliceConfig,
	pull_batch_bytes: ByteSize,
	load_batch_bytes: ByteSize,
	retry_limit: u32,
	retry_backoff: Duration,
	checkpoint_max_age: Duration,
	initial_source_objects: Arc<BTreeSet<ObjectId>>,
	initial_cursor: CommitVersion,
	snapshots: Option<FlowSnapshots>,
	snapshot_load: FlowSnapshotLoad,
}

pub struct FlowActorState {
	flow_engine: FlowEngineInner,
	source_objects: Arc<BTreeSet<ObjectId>>,
	cursor: CommitVersion,
	durable_cursor: CommitVersion,
	committing: bool,
	awaiting_load: bool,
	poisoned: bool,
	catching_up: bool,
	replay_cursor: CommitVersion,
	replay_started_at: Instant,
	retry_count: u32,
	overlay: FlowWriteOverlay,
	drain_after_commit: bool,
	last_checkpoint_at: DateTime,
	last_snapshot_at: DateTime,
}

impl FlowActor {
	pub fn new(params: FlowActorParams) -> Self {
		let flow_id = params.flow.id;
		let ticks_enabled = params.flow.ticks();
		Self {
			computer: SliceComputer::new(params.engine.clone()),
			config: SliceConfig {
				checkpoint_lag: params.checkpoint_lag,
			},
			pull_batch_bytes: params.pull_batch_bytes,
			load_batch_bytes: params.load_batch_bytes,
			engine: params.engine,
			committer: params.committer,
			backlog: params.backlog,
			loader: params.loader,
			control: params.control,
			custom_operators: params.custom_operators,
			substrate: params.substrate,
			operator_samples: params.operator_samples,
			state_budget: params.state_budget,
			clock: params.clock,
			health: params.health,
			flow_tracker: params.flow_tracker,
			flow: params.flow,
			flow_id,
			ticks_enabled,
			retry_limit: params.retry_limit,
			retry_backoff: params.retry_backoff,
			checkpoint_max_age: params.checkpoint_max_age,
			initial_source_objects: params.source_objects,
			initial_cursor: params.cursor,
			snapshots: params.snapshots,
			snapshot_load: params.snapshot_load,
		}
	}

	fn tick_interval(&self) -> Duration {
		self.engine.catalog().get_config_duration(ConfigKey::FlowTick)
	}

	fn sample_interval(&self) -> Option<Duration> {
		self.engine.catalog().get_config_duration_opt(ConfigKey::FlowSampleInterval)
	}

	fn snapshot_interval(&self) -> Option<Duration> {
		self.engine.catalog().get_config_duration_opt(ConfigKey::OperatorSnapshotInterval)
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
		let mut flow_engine = self.build_flow_engine();
		if let Err(e) = self.register_flow(&mut flow_engine) {
			self.poison(state, format!("flow engine rebuild failed after error: {e} (original: {reason})"));
			return;
		}
		state.flow_engine = flow_engine;
		let backoff = self.retry_backoff * (1i64 << state.retry_count.min(16));
		warn!(
			flow_id = self.flow_id.0,
			attempt = state.retry_count,
			reason = %reason,
			"flow error, rebuilt operators and retrying after backoff"
		);
		ctx.schedule_once(backoff, || FlowActorMessage::Drain);
	}

	fn build_flow_engine(&self) -> FlowEngineInner {
		let engine = FlowEngineInner::new(
			self.engine.catalog(),
			self.engine.executor(),
			self.engine.event_bus().clone(),
			RuntimeContext::with_clock(self.clock.clone()),
			self.custom_operators.clone(),
			self.substrate.clone(),
			self.operator_samples.clone(),
			self.state_budget.clone(),
		);
		engine
	}

	fn register_flow(&self, flow_engine: &mut FlowEngineInner) -> Result<()> {
		let mut txn = self.engine.begin_command(IdentityId::system())?;
		flow_engine.register(&mut txn, self.flow.clone())?;
		txn.rollback()?;
		Ok(())
	}

	fn checkpoint_if_stale(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>) {
		if state.committing || state.cursor <= state.durable_cursor {
			return;
		}
		let now = self.clock.now();
		if now - state.last_checkpoint_at < self.checkpoint_max_age {
			return;
		}
		state.last_checkpoint_at = now;

		let advance_to = state.cursor;
		let mut slice = FlowSlice::empty();
		slice.checkpoints.push((self.flow_id, advance_to));
		self.dispatch_commit(state, ctx, slice, advance_to, false);
	}

	fn safe_bound(&self) -> CommitVersion {
		self.engine.cdc_producer_watermark().min(self.engine.done_until()).min(self.control.get())
	}

	fn begin_catch_up(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>) -> bool {
		let cursor = match self.snapshot_load {
			FlowSnapshotLoad::Empty => return false,
			FlowSnapshotLoad::Inconsistent => {
				self.poison(
					state,
					"the flow's operator snapshots carry no cursor every operator agrees on; \
					 resuming would mix state from different versions"
						.to_string(),
				);
				return false;
			}
			FlowSnapshotLoad::Restored(cursor) => cursor,
		};
		if cursor >= self.initial_cursor {
			return false;
		}
		let truncated_before = self.engine.cdc_store().truncated_before().unwrap_or(CommitVersion(0));
		if truncated_before > cursor {
			self.poison(
				state,
				format!(
					"the operator snapshot resumes at {} but cdc is truncated before {}; the \
					 versions needed to rebuild the gap are gone",
					cursor.0, truncated_before.0
				),
			);
			return false;
		}
		info!(
			flow_id = self.flow_id.0,
			from = cursor.0,
			to = self.initial_cursor.0,
			versions = self.initial_cursor.0.saturating_sub(cursor.0),
			"flow catch-up replay starting"
		);
		state.catching_up = true;
		state.replay_cursor = cursor;
		state.replay_started_at = Instant::now();
		self.request_catch_up(state, ctx);
		true
	}

	fn request_catch_up(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>) {
		let self_ref = ctx.self_ref().clone();
		let reply: LoaderReply = Box::new(move |outcome| {
			let _ = self_ref.send(FlowActorMessage::CatchUp {
				outcome,
			});
		});
		if self.loader
			.send(LoaderMessage::Fetch {
				from: state.replay_cursor,
				up_to: self.initial_cursor,
				budget: self.load_batch_bytes,
				reply,
			})
			.is_err()
		{
			state.catching_up = false;
			self.poison(state, "loader stopped during catch-up replay".to_string());
		}
	}

	fn on_catch_up(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		outcome: Result<(Vec<Arc<Cdc>>, CommitVersion)>,
	) {
		if state.poisoned {
			return;
		}
		let (items, advance_to) = match outcome {
			Ok(batch) => batch,
			Err(e) => {
				state.catching_up = false;
				self.poison(state, format!("flow catch-up replay could not read cdc: {e}"));
				return;
			}
		};
		match self.computer.replay(
			&mut state.flow_engine,
			self.flow_id,
			&items,
			&state.source_objects,
			advance_to,
		) {
			Ok(pending) => apply_operator_state(&self.substrate.operators, advance_to, &pending),
			Err(e) => {
				state.catching_up = false;
				self.poison(state, format!("flow catch-up replay failed: {e}"));
				return;
			}
		}
		if advance_to <= state.replay_cursor {
			state.catching_up = false;
			self.poison(
				state,
				format!("flow catch-up replay made no progress past {}", state.replay_cursor.0),
			);
			return;
		}
		state.replay_cursor = advance_to;
		if state.replay_cursor < self.initial_cursor {
			self.request_catch_up(state, ctx);
			return;
		}
		self.finish_catch_up(state, ctx);
	}

	fn finish_catch_up(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>) {
		state.catching_up = false;
		info!(
			flow_id = self.flow_id.0,
			through = state.replay_cursor.0,
			elapsed_ms = state.replay_started_at.elapsed().as_millis() as u64,
			arena_bytes = self.substrate.operators.total_bytes(),
			"flow catch-up replay complete"
		);
		self.snapshot_now(state, ctx);
		if state.committing {
			state.drain_after_commit = true;
		} else {
			let _ = ctx.self_ref().send(FlowActorMessage::Drain);
		}
	}

	fn on_drain(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>) {
		if state.poisoned || state.committing || state.awaiting_load || state.catching_up {
			return;
		}
		let safe = self.safe_bound();
		if safe <= state.cursor {
			state.retry_count = 0;
			state.overlay.prune_through(state.cursor);
			self.checkpoint_if_stale(state, ctx);
			return;
		}
		match self.backlog.pull(state.cursor, safe, self.pull_batch_bytes) {
			BacklogPull::Hit {
				items,
				advance_to,
				more,
			} => self.apply_items(state, ctx, &items, advance_to, more),
			BacklogPull::Behind => self.request_load(state, ctx, safe),
		}
	}

	fn apply_items(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		items: &[Arc<Cdc>],
		advance_to: CommitVersion,
		more: bool,
	) {
		let step = self.computer.compute_pulled(
			&mut state.flow_engine,
			items,
			SliceCursor {
				flow_id: self.flow_id,
				source_objects: &state.source_objects,
				cursor: state.cursor,
				durable_cursor: state.durable_cursor,
			},
			advance_to,
			more,
			&self.config,
			&mut state.overlay,
		);
		match step {
			Ok(SliceStep::Skip {
				advance_to,
				more,
			}) => {
				state.retry_count = 0;
				state.cursor = advance_to;
				self.publish_position(advance_to);
				if more {
					let _ = ctx.self_ref().send(FlowActorMessage::Drain);
				} else {
					self.checkpoint_if_stale(state, ctx);
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

	fn request_load(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>, up_to: CommitVersion) {
		state.awaiting_load = true;
		let self_ref = ctx.self_ref().clone();
		let reply: LoaderReply = Box::new(move |outcome| {
			let _ = self_ref.send(FlowActorMessage::Loaded {
				outcome,
			});
		});
		if self.loader
			.send(LoaderMessage::Fetch {
				from: state.cursor,
				up_to,
				budget: self.load_batch_bytes,
				reply,
			})
			.is_err()
		{
			state.awaiting_load = false;
			self.poison(state, "loader stopped".to_string());
		}
	}

	fn on_loaded(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		outcome: Result<(Vec<Arc<Cdc>>, CommitVersion)>,
	) {
		state.awaiting_load = false;
		if state.poisoned {
			return;
		}
		if state.committing {
			state.drain_after_commit = true;
			return;
		}
		match outcome {
			Ok((items, advance_to)) => {
				if advance_to <= state.cursor {
					let _ = ctx.self_ref().send(FlowActorMessage::Drain);
					return;
				}
				self.apply_items(state, ctx, &items, advance_to, true);
			}
			Err(e) => {
				self.retry_or_poison(state, ctx, format!("flow catch-up load failed: {e}"));
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
		reifydb_assertions! {
			assert!(
				!slice.checkpoints.is_empty(),
				"a slice commit must carry a checkpoint for {advance_to:?}; committing one without \
				 would record a durability this commit never wrote, and the flow would stop \
				 checkpointing because it believes it has nothing left to record"
			);
		}
		state.committing = true;
		let self_ref = ctx.self_ref().clone();
		let reply: SliceCommitReply = Box::new(move |result| {
			let (result, committed) = match result {
				Ok(committed) => (Ok(()), Some(committed)),
				Err(e) => (Err(e), None),
			};
			let _ = self_ref.send(FlowActorMessage::SliceCommitted {
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

	fn on_slice_committed(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		advance_to: CommitVersion,
		more: bool,
		result: Result<()>,
		committed: Option<(CommitVersion, Pending)>,
	) {
		self.settle_commit(state, committed);
		match result {
			Ok(()) => {
				state.retry_count = 0;
				state.cursor = advance_to;
				state.durable_cursor = advance_to;
				state.last_checkpoint_at = self.clock.now();
				self.publish_position(advance_to);
				self.resume_after_commit(state, ctx, more);
			}
			Err(e) => {
				self.retry_or_poison(state, ctx, format!("slice commit failed: {e}"));
			}
		}
	}

	fn on_tick_committed(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		committed: Option<(CommitVersion, Pending)>,
	) {
		self.settle_commit(state, committed);
		state.retry_count = 0;
		self.resume_after_commit(state, ctx, false);
	}

	fn settle_commit(&self, state: &mut FlowActorState, committed: Option<(CommitVersion, Pending)>) {
		state.committing = false;
		if let Some((commit_version, pending)) = committed {
			state.overlay.promote(commit_version, pending);
		}
	}

	fn resume_after_commit(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>, more: bool) {
		if more || take(&mut state.drain_after_commit) {
			let _ = ctx.self_ref().send(FlowActorMessage::Drain);
		}
	}

	fn on_tick(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>) {
		if self.ticks_enabled && !state.poisoned && !state.committing && !state.catching_up {
			let timestamp = DateTime::from_millis(self.clock.now().to_millis());
			match self.computer.tick(&mut state.flow_engine, self.flow_id, timestamp, state.durable_cursor)
			{
				Ok((pending, pending_shapes, view_changes)) => {
					let has_output = pending.iter_sorted().next().is_some()
						|| !pending_shapes.is_empty()
						|| !view_changes.is_empty();
					if has_output {
						self.dispatch_tick_commit(state, ctx, pending, pending_shapes, view_changes);
					}
				}
				Err(e) => {
					warn!(flow_id = self.flow_id.0, error = %e, "flow tick failed");
				}
			}
		}

		ctx.schedule_once(self.tick_interval(), || FlowActorMessage::Tick);

		self.maybe_snapshot(state, ctx);

		if !state.poisoned && !state.committing && !state.catching_up {
			let _ = ctx.self_ref().send(FlowActorMessage::Drain);
		}
	}

	fn maybe_snapshot(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>) {
		if self.snapshots.is_none() || state.poisoned || state.committing || state.catching_up {
			return;
		}
		let Some(interval) = self.snapshot_interval() else {
			return;
		};
		if self.clock.now() - state.last_snapshot_at < interval {
			return;
		}
		self.snapshot_now(state, ctx);
	}

	fn snapshot_now(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>) {
		let Some(snapshots) = &self.snapshots else {
			return;
		};
		state.last_snapshot_at = self.clock.now();

		let ids: Vec<_> = self.flow.get_operator_ids().collect();
		let Some(pin) = snapshots.write_flow(&self.substrate.operators, &ids, state.cursor) else {
			return;
		};

		let advance_to = state.cursor;
		let mut slice = FlowSlice::empty();
		slice.checkpoints.push((self.flow_id, advance_to));
		slice.snapshot_pins.push((self.flow_id, pin));
		self.dispatch_commit(state, ctx, slice, advance_to, false);
	}

	fn on_sample(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>) {
		let Some(interval) = self.sample_interval() else {
			return;
		};
		if !state.poisoned {
			state.flow_engine.sample_operators();
		}
		ctx.schedule_once(interval, || FlowActorMessage::Sample);
	}

	fn dispatch_tick_commit(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		pending: Pending,
		pending_shapes: Vec<RowShape>,
		view_changes: Vec<Change>,
	) {
		state.committing = true;
		state.drain_after_commit = true;
		let self_ref = ctx.self_ref().clone();
		let reply: TickCommitReply = Box::new(move |committed| {
			let _ = self_ref.send(FlowActorMessage::TickCommitted {
				committed,
			});
		});
		if self.committer
			.send(CommitterMessage::Tick {
				pending,
				pending_shapes,
				view_changes,
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
		if let Some(interval) = self.sample_interval() {
			ctx.schedule_once(interval, || FlowActorMessage::Sample);
		}

		let mut state = FlowActorState {
			flow_engine,
			source_objects: self.initial_source_objects.clone(),
			cursor: self.initial_cursor,
			durable_cursor: self.initial_cursor,
			committing: false,
			awaiting_load: false,
			poisoned,
			catching_up: false,
			replay_cursor: self.initial_cursor,
			replay_started_at: Instant::now(),
			retry_count: 0,
			overlay: FlowWriteOverlay::new(),
			drain_after_commit: false,
			last_checkpoint_at: self.clock.now(),
			last_snapshot_at: self.clock.now(),
		};

		if !state.poisoned && !self.begin_catch_up(&mut state, ctx) && !state.poisoned {
			let _ = ctx.self_ref().send(FlowActorMessage::Drain);
		}

		state
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		let directive = catch_unwind(AssertUnwindSafe(|| match msg {
			FlowActorMessage::Drain => {
				self.on_drain(state, ctx);
				Directive::Continue
			}
			FlowActorMessage::Wake => {
				if !state.poisoned {
					if state.committing || state.awaiting_load || state.catching_up {
						state.drain_after_commit = true;
					} else {
						let _ = ctx.self_ref().send(FlowActorMessage::Drain);
					}
				}
				Directive::Continue
			}
			FlowActorMessage::Loaded {
				outcome,
			} => {
				self.on_loaded(state, ctx, outcome);
				Directive::Continue
			}
			FlowActorMessage::CatchUp {
				outcome,
			} => {
				self.on_catch_up(state, ctx, outcome);
				Directive::Continue
			}
			FlowActorMessage::Tick => {
				self.on_tick(state, ctx);
				Directive::Continue
			}
			FlowActorMessage::Sample => {
				self.on_sample(state, ctx);
				Directive::Continue
			}
			FlowActorMessage::UpdateSources {
				source_objects,
			} => {
				state.source_objects = source_objects;
				if !state.poisoned && !state.committing {
					let _ = ctx.self_ref().send(FlowActorMessage::Drain);
				}
				Directive::Continue
			}
			FlowActorMessage::SliceCommitted {
				advance_to,
				more,
				result,
				committed,
			} => {
				self.on_slice_committed(state, ctx, advance_to, more, result, committed);
				Directive::Continue
			}
			FlowActorMessage::TickCommitted {
				committed,
			} => {
				self.on_tick_committed(state, ctx, committed);
				Directive::Continue
			}
			FlowActorMessage::Stop {
				delete_checkpoint,
				reply,
			} => {
				state.flow_engine.forget_operator_samples();
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

#[cfg(test)]
mod pull_protocol {
	use std::{
		collections::HashMap,
		ops::Bound,
		thread::sleep,
		time::{Duration as StdDuration, Instant},
	};

	use reifydb_cdc::{
		consume::{checkpoint::CdcCheckpoint, watermark::CdcConsumerWatermark},
		produce::watermark::CdcProducerWatermark,
		storage::CdcStorage,
	};
	use reifydb_core::{
		actors::{flow::FlowActorHandle, pending::PendingLayers},
		interface::{
			catalog::{
				flow::OperatorId,
				ringbuffer::{RingBufferMetadata, decode_ringbuffer_metadata},
			},
			cdc::{ConsumerClass, SystemChange},
			change::{ChangeOrigin, Diff},
		},
		key::{
			Key, cdc_consumer::FlowSnapshotPin, kind::KeyKind,
			operator_group_state::{Keyspace, OperatorGroupStateKey},
			operator_state::OperatorStateKey,
		},
	};
	use reifydb_codec::{
		encoded::row::EncodedRow,
		key::encoded::{EncodedKey, EncodedKeyRange},
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_flow::transaction::{DeferredParams, FlowTransaction};
	use reifydb_runtime::sync::waiter::WaiterHandle;
	use reifydb_sqlite::SqliteConfig;
	use reifydb_store_operator::{OperatorStore, snapshot::SnapshotStore};
	use reifydb_testing_flow::state::{State, assert_batch_equivalent};
	use reifydb_transaction::{
		group::{GroupCommitBegin, GroupCommitHandle},
		multi::RangeScope,
		transaction::Transaction,
	};
	use reifydb_value::{util::cowvec::CowVec, value::Value};

	use super::*;
	use crate::{
		catalog::FlowCatalog,
		deferred::{
			committer::{Committer, CommitterActor, CommitterHandle},
			loader::{LoaderActor, LoaderHandle, LoaderMetrics},
			quiescence::FlowMaterialization,
			routing,
			snapshot::SnapshotPinTracker,
		},
	};

	struct Harness {
		te: TestEngine,
		engine: StandardEngine,
		backlog: FlowBacklog,
		control: ControlFrontier,
		tracker: FlowPositionTracker,
		committer_handle: CommitterHandle,
		loader_handle: LoaderHandle,
		flow: FlowDag,
		flow_id: FlowId,
		source_objects: Arc<BTreeSet<ObjectId>>,
		substrate: FlowSubstrate,
		health: FlowHealthRegistry,
		snapshots: Option<FlowSnapshots>,
		snapshot_guard: Option<reifydb_sqlite::SqliteTempPathGuard>,
	}

	fn harness() -> Harness {
		// FLOW_TICK is an hour out and there is no supervisor, so only wakes these tests
		// send can advance the actor.
		harness_with(
			"CREATE TABLE app::t { id: int4 }",
			"CREATE DEFERRED VIEW app::v { id: int4 } AS { FROM app::t MAP { id } }",
		)
	}

	fn harness_with(table_rql: &str, view_rql: &str) -> Harness {
		let te = TestEngine::builder().with_cdc().build();
		let engine = te.inner().clone();

		{
			let catalog = engine.catalog();
			let mut admin = engine.begin_admin(IdentityId::system()).expect("begin admin");
			catalog.set_config(&mut admin, ConfigKey::FlowTick, Value::duration_seconds(3600))
				.expect("set flow tick");
			admin.commit().expect("commit config");
		}

		te.admin("CREATE NAMESPACE app");
		te.admin(table_rql);
		te.admin(view_rql);

		let flow_catalog = FlowCatalog::new(engine.catalog());

		let mut query = engine.begin_query(IdentityId::system()).expect("query");
		let flows = engine.catalog().list_flows_all(&mut Transaction::Query(&mut query)).expect("list flows");
		let flow_id = flows.first().expect("one flow").id;
		drop(query);

		let substrate = FlowSubstrate::with_dictionary(engine.dictionary_allocators(), engine.operator_state());
		let mut probe = FlowEngineInner::new(
			engine.catalog(),
			engine.executor(),
			engine.event_bus().clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			CustomOperators::new(HashMap::new()),
			substrate.clone(),
			OperatorSampleRegistry::new(),
			OperatorStateBudgetHandle::default(),
		);
		let mut txn = engine.begin_command(IdentityId::system()).expect("command");
		let (flow, _) =
			flow_catalog.get_or_load_flow(&mut Transaction::Command(&mut txn), flow_id).expect("load flow");
		probe.register(&mut txn, flow.clone()).expect("register probe");
		txn.rollback().expect("rollback probe");

		let source_objects = {
			let graph = probe.analyzer.get_dependency_graph();
			let registered = |f: FlowId| f == flow_id;
			let view_route = |vid| {
				flow_catalog.find_view(vid).map(|v| routing::ViewRoute {
					kind: v.kind(),
					storage: v.storage_id(),
				})
			};
			Arc::new(routing::flow_source_objects(graph, flow_id, &registered, &view_route))
		};

		let tracker = FlowPositionTracker::new();
		let committer = Committer::new(
			flow_catalog,
			tracker.clone(),
			FlowMaterialization::new(CdcConsumerWatermark::new(), FlowPositionTracker::new()),
			substrate.operators.clone(),
			SnapshotPinTracker::new(),
		);
		let begin_engine = engine.clone();
		let begin: GroupCommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
		let group = GroupCommitHandle::spawn(
			&engine.spawner(),
			begin,
			Duration::from_milliseconds(100).unwrap(),
			256,
		);
		let committer_handle = engine.spawner().spawn_flow(
			"pull-protocol-committer",
			CommitterActor::new(committer, group, OperatorStateBudgetHandle::default()),
		);

		let loader_handle = engine.spawner().spawn_flow(
			"pull-protocol-loader",
			LoaderActor::new(engine.cdc_store().hot_reader(), LoaderMetrics::default()),
		);

		let backlog =
			engine.ioc().resolve::<FlowBacklog>().expect("test harness must register the flow backlog");

		Harness {
			te,
			engine,
			backlog,
			control: ControlFrontier::new(),
			tracker,
			committer_handle,
			loader_handle,
			flow,
			flow_id,
			source_objects,
			substrate,
			health: FlowHealthRegistry::new(),
			snapshots: None,
			snapshot_guard: None,
		}
	}

	fn snapshotting_harness(table_rql: &str, view_rql: &str) -> (Harness, SnapshotStore) {
		let mut h = harness_with(table_rql, view_rql);
		{
			let catalog = h.engine.catalog();
			let mut admin = h.engine.begin_admin(IdentityId::system()).expect("begin admin");
			catalog.set_config(&mut admin, ConfigKey::OperatorSnapshotInterval, Value::duration_seconds(1))
				.expect("set snapshot interval");
			admin.commit().expect("commit config");
		}
		let (config, guard) = SqliteConfig::test();
		let store = SnapshotStore::sqlite(config);
		h.snapshots = Some(FlowSnapshots::new(
			store.clone(),
			h.engine.single().read_store(),
			h.engine.dictionary_allocators(),
		));
		h.snapshot_guard = Some(guard);
		(h, store)
	}

	fn aggregate_harness() -> (Harness, SnapshotStore) {
		snapshotting_harness(
			"CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { ts: ts }",
			"CREATE DEFERRED VIEW app::v { g: int4, total: int8 } with { time: event } \
			 AS { FROM app::t AGGREGATE { total: math::count(id) } BY { g } }",
		)
	}

	impl Harness {
		fn spawn_actor(&self, cursor: CommitVersion) -> FlowActorHandle {
			// `init`'s lazy Drain skips the cursor to the safe watermark, so the actor must
			// be settled at `cursor` before the test writes anything a wake will deliver.
			self.control.store(CommitVersion(u64::MAX));
			self.spawn_actor_with_bounded_control(cursor)
		}

		fn spawn_actor_with_bounded_control(&self, cursor: CommitVersion) -> FlowActorHandle {
			self.spawn_actor_with_substrate(cursor, self.substrate.clone())
		}

		fn spawn_actor_with_substrate(&self, cursor: CommitVersion, substrate: FlowSubstrate) -> FlowActorHandle {
			self.await_safe_watermark(cursor);

			let handle = self.engine.spawner().spawn_flow(
				"pull-protocol-flow",
				FlowActor::new(FlowActorParams {
					engine: self.engine.clone(),
					committer: self.committer_handle.actor_ref().clone(),
					backlog: self.backlog.clone(),
					loader: self.loader_handle.actor_ref().clone(),
					control: self.control.clone(),
					custom_operators: CustomOperators::new(HashMap::new()),
					substrate,
					operator_samples: OperatorSampleRegistry::new(),
					state_budget: OperatorStateBudgetHandle::default(),
					clock: self.engine.clock().clone(),
					health: self.health.clone(),
					flow_tracker: self.tracker.clone(),
					flow: self.flow.clone(),
					source_objects: self.source_objects.clone(),
					cursor: CommitVersion(cursor.0 - 1),
					pull_batch_bytes: ByteSize::from_mib(8),
					load_batch_bytes: ByteSize::from_mib(8),
					checkpoint_lag: 10_000,
					checkpoint_max_age: Duration::from_milliseconds(5_000).unwrap(),
					retry_limit: 3,
					retry_backoff: Duration::from_milliseconds(50).unwrap(),
					snapshots: self.snapshots.clone(),
					snapshot_load: FlowSnapshotLoad::Empty,
				}),
			);

			assert_eq!(
				self.await_position(cursor, StdDuration::from_secs(10)),
				Some(cursor),
				"the init Drain must be consumed, and the cursor settled at {}, before the test \
				 writes anything a wake will deliver",
				cursor.0
			);
			handle
		}

		fn wake(&self, actor: &FlowActorHandle) {
			assert!(actor.actor_ref().send(FlowActorMessage::Wake).is_ok(), "send wake");
		}

		fn view_rows(&self) -> usize {
			self.te.query("FROM app::v").first().map(|f| f.row_count()).unwrap_or(0)
		}

		fn view_bearing_records(&self, up_to: CommitVersion) -> usize {
			// This flow's slices never overlap, so one view-bearing commit is exactly one
			// slice.
			self.engine
				.cdc_store()
				.read_range(Bound::Unbounded, Bound::Unbounded, 10_000)
				.expect("read range")
				.items
				.iter()
				.filter(|cdc| cdc.version > up_to)
				.filter(|cdc| {
					cdc.changes.iter().any(|change| {
						matches!(change.origin, ChangeOrigin::Object(ObjectId::View(_)))
					})
				})
				.count()
		}

		fn await_view_rows(&self, want: usize, timeout: StdDuration) -> usize {
			let deadline = Instant::now() + timeout;
			loop {
				let got = self.view_rows();
				if got >= want || Instant::now() >= deadline {
					return got;
				}
				sleep(StdDuration::from_millis(10));
			}
		}

		fn await_safe_watermark(&self, want: CommitVersion) {
			// The spawn waits for the init Drain to land on exactly `want`, which only
			// happens once the safe watermark already covers it.
			let deadline = Instant::now() + StdDuration::from_secs(10);
			loop {
				let safe = self.engine.cdc_producer_watermark().min(self.engine.done_until());
				if safe >= want {
					return;
				}
				assert!(Instant::now() < deadline, "safe watermark never reached {}", want.0);
				sleep(StdDuration::from_millis(5));
			}
		}

		fn await_position(&self, want: CommitVersion, timeout: StdDuration) -> Option<CommitVersion> {
			let deadline = Instant::now() + timeout;
			loop {
				let got = self.tracker.all().get(&self.flow_id).copied();
				if got == Some(want) || Instant::now() >= deadline {
					return got;
				}
				sleep(StdDuration::from_millis(10));
			}
		}

		fn poll_until<T>(&self, timeout: Duration, mut probe: impl FnMut() -> Option<T>) -> Option<T> {
			// Not the engine clock: that one is a frozen mock the tick tests advance by
			// hand, so an elapsed check against it would hang rather than fail.
			let started = Clock::testing().instant();
			let timeout = timeout.to_std();
			loop {
				if let Some(found) = probe() {
					return Some(found);
				}
				if started.elapsed() >= timeout {
					return None;
				}
				sleep(millis(10).to_std());
			}
		}

		fn await_position_at_least(&self, floor: CommitVersion, timeout: Duration) -> Option<CommitVersion> {
			// A drain that skips lands wherever is safe at that instant, at or above the
			// floor but never exactly on it.
			self.poll_until(timeout, || {
				self.tracker.all().get(&self.flow_id).copied().filter(|got| *got >= floor)
			})
		}

		fn await_version_beyond(&self, floor: CommitVersion, timeout: Duration) -> Option<CommitVersion> {
			self.poll_until(timeout, || self.engine.current_version().ok().filter(|got| *got > floor))
		}

		fn persisted_checkpoint(&self) -> Option<CommitVersion> {
			// The durable position, not the in-memory one: only what survives a restart can
			// tell a real checkpoint from a claimed one.
			let mut txn = self.engine.begin_query(IdentityId::system()).expect("query");
			CdcCheckpoint::fetch_opt(&mut Transaction::Query(&mut txn), &self.flow_id)
				.expect("fetch checkpoint")
		}

		fn await_checkpoint_beyond(&self, floor: CommitVersion, timeout: Duration) -> Option<CommitVersion> {
			self.poll_until(timeout, || self.persisted_checkpoint().filter(|got| *got > floor))
		}

		fn advance_source_watermarks(&self, substrate: &FlowSubstrate, at: DateTime) {
			// Reproduces the state a tick fires timers from: the watermark already covers the
			// timer but no batch dispatched it (restart hydration, or a batch that exhausted
			// its timer budget). The advance sticks in the substrate the actor shares; the
			// throwaway transaction only carries the hydration read and is dropped.
			let sources: Vec<OperatorId> = self
				.flow
				.get_operator_ids()
				.filter(|id| self.flow.get_operator(id).is_some_and(|op| op.ty.is_source()))
				.collect();
			assert!(!sources.is_empty(), "the flow under test must have a source to advance");
			let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
				version: self.engine.current_version().expect("current version"),
				pending: Pending::new(),
				base_pending: PendingLayers::empty(),
				query: self.engine.multi().begin_query().expect("query"),
				state_query: self.engine.multi().begin_query().expect("state query"),
				single: self.engine.single().clone(),
				catalog: self.engine.catalog(),
				interceptors: self.engine.create_interceptors(),
				clock: self.engine.clock().clone(),
				substrate: substrate.clone(),
				state_budget: OperatorStateBudgetHandle::default(),
			});
			for source in sources {
				substrate.watermarks.advance(source, &mut txn, at).expect("advance watermark");
			}
		}

		fn cdc_records(&self) -> Vec<Cdc> {
			self.engine
				.cdc_store()
				.read_range(Bound::Unbounded, Bound::Unbounded, 10_000)
				.expect("read cdc range")
				.items
		}

		fn view_change_beyond(&self, floor: CommitVersion) -> Option<Change> {
			self.cdc_records()
				.into_iter()
				.filter(|cdc| cdc.version > floor)
				.flat_map(|cdc| cdc.changes)
				.find(|change| matches!(change.origin, ChangeOrigin::Object(ObjectId::View(_))))
		}

		fn arena_state(&self, substrate: &FlowSubstrate) -> State {
			// Full keys, not the arena's inner form: the shared comparator decodes the operator
			// prefix to name the keyspace a difference lives in.
			let mut out = State::new();
			for operator in self.flow.get_operator_ids() {
				let prefix = OperatorStateKey::encoded(operator, vec![]);
				let batch = substrate.operators.range_batch(
					operator,
					EncodedKeyRange::new(Bound::Unbounded, Bound::Unbounded),
					u64::MAX,
				);
				for (key, row) in batch.items {
					let mut full = prefix.as_slice().to_vec();
					full.extend_from_slice(key.as_slice());
					out.push((EncodedKey::new(full), row));
				}
			}
			out
		}

		/// Blocks until catch-up has COMPLETED, using the only unambiguous signal there is: the
		/// snapshot it publishes on completion, stamped at the cursor it caught up to. Waiting on
		/// arena size instead would race a replay that is merely partway there.
		fn await_catch_up(&self, store: &SnapshotStore, upto: CommitVersion) {
			let probe = self.flow.get_operator_ids().next().expect("the flow must have an operator");
			assert!(
				self.poll_until(seconds(20), || {
					store.generation_cursors(probe)
						.expect("generation cursors")
						.first()
						.filter(|(_, cursor)| *cursor == upto)
						.map(|_| ())
				})
				.is_some(),
				"catch-up never completed: no snapshot generation was published at cursor {}, \
				 newest is {:?}",
				upto.0,
				store.generation_cursors(probe).expect("generation cursors").first()
			);
		}

		fn write_snapshot(&self, at: CommitVersion) -> CommitVersion {
			let snapshots = self.snapshots.as_ref().expect("the harness must carry a snapshot store");
			let ids: Vec<OperatorId> = self.flow.get_operator_ids().collect();
			snapshots
				.write_flow(&self.substrate.operators, &ids, at)
				.expect("the flow must hold state worth snapshotting")
		}

		fn stop(&self, actor: &FlowActorHandle) {
			let stopped = Arc::new(WaiterHandle::new());
			let notify = Arc::clone(&stopped);
			assert!(actor.actor_ref()
				.send(FlowActorMessage::Stop {
					delete_checkpoint: false,
					reply: Box::new(move || notify.notify()),
				})
				.is_ok());
			assert!(stopped.wait_timeout(seconds(10)), "the actor must stop before the restart");
		}

		/// The supervisor's bootstrap in miniature: a FRESH arena, `load_flow` into it, and an
		/// actor spawned at the durable checkpoint carrying whatever the load reported. This is
		/// the only shape a real restart takes, so catch-up must be driven through it.
		fn restart(&self, cursor: CommitVersion, load_batch_bytes: ByteSize) -> Restart {
			let substrate =
				FlowSubstrate::with_dictionary(self.engine.dictionary_allocators(), OperatorStore::default());
			let snapshot_load = match &self.snapshots {
				Some(snapshots) => snapshots.load_flow(
					&substrate.operators,
					self.flow.get_operator_ids(),
					self.engine.cdc_store().truncated_before().unwrap_or(CommitVersion(0)),
				),
				None => FlowSnapshotLoad::Empty,
			};

			let committer = Committer::new(
				FlowCatalog::new(self.engine.catalog()),
				self.tracker.clone(),
				FlowMaterialization::new(CdcConsumerWatermark::new(), FlowPositionTracker::new()),
				substrate.operators.clone(),
				SnapshotPinTracker::new(),
			);
			let begin_engine = self.engine.clone();
			let begin: GroupCommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
			let committer_handle = self.engine.spawner().spawn_flow(
				"pull-protocol-committer-restart",
				CommitterActor::new(
					committer,
					GroupCommitHandle::inline(begin),
					OperatorStateBudgetHandle::default(),
				),
			);
			let health = FlowHealthRegistry::new();
			let actor = self.engine.spawner().spawn_flow(
				"pull-protocol-flow-restart",
				FlowActor::new(FlowActorParams {
					engine: self.engine.clone(),
					committer: committer_handle.actor_ref().clone(),
					backlog: self.backlog.clone(),
					loader: self.loader_handle.actor_ref().clone(),
					control: self.control.clone(),
					custom_operators: CustomOperators::new(HashMap::new()),
					substrate: substrate.clone(),
					operator_samples: OperatorSampleRegistry::new(),
					state_budget: OperatorStateBudgetHandle::default(),
					clock: self.engine.clock().clone(),
					health: health.clone(),
					flow_tracker: self.tracker.clone(),
					flow: self.flow.clone(),
					source_objects: self.source_objects.clone(),
					cursor,
					pull_batch_bytes: ByteSize::from_mib(8),
					load_batch_bytes,
					checkpoint_lag: 10_000,
					checkpoint_max_age: Duration::from_milliseconds(5_000).unwrap(),
					retry_limit: 3,
					retry_backoff: Duration::from_milliseconds(50).unwrap(),
					snapshots: self.snapshots.clone(),
					snapshot_load,
				}),
			);
			Restart {
				substrate,
				health,
				snapshot_load,
				actor,
				_committer: committer_handle,
			}
		}
	}

	struct Restart {
		substrate: FlowSubstrate,
		health: FlowHealthRegistry,
		snapshot_load: FlowSnapshotLoad,
		actor: FlowActorHandle,
		_committer: CommitterHandle,
	}

	fn seconds(seconds: i64) -> Duration {
		Duration::from_seconds(seconds).expect("duration from seconds")
	}

	fn millis(milliseconds: i64) -> Duration {
		Duration::from_milliseconds(milliseconds).expect("duration from milliseconds")
	}

	#[test]
	fn a_wake_that_lands_during_a_commit_is_not_lost() {
		// A wake that lands mid-commit must leave a marker the commit completion honors:
		// nothing else wakes this actor, so dropping it strands the second row for good.
		let h = harness();
		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(v0);

		h.te.command("INSERT app::t [{ id: 1 }]");
		let first = h.engine.current_version().expect("current version");
		h.await_safe_watermark(first);
		h.wake(&actor);

		h.te.command("INSERT app::t [{ id: 2 }]");
		let target = h.engine.current_version().expect("current version");
		h.await_safe_watermark(target);
		h.wake(&actor);

		let rows = h.await_view_rows(2, StdDuration::from_secs(10));
		assert_eq!(
			rows, 2,
			"a wake received during a commit must schedule a follow-up pull; nothing else \
			 wakes this actor, so losing it strands the second row"
		);
		assert!(
			h.await_position_at_least(target, seconds(5)).is_some(),
			"the follow-up pull must advance the flow position to at least the safe bound at \
			 the time of the wake"
		);
		drop(actor);
	}

	#[test]
	fn a_burst_of_commits_coalesces_into_few_slices() {
		// Rows that accumulate behind an in-flight commit must ride out as one slice: the
		// per-slice envelope (transaction, DAG walk, state flush, commit) is the bulk of the
		// flow CPU bill. Nine uncoalesced wakes are nine slices; coalesced they are two.
		let h = harness();
		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(v0);

		let total = 9;
		for id in 0..total {
			h.te.command(&format!("INSERT app::t [{{ id: {id} }}]"));
			h.wake(&actor);
		}
		let target = h.engine.current_version().expect("current version");
		h.await_safe_watermark(target);
		h.wake(&actor);

		let rows = h.await_view_rows(total, StdDuration::from_secs(15));
		assert_eq!(rows, total, "coalescing must not drop an accumulated version");

		let slices = h.view_bearing_records(target);
		assert!(
			slices <= 4,
			"the rows that accumulated behind the first commit must be pulled as one slice, \
			 not one each: expected 2 view-bearing commits, tolerated up to 4, got {slices} \
			 (with no coalescing this is {total})"
		);
		drop(actor);
	}

	#[test]
	fn a_flow_behind_the_backlog_recovers_through_the_loader() {
		// A cursor the backlog has evicted past can only be recovered through durable CDC:
		// nothing may be lost to the eviction and nothing applied twice on rejoin.
		let h = harness();
		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(v0);

		let total = 12;
		for id in 0..total {
			h.te.command(&format!("INSERT app::t [{{ id: {id} }}]"));
		}
		let target = h.engine.current_version().expect("current version");
		h.await_safe_watermark(target);

		// With everything unconsumed evicted, the loader is the only path to the view.
		h.backlog.evict_below(target);
		h.wake(&actor);

		let rows = h.await_view_rows(total, StdDuration::from_secs(15));
		assert_eq!(rows, total, "the loader path must recover every version evicted from the backlog");
		assert!(
			h.await_position_at_least(target, seconds(10)).is_some(),
			"the catch-up must advance the flow position to at least the safe bound it \
			 recovered through"
		);

		sleep(StdDuration::from_millis(200));
		assert_eq!(
			h.view_rows(),
			total,
			"no version may be applied twice across the loader chunk and later backlog pulls"
		);
		drop(actor);
	}

	#[test]
	fn the_control_frontier_bounds_how_far_a_flow_may_pull() {
		// A flow that outruns the control frontier could consume versions carrying its own
		// deletion or a sibling's creation before the supervisor had scanned them.
		let h = harness();
		let v0 = h.engine.current_version().expect("current version");
		h.control.store(v0);
		let actor = h.spawn_actor_with_bounded_control(v0);

		h.te.command("INSERT app::t [{ id: 1 }]");
		h.te.command("INSERT app::t [{ id: 2 }]");
		let target = h.engine.current_version().expect("current version");
		h.await_safe_watermark(target);
		h.wake(&actor);

		sleep(StdDuration::from_millis(300));
		assert_eq!(
			h.tracker.all().get(&h.flow_id).copied(),
			Some(v0),
			"a flow must hold at the control frontier even though the safe watermark is beyond it"
		);
		assert_eq!(h.view_rows(), 0, "no row above the control frontier may materialize");

		h.control.store(target);
		h.wake(&actor);
		assert_eq!(
			h.await_view_rows(2, StdDuration::from_secs(10)),
			2,
			"raising the frontier and waking must release exactly the held-back versions"
		);
		assert_eq!(h.await_position(target, StdDuration::from_secs(5)), Some(target));
		drop(actor);
	}

	#[test]
	fn a_producer_watermark_overshoot_does_not_stall_or_overrun_the_pull() {
		// The producer watermark can transiently sit ahead of done_until. Reading past
		// done_until consumes versions whose effects are not yet visible; treating the
		// overshoot as a stall strands the row until the (1h) tick.
		let h = harness();
		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(v0);

		h.te.command("INSERT app::t [{ id: 1 }]");
		let target = h.engine.current_version().expect("current version");
		h.await_safe_watermark(target);

		// An overshoot of one would be swallowed by the flow's own slice commit, leaving an
		// unclamped pull indistinguishable from a clamped one.
		let producer = h.engine.ioc().resolve::<CdcProducerWatermark>().expect("producer watermark");
		for _ in 0..10 {
			producer.advance(CommitVersion(producer.get().0 + 1));
		}
		assert!(
			producer.get().0 >= h.engine.done_until().0 + 5,
			"test precondition: producer watermark must overshoot done_until by more than the 			 test's own commits"
		);

		h.wake(&actor);
		assert_eq!(
			h.await_view_rows(1, StdDuration::from_secs(10)),
			1,
			"an overshooting producer watermark must not stall the pull below done_until"
		);
		let done = h.engine.done_until();
		let position = h
			.await_position_at_least(target, seconds(5))
			.expect("the pull must advance at least through the insert");
		assert!(
			position <= done,
			"the pull advanced to {} which overruns done_until {}: versions beyond the done \
			 watermark are not yet safe to consume",
			position.0,
			done.0
		);
		drop(actor);
	}

	#[test]
	fn a_tick_that_commits_must_still_drain_afterwards() {
		// A tick that commits must still drain afterwards, or the generation it promoted is
		// never pruned - a leak on precisely the quietest flows. Only a pull moves the cursor
		// past the gap, so a cursor that stays put is proof none ran.
		//
		// The tick commit is manufactured through retention: the watermark derives from the
		// rows' arrival stamps, never the clock, so seals fire inline with the data that
		// carries the watermark past them and what is left for a quiet flow's tick to commit
		// is the reclaim sweep retiring the window groups those seals left behind.
		let h = harness_with(
			"CREATE TABLE app::t { id: int4, g: int4, v: int4 }",
			r#"CREATE DEFERRED VIEW app::v { g: int4, total: int8 } AS {
				FROM app::t
					| window tumbling { total: math::sum(v) }
						with { interval: "1s", grace: "0s" }
						by { g }
			}"#,
		);
		assert!(h.flow.ticks(), "a flow whose operators never tick would skip on_tick's body entirely");
		h.te.admin("CREATE TABLE app::unrelated { id: int4 }");

		let Clock::Mock(clock) = h.engine.clock() else {
			panic!("this test separates arrival stamps by hand and needs the mock clock")
		};

		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(v0);

		// Three arrivals 3s apart: each lands in its own 1s bucket and its wake seals the
		// bucket before it inline. The second seal moves the sealed-through anchor past the
		// first bucket's group, which is what gives the tick's reclaim something to retire.
		let mut target = v0;
		for (id, expected_rows) in [(1u64, 1usize), (2, 2), (3, 3)] {
			if id > 1 {
				clock.advance_secs(3);
			}
			h.te.command(&format!("INSERT app::t [{{ id: {id}, g: 1, v: 5 }}]"));
			target = h.engine.current_version().expect("current version");
			h.await_safe_watermark(target);
			h.wake(&actor);
			assert_eq!(
				h.await_view_rows(expected_rows, seconds(10).to_std()),
				expected_rows,
				"each arrival must land in its own bucket, or no sealed group is left to reclaim"
			);
			assert_eq!(
				h.await_position(target, seconds(5).to_std()),
				Some(target),
				"the wake must settle the cursor before the tick, so that any later move is \
				 the tick's doing and nothing else's"
			);
		}

		// Versions the actor is never woken for: only the tick commit's follow-up pull can
		// carry the cursor over them.
		h.te.command("INSERT app::unrelated [{ id: 1 }]");
		let gap = h.engine.current_version().expect("current version");
		h.await_safe_watermark(gap);
		assert!(gap > target, "the test needs unconsumed safe versions above the settled cursor");

		// A reclaim-only tick writes only to the arena, so arena shrinkage is the proof it
		// reached a commit.
		let arena_before = h.substrate.operators.total_bytes();
		assert!(arena_before > 0, "precondition: the sealed window groups must hold arena state to retire");
		assert!(actor.actor_ref().send(FlowActorMessage::Tick).is_ok(), "send tick");

		assert!(
			h.poll_until(seconds(10), || (h.substrate.operators.total_bytes() < arena_before)
				.then_some(()))
				.is_some(),
			"precondition: the tick must actually reach a commit - its reclaim must retire \
			 the sealed window groups from the arena. A tick that produces no output leaves \
			 `committing` false and falls straight through to on_tick's own trailing Drain, \
			 which would satisfy the assertion below for the wrong reason"
		);

		assert!(
			h.await_position_at_least(gap, seconds(10)).is_some(),
			"a tick that committed must still be drained afterwards: the cursor never reached \
			 {} though it was long safe, so the generation that tick promoted has nothing left \
			 to prune it (position is {:?})",
			gap.0,
			h.tracker.all().get(&h.flow_id).copied()
		);
		drop(actor);
	}

	#[test]
	fn a_tick_commit_must_not_pass_itself_off_as_a_checkpoint() {
		// A tick commit persists no checkpoint. Crediting it with one makes the flow believe
		// it is more durable than it is and stop checkpointing for good; flow checkpoints
		// gate CDC log compaction, so the log would then grow without bound.
		let h = harness_with(
			"CREATE TABLE app::t { id: int4, g: int4, v: int4 }",
			r#"CREATE DEFERRED VIEW app::v { g: int4, total: int8 } AS {
				FROM app::t
					| window tumbling { total: math::sum(v) }
						with { interval: "1s", grace: "0s" }
						by { g }
			}"#,
		);
		assert!(h.flow.ticks(), "a flow whose operators never tick can never commit a tick");
		h.te.admin("CREATE TABLE app::unrelated { id: int4 }");

		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(v0);

		h.te.command("INSERT app::t [{ id: 1, g: 1, v: 5 }]");
		let target = h.engine.current_version().expect("current version");
		h.await_safe_watermark(target);
		h.wake(&actor);
		assert_eq!(
			h.await_view_rows(1, seconds(10).to_std()),
			1,
			"the woken row must reach the window, or no seal timer is ever armed"
		);
		assert_eq!(
			h.await_position(target, seconds(5).to_std()),
			Some(target),
			"the wake must settle before the tick, so any later movement is the tick's doing"
		);
		assert_eq!(
			h.persisted_checkpoint(),
			Some(target),
			"baseline: the wake's slice commit carries a checkpoint at its bound, so the \
			 durable position starts where the cursor does and any later gap is a real stall"
		);

		// Versions the actor is never woken for: only going idle after the tick commits can
		// carry the cursor over them, which is also when it decides whether to checkpoint.
		h.te.command("INSERT app::unrelated [{ id: 1 }]");
		let gap = h.engine.current_version().expect("current version");
		h.await_safe_watermark(gap);
		assert!(gap > target, "the test needs unconsumed safe versions above the settled cursor");

		let Clock::Mock(clock) = h.engine.clock() else {
			panic!("this test ages the checkpoint and fires a seal timer by hand, and needs the mock clock")
		};
		clock.advance_secs(6);

		let pre_tick = h.engine.current_version().expect("current version");
		assert!(actor.actor_ref().send(FlowActorMessage::Tick).is_ok(), "send tick");

		assert!(
			h.await_version_beyond(pre_tick, seconds(10)).is_some(),
			"precondition: the tick must actually reach a commit. A tick that produces no \
			 output never touches the checkpoint bookkeeping at all, which would satisfy the \
			 assertion below for the wrong reason"
		);

		assert!(
			h.await_checkpoint_beyond(target, seconds(10)).is_some_and(|got| got >= gap),
			"a tick commit persists no checkpoint, so it must not be credited with one: the \
			 durable checkpoint never passed {} though the flow had consumed through {} and the \
			 clock was 6s past a 5s staleness bound (checkpoint is stuck at {:?})",
			target.0,
			gap.0,
			h.persisted_checkpoint()
		);
		drop(actor);
	}

	fn ring_harness(announce: bool) -> Harness {
		// An event-time ring whose capacity is never reached, so the only eviction these tests
		// can observe is the row ttl. The ttl announce flag is the variable under test.
		harness_with(
			"CREATE TABLE app::t { id: int4, v: int4, ts: datetime } with { ts: ts }",
			&format!(
				"CREATE DEFERRED RINGBUFFER VIEW app::v {{ id: int4, v: int4 }} \
				 WITH {{ capacity: 1000, time: event, row: {{ ttl: {{ duration: '1s', announce: {announce} }} }} }} \
				 AS {{ FROM app::t map {{ id, v }} }}"
			),
		)
	}

	fn drive_ring_ttl_tick(h: &Harness) -> (FlowActorHandle, CommitVersion) {
		// Lands one row at event time 60s (its 1s ttl timer becomes due at 61s), settles the
		// wake's slice - the watermark is then 60s, so the wake itself cannot fire the timer -
		// and only then carries the watermark to 120s without a batch. The tick sent last is
		// therefore the only thing that can fire the eviction, and pre_tick bounds its commit.
		assert!(h.flow.ticks(), "a ring buffer sink with a row ttl must tick, or on_tick never runs");
		let substrate = h.substrate.clone();
		let v0 = h.engine.current_version().expect("current version");
		h.control.store(CommitVersion(u64::MAX));
		let actor = h.spawn_actor_with_substrate(v0, substrate.clone());

		h.te.command(r#"INSERT app::t [{ id: 1, v: 10, ts: "1970-01-01T00:01:00Z" }]"#);
		let target = h.engine.current_version().expect("current version");
		h.await_safe_watermark(target);
		h.wake(&actor);
		assert_eq!(
			h.await_view_rows(1, StdDuration::from_secs(10)),
			1,
			"the row must land in the ring before its ttl can have anything to evict"
		);
		assert_eq!(
			h.await_position(target, StdDuration::from_secs(5)),
			Some(target),
			"the wake must settle before the tick, so the eviction is the tick's doing and \
			 nothing else's"
		);

		h.advance_source_watermarks(&substrate, DateTime::from_millis(120_000));

		let pre_tick = h.engine.current_version().expect("current version");
		assert!(actor.actor_ref().send(FlowActorMessage::Tick).is_ok(), "send tick");
		let tick_version = h
			.await_version_beyond(pre_tick, seconds(10))
			.expect("the tick must commit the eviction it fired");
		h.await_safe_watermark(tick_version);
		(actor, pre_tick)
	}

	#[test]
	fn a_tick_fired_eviction_lands_in_the_commits_change_stream() {
		// A ring row's ttl can become due with no batch in flight: the watermark covering the
		// timer arrives via restart hydration or a budget-capped dispatch, and the periodic
		// tick is what fires it. The eviction is announced, so the tick commit must carry the
		// retraction as a flow change record exactly like a batch-fired eviction would - CDC
		// and subscription consumers otherwise keep serving a row the view no longer has.
		// Falsified by dropping view_changes anywhere on the tick path (SliceComputer::tick
		// discarding the accumulator, CommitterMessage::Tick not carrying them, or apply_tick
		// not tracking them): storage still loses the row, but no record appears here.
		let h = ring_harness(true);
		let (_actor, pre_tick) = drive_ring_ttl_tick(&h);

		let change = h
			.poll_until(seconds(10), || h.view_change_beyond(pre_tick))
			.expect("the tick commit must carry a view change record for the announced eviction");
		let removes: Vec<&Diff> =
			change.diffs.iter().filter(|diff| matches!(diff, Diff::Remove { .. })).collect();
		assert_eq!(removes.len(), 1, "the eviction must surface as one Remove diff, got {:?}", change.diffs);
		if let Diff::Remove {
			pre,
			..
		} = removes[0]
		{
			assert_eq!(pre.row_count(), 1, "the retraction must carry the evicted row as its pre-image");
		}
	}

	#[test]
	fn an_unannounced_tick_eviction_stays_out_of_the_change_stream() {
		// announce: false is the per-view declaration that retention is not change: the tick
		// must still delete the expired row from storage, but nothing may reach the change
		// stream - downstream consumers of such views deliberately keep results built from
		// rows the ring has already dropped. Falsified by forcing announce_evictions to true
		// where the sink is registered (register.rs): the eviction is then routed into the
		// accumulator and a change record appears below.
		let h = ring_harness(false);
		let (_actor, pre_tick) = drive_ring_ttl_tick(&h);

		assert_eq!(
			h.poll_until(seconds(10), || (h.view_rows() == 0).then_some(())),
			Some(()),
			"precondition: the unannounced eviction must still remove the expired row from \
			 storage, otherwise there is no eviction whose silence could be asserted"
		);
		assert!(
			h.view_change_beyond(pre_tick).is_none(),
			"an announce: false eviction must leave no view change record on the tick commit"
		);
	}

	#[test]
	fn a_tick_evictions_storage_delete_carries_the_slice_paths_pre_image() {
		// Tick and slice commits share apply_pending_writes, so a Row-key remove on the tick
		// path must announce the stored row as its pre-image exactly as a slice remove would -
		// CDC consumers use that pre-image to retract without a point lookup into state that
		// no longer exists. Falsified by routing apply_tick's writes through remove_silent for
		// Row keys: the delete then either vanishes from the record or loses its pre. (The
		// once-suggested mutation to plain transaction.remove no longer falsifies anything:
		// since the remove/drop unification, remove() itself fetches and announces the
		// pre-image, which is exactly the parity this test pins.)
		let h = ring_harness(true);
		let (_actor, pre_tick) = drive_ring_ttl_tick(&h);

		let (evicted_key, evicted_pre) = h
			.poll_until(seconds(10), || {
				h.cdc_records()
					.into_iter()
					.filter(|cdc| cdc.version > pre_tick)
					.flat_map(|cdc| cdc.system_changes)
					.find_map(|sc| match sc {
						SystemChange::Delete {
							key,
							pre,
						} if matches!(Key::kind(&key), Some(KeyKind::Row)) => Some((key, pre)),
						_ => None,
					})
			})
			.expect("the tick commit must delete the expired ring row");

		let inserted_post = h
			.cdc_records()
			.into_iter()
			.filter(|cdc| cdc.version <= pre_tick)
			.flat_map(|cdc| cdc.system_changes)
			.find_map(|sc| match sc {
				SystemChange::Insert {
					key,
					post,
				} if key == evicted_key => Some(post),
				_ => None,
			})
			.expect("the wake's slice commit must have inserted the ring row the tick later evicts");

		assert_eq!(
			evicted_pre,
			Some(inserted_post),
			"a tick-path Row remove must announce the same pre-image the slice path stored"
		);
	}

	#[test]
	fn operator_state_lives_in_the_arena_and_never_reaches_the_multi_store() {
		// State must land in the arena with zero OperatorState keys in the multi store or
		// CDC (falsified by reverting the split in apply_pending_writes), and a restart with
		// an empty arena must not wedge the actor.
		let h = harness_with(
			"CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { ts: ts }",
			"CREATE DEFERRED VIEW app::v { g: int4, total: int8 } with { time: event } \
			 AS { FROM app::t AGGREGATE { total: math::count(id) } BY { g } }",
		);
		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(v0);

		h.te.command(
			r#"INSERT app::t [{id: 1, g: 1, ts: "1970-01-01T00:00:00Z"},
			                   {id: 2, g: 1, ts: "1970-01-01T00:01:00Z"},
			                   {id: 3, g: 2, ts: "1970-01-01T00:02:00Z"}]"#,
		);
		let target = h.engine.current_version().expect("current version");
		h.await_safe_watermark(target);
		h.wake(&actor);
		assert_eq!(
			h.await_view_rows(2, StdDuration::from_secs(10)),
			2,
			"the aggregate must materialize its two groups"
		);
		assert_eq!(h.await_position(target, StdDuration::from_secs(5)), Some(target));

		assert!(
			h.substrate.operators.total_bytes() > 0,
			"the aggregate's operator state must land in the shared arena"
		);

		// State shards per operator; a whole-keyspace scan never routes there.
		let query = h.engine.multi().begin_query().expect("query");
		for operator in h.flow.get_operator_ids() {
			let leaked = query
				.range(OperatorStateKey::node_range(operator), RangeScope::All, 1024)
				.collect::<Result<Vec<_>>>()
				.expect("scan the operator's state range");
			assert!(
				leaked.is_empty(),
				"the multi store must receive ZERO operator-state writes, operator {} has {}",
				operator.0,
				leaked.len()
			);
		}
		drop(query);

		let cdc_state_keys = h
			.cdc_records()
			.into_iter()
			.flat_map(|cdc| cdc.system_changes)
			.filter(|change| matches!(Key::kind(change.key()), Some(KeyKind::OperatorState)))
			.count();
		assert_eq!(cdc_state_keys, 0, "no CDC record may carry an OperatorState key either");

		let stopped = Arc::new(WaiterHandle::new());
		let notify = Arc::clone(&stopped);
		assert!(actor.actor_ref()
			.send(FlowActorMessage::Stop {
				delete_checkpoint: false,
				reply: Box::new(move || notify.notify()),
			})
			.is_ok());
		assert!(stopped.wait_timeout(seconds(10)), "the first actor must stop before the restart");

		let substrate2 =
			FlowSubstrate::with_dictionary(h.engine.dictionary_allocators(), OperatorStore::default());
		assert_eq!(substrate2.operators.total_bytes(), 0, "the restarted arena starts empty");
		let committer2 = Committer::new(
			FlowCatalog::new(h.engine.catalog()),
			h.tracker.clone(),
			FlowMaterialization::new(CdcConsumerWatermark::new(), FlowPositionTracker::new()),
			substrate2.operators.clone(),
			SnapshotPinTracker::new(),
		);
		let begin_engine = h.engine.clone();
		let begin: GroupCommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
		let committer2_handle = h.engine.spawner().spawn_flow(
			"pull-protocol-committer-restart",
			CommitterActor::new(committer2, GroupCommitHandle::inline(begin), OperatorStateBudgetHandle::default()),
		);
		let health2 = FlowHealthRegistry::new();
		let actor2 = h.engine.spawner().spawn_flow(
			"pull-protocol-flow-restart",
			FlowActor::new(FlowActorParams {
				engine: h.engine.clone(),
				committer: committer2_handle.actor_ref().clone(),
				backlog: h.backlog.clone(),
				loader: h.loader_handle.actor_ref().clone(),
				control: h.control.clone(),
				custom_operators: CustomOperators::new(HashMap::new()),
				substrate: substrate2.clone(),
				operator_samples: OperatorSampleRegistry::new(),
				state_budget: OperatorStateBudgetHandle::default(),
				clock: h.engine.clock().clone(),
				health: health2.clone(),
				flow_tracker: h.tracker.clone(),
				flow: h.flow.clone(),
				source_objects: h.source_objects.clone(),
				cursor: target,
				pull_batch_bytes: ByteSize::from_mib(8),
				load_batch_bytes: ByteSize::from_mib(8),
				checkpoint_lag: 10_000,
				checkpoint_max_age: Duration::from_milliseconds(5_000).unwrap(),
				retry_limit: 3,
				retry_backoff: Duration::from_milliseconds(50).unwrap(),
				snapshots: None,
				snapshot_load: FlowSnapshotLoad::Empty,
			}),
		);

		h.te.command(r#"INSERT app::t [{id: 4, g: 3, ts: "1970-01-01T00:03:00Z"}]"#);
		let after_restart = h.engine.current_version().expect("current version");
		h.await_safe_watermark(after_restart);
		h.wake(&actor2);

		assert!(
			h.await_position_at_least(after_restart, seconds(10)).is_some(),
			"the restarted actor must boot against an empty arena and keep consuming \
			 (position is {:?})",
			h.tracker.all().get(&h.flow_id).copied()
		);
		assert!(
			health2.poisoned().is_empty(),
			"an empty arena at boot must not poison the flow: {:?}",
			health2.poisoned()
		);
		assert!(
			substrate2.operators.total_bytes() > 0,
			"the restarted flow must rebuild its state in its own arena"
		);
		drop(actor2);
	}

	#[test]
	fn an_elapsed_snapshot_interval_persists_generations_and_advances_the_pin() {
		// End-to-end trigger wiring: once OperatorSnapshotInterval elapses on the actor's
		// clock, the next tick with no commit in flight must write a snapshot generation for
		// every stateful operator of the flow and ride a slice commit that advances the
		// flow's snapshot pin to the FLOW CURSOR every manifest was stamped with. Falsified by
		// never calling maybe_snapshot from the tick path, by gating it on ticks_enabled, by
		// not dispatching the pin commit, by reading the interval from the wrong config key,
		// or by pinning at min(upper): an arena upper is the flow's own commit version, which
		// always sits ABOVE the cursor, so pinning there lets CDC truncate the very records
		// catch-up needs.
		let mut h = harness_with(
			"CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { ts: ts }",
			"CREATE DEFERRED VIEW app::v { g: int4, total: int8 } with { time: event } \
			 AS { FROM app::t AGGREGATE { total: math::count(id) } BY { g } }",
		);
		{
			let catalog = h.engine.catalog();
			let mut admin = h.engine.begin_admin(IdentityId::system()).expect("begin admin");
			catalog.set_config(&mut admin, ConfigKey::OperatorSnapshotInterval, Value::duration_seconds(1))
				.expect("set snapshot interval");
			admin.commit().expect("commit config");
		}
		let (snapshot_config, _db_guard) = SqliteConfig::test();
		let snapshot_store = SnapshotStore::sqlite(snapshot_config);
		h.snapshots = Some(FlowSnapshots::new(
			snapshot_store.clone(),
			h.engine.single().read_store(),
			h.engine.dictionary_allocators(),
		));

		let Clock::Mock(clock) = h.engine.clock() else {
			panic!("this test drives the snapshot interval by hand and needs the mock clock")
		};

		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(v0);

		h.te.command(
			r#"INSERT app::t [{id: 1, g: 1, ts: "1970-01-01T00:00:00Z"},
			                   {id: 2, g: 2, ts: "1970-01-01T00:01:00Z"}]"#,
		);
		let target = h.engine.current_version().expect("current version");
		h.await_safe_watermark(target);
		h.wake(&actor);
		assert_eq!(h.await_view_rows(2, StdDuration::from_secs(10)), 2, "the aggregate must materialize");
		assert_eq!(h.await_position(target, StdDuration::from_secs(5)), Some(target));

		let stateful: Vec<OperatorId> = h
			.flow
			.get_operator_ids()
			.filter(|id| h.substrate.operators.upper(*id) > CommitVersion(0))
			.collect();
		assert!(!stateful.is_empty(), "precondition: the aggregate flow must hold committed operator state");

		// Repeated ticks because a tick whose body commits sets `committing` and skips the
		// snapshot; only a quiet tick after the interval elapsed can run it.
		clock.advance_secs(2);
		let snapshotted = h.poll_until(seconds(10), || {
			assert!(actor.actor_ref().send(FlowActorMessage::Tick).is_ok(), "send tick");
			sleep(StdDuration::from_millis(20));
			stateful.iter()
				.all(|id| !snapshot_store.generations(*id).expect("generations").is_empty())
				.then_some(())
		});
		assert!(
			snapshotted.is_some(),
			"every stateful operator must gain a snapshot generation once the interval elapses"
		);

		let pin = h
			.poll_until(seconds(10), || {
				let mut query = h.engine.begin_query(IdentityId::system()).expect("query");
				CdcCheckpoint::fetch_row(&mut Transaction::Query(&mut query), &FlowSnapshotPin(h.flow_id))
					.expect("fetch pin")
			})
			.expect("the completed snapshot must advance the flow's pin through a slice commit");
		for id in &stateful {
			let cursors = snapshot_store.generation_cursors(*id).expect("generation cursors");
			assert!(
				cursors.iter().all(|(_, cursor)| *cursor == pin.version),
				"operator {} recorded cursors {:?}, but the pin sits at {}: pin and manifest must be \
				 the same flow cursor or catch-up resumes from a version CDC no longer covers",
				id.0,
				cursors,
				pin.version.0
			);
			assert_ne!(
				h.substrate.operators.upper(*id),
				pin.version,
				"the arena upper is the flow's own commit version and must not be what the pin \
				 records, or this test could not tell the two version spaces apart"
			);
		}
		assert!(
			h.tracker.all().get(&h.flow_id).is_some_and(|position| *position >= pin.version),
			"the pin must be a CDC position the flow has actually consumed"
		);
		assert_eq!(pin.class, ConsumerClass::Pinning, "an Ephemeral pin would not bound CDC truncation");
		drop(actor);
	}

	/// Runs the shared catch-up scenario: fill state, snapshot at S, keep committing to C, stop,
	/// then restart against a fresh arena so the gap (S, C] can only be closed by replay.
	/// Returns the reference state of the uninterrupted run and the checkpoint it reached.
	fn drive_to_snapshot_and_gap(
		h: &Harness,
		fill: &[&str],
		gap: &[&str],
	) -> (State, CommitVersion, FlowActorHandle) {
		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(v0);

		for rql in fill {
			h.te.command(rql);
		}
		let snapshot_at = h.engine.current_version().expect("current version");
		h.await_safe_watermark(snapshot_at);
		h.wake(&actor);
		assert_eq!(
			h.await_position(snapshot_at, StdDuration::from_secs(10)),
			Some(snapshot_at),
			"the fill must settle before the snapshot, or the snapshot records a cursor the arena \
			 has not caught up to"
		);
		let pin = h.write_snapshot(snapshot_at);
		assert_eq!(pin, snapshot_at);

		for rql in gap {
			h.te.command(rql);
		}
		let checkpoint = h.engine.current_version().expect("current version");
		h.await_safe_watermark(checkpoint);
		h.wake(&actor);
		assert_eq!(
			h.await_position(checkpoint, StdDuration::from_secs(10)),
			Some(checkpoint),
			"the uninterrupted run must consume the whole gap; it is the reference every replay \
			 is compared against"
		);

		let reference = h.arena_state(&h.substrate);
		assert!(!reference.is_empty(), "precondition: the reference run must have built arena state");
		(reference, checkpoint, actor)
	}

	fn aggregate_rows(ids: std::ops::RangeInclusive<u32>) -> Vec<String> {
		ids.map(|id| {
			format!(
				r#"INSERT app::t [{{id: {id}, g: {}, ts: "1970-01-01T00:{id:02}:00Z"}}]"#,
				id % 3
			)
		})
		.collect()
	}

	fn aggregate_fill() -> Vec<String> {
		aggregate_rows(1..=4)
	}

	fn aggregate_gap() -> Vec<String> {
		aggregate_rows(5..=12)
	}

	fn as_rql(owned: &[String]) -> Vec<&str> {
		owned.iter().map(String::as_str).collect()
	}

	#[test]
	fn catch_up_rebuilds_state_byte_identically() {
		// The whole point of the chunk: a flow that boots from a snapshot taken at S while its
		// durable checkpoint sits at C must replay (S, C] into the arena, and land where an
		// uninterrupted run landed. Booting at C with state from S is silent corruption - the
		// aggregate would be missing every row in the gap and no error would ever surface.
		// Falsified by skipping the replay loop (returning false from begin_catch_up): the
		// restarted arena then holds only the four fill rows' state.
		let (h, store) = aggregate_harness();
		let fill = aggregate_fill();
		let gap = aggregate_gap();
		let (reference, checkpoint, actor) =
			drive_to_snapshot_and_gap(&h, &as_rql(&fill), &as_rql(&gap));
		h.stop(&actor);

		let restart = h.restart(checkpoint, ByteSize::from_mib(8));
		assert!(
			matches!(restart.snapshot_load, FlowSnapshotLoad::Restored(_)),
			"precondition: the restart must load the snapshot, not boot empty ({:?})",
			restart.snapshot_load
		);
		h.await_catch_up(&store, checkpoint);
		assert!(restart.health.poisoned().is_empty(), "a covered gap must not poison: {:?}", restart.health.poisoned());

		assert_batch_equivalent("aggregate catch-up", &reference, &h.arena_state(&restart.substrate));
		drop(restart);
	}

	#[test]
	fn the_ring_sink_metadata_mirror_tracks_the_mvcc_row() {
		// The ringbuffer metadata row is routed to an UNPINNED state handle, so during a replay
		// it would answer with the post-crash head/tail and the sink would assign storage row
		// numbers the live run never assigned. The arena mirror is what makes that read
		// replayable, and it is only worth anything if it holds the SAME numbers the mvcc row
		// does - through inserts and through the capacity evictions that move head. Falsified by
		// dropping the mirror write from write_metadata (nothing to find in the arena) or by
		// writing it only on the insert path and not on the eviction path (head drifts apart).
		let h = harness_with(
			"CREATE TABLE app::t { id: int4, v: int4, ts: datetime } with { ts: ts }",
			"CREATE DEFERRED RINGBUFFER VIEW app::v { id: int4, v: int4 } \
			 WITH { capacity: 3, time: event } AS { FROM app::t map { id, v } }",
		);
		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(v0);

		// More rows than the capacity, one wake each, so head moves under real evictions rather
		// than sitting at its initial value.
		for id in 1..=6u32 {
			h.te.command(&format!(
				r#"INSERT app::t [{{ id: {id}, v: {id}, ts: "1970-01-01T00:{id:02}:00Z" }}]"#
			));
			let target = h.engine.current_version().expect("current version");
			h.await_safe_watermark(target);
			h.wake(&actor);
			assert_eq!(
				h.await_position(target, StdDuration::from_secs(10)),
				Some(target),
				"each row must settle, or the eviction it triggers has not run yet"
			);
		}
		assert_eq!(h.view_rows(), 3, "precondition: the ring must be at capacity, so head has moved");

		let stored = {
			let query = h.engine.multi().begin_query().expect("query");
			let rows: Vec<_> = query
				.range(EncodedKeyRange::all(), RangeScope::All, 100_000)
				.collect::<Result<Vec<_>>>()
				.expect("scan for the ringbuffer metadata row")
				.into_iter()
				.filter(|row| Key::kind(&row.key) == Some(KeyKind::RingBufferMetadata))
				.collect();
			assert_eq!(rows.len(), 1, "the test view owns exactly one unpartitioned metadata row");
			decode_ringbuffer_metadata(&rows[0].row)
		};

		let shape = RowShape::operator_state();
		let mirrored: Vec<RingBufferMetadata> = h
			.flow
			.get_operator_ids()
			.flat_map(|operator| {
				h.substrate
					.operators
					.range_batch(
						operator,
						EncodedKeyRange::new(Bound::Unbounded, Bound::Unbounded),
						u64::MAX,
					)
					.items
					.into_iter()
					.filter(|(key, _)| {
						OperatorGroupStateKey::decode_inner(key.as_slice())
							.is_some_and(|(_, ks, _)| ks == Keyspace::RINGBUFFER_META)
					})
					.map(|(_, row)| {
						let blob = shape.get_blob(&row, 0);
						decode_ringbuffer_metadata(&EncodedRow(CowVec::new(
							blob.as_bytes().to_vec(),
						)))
					})
					.collect::<Vec<_>>()
			})
			.collect();

		assert_eq!(mirrored.len(), 1, "the sink must keep exactly one mirror for its global metadata");
		assert_eq!(
			mirrored[0], stored,
			"the arena mirror and the mvcc row must carry the same capacity, count, head and tail: \
			 the mirror is what a replay reads, so any drift makes the replay assign different \
			 storage row numbers than the live run did"
		);
		assert!(mirrored[0].head > 1, "precondition: evictions must have moved head off its initial value");
		drop(actor);
	}

	#[test]
	fn catch_up_writes_no_view_rows_and_no_change_records() {
		// Replay recomputes state from input the flow already committed for, so its view rows and
		// change records are already durable. Re-emitting them would double-write every row in
		// the gap and hand every CDC and subscription consumer a second copy of changes they
		// already saw. Falsified by dropping the suppression (committing the replay's pending
		// instead of applying only its operator-state keys).
		let (h, store) = aggregate_harness();
		let fill = aggregate_fill();
		let gap = aggregate_gap();
		let (reference, checkpoint, actor) = drive_to_snapshot_and_gap(&h, &as_rql(&fill), &as_rql(&gap));
		let rows_before = h.view_rows();
		let version_before = h.engine.current_version().expect("current version");
		h.stop(&actor);

		let restart = h.restart(checkpoint, ByteSize::from_mib(8));
		h.await_catch_up(&store, checkpoint);
		assert_batch_equivalent(
			"precondition: the replay must have rebuilt the arena, so the silence below is about \
			 view output and not about doing nothing at all",
			&reference,
			&h.arena_state(&restart.substrate),
		);

		// Actively hunt for a violation rather than sampling once: a replay that did commit its
		// output would land asynchronously, and a single sample right after completion could miss
		// it. Finding nothing over the whole window is the assertion.
		let offender = h.poll_until(seconds(2), || {
			h.cdc_records()
				.into_iter()
				.filter(|cdc| cdc.version > version_before)
				.find(|cdc| {
					!cdc.changes.is_empty()
						|| cdc.system_changes.iter().any(|change| {
							matches!(Key::kind(change.key()), Some(KeyKind::Row))
						})
				})
				.map(|cdc| (cdc.version, cdc.changes.len(), cdc.system_changes.len()))
		});
		assert!(
			offender.is_none(),
			"catch-up must write no view rows and emit no flow change records - every version in \
			 the gap was already committed once, and a second copy re-writes rows and re-notifies \
			 every cdc and subscription consumer. Offending commit: {offender:?}"
		);
		assert_eq!(h.view_rows(), rows_before, "and the view itself must be untouched");
		drop(restart);
	}

	#[test]
	fn a_crash_during_catch_up_leaves_the_snapshot_alone_and_the_retry_still_lands() {
		// Catch-up must never publish a snapshot of half-replayed state: a snapshot written mid
		// replay would be stamped with the flow's CHECKPOINT while the arena only holds state
		// through the replay cursor, so the next boot would replay nothing and keep the hole
		// forever. A crash mid catch-up must therefore leave the old generation newest, and a
		// fresh attempt must still land on the reference. Falsified by removing `catching_up`
		// from maybe_snapshot's guard: the tick sent below then publishes the partial arena at
		// the checkpoint, the second restart loads THAT and replays nothing, and the comparison
		// at the end fails.
		let (h, store) = aggregate_harness();
		let fill = aggregate_fill();
		let gap = aggregate_gap();
		let (reference, checkpoint, actor) = drive_to_snapshot_and_gap(&h, &as_rql(&fill), &as_rql(&gap));
		let snapshot_cursor = match h.snapshots.as_ref().map(|_| ()) {
			Some(()) => {
				let ids: Vec<OperatorId> = h.flow.get_operator_ids().collect();
				store.generation_cursors(ids[0]).expect("cursors").first().expect("a generation").1
			}
			None => unreachable!("the harness installs snapshots"),
		};
		h.stop(&actor);

		let Clock::Mock(clock) = h.engine.clock() else {
			panic!("this test ages the snapshot interval by hand and needs the mock clock")
		};

		// One CDC record per loader chunk, so the gap takes many round trips and a tick can land
		// while the replay is still in flight.
		let interrupted = h.restart(checkpoint, ByteSize::from_bytes(1));
		assert!(
			matches!(interrupted.snapshot_load, FlowSnapshotLoad::Restored(_)),
			"precondition: the interrupted attempt must have had a snapshot to resume from"
		);
		// After the spawn, never before: the actor stamps last_snapshot_at in init, so a clock
		// advance made earlier is erased and the tick below would skip the interval check for a
		// reason that has nothing to do with catching up.
		clock.advance_secs(5);
		h.poll_until(seconds(10), || (interrupted.substrate.operators.total_bytes() > 0).then_some(()));
		assert!(interrupted.actor.actor_ref().send(FlowActorMessage::Tick).is_ok(), "send tick");
		h.stop(&interrupted.actor);
		drop(interrupted);

		// Checked per operator, not just through the load: a mid-replay snapshot may cover only
		// the operators the partial replay happened to touch, and the consistent-set load would
		// then quietly fall back and hide it. The gate is what must stop it being written at all.
		for id in h.flow.get_operator_ids() {
			let cursors = store.generation_cursors(id).expect("generation cursors");
			assert!(
				cursors.iter().all(|(_, cursor)| *cursor <= snapshot_cursor),
				"operator {} gained a generation past {} during catch-up ({:?}): a snapshot taken \
				 mid-replay is stamped with the flow's CHECKPOINT while the arena only holds state \
				 through the replay cursor, so the next boot would replay nothing and keep the hole",
				id.0,
				snapshot_cursor.0,
				cursors
			);
		}

		let retry = h.restart(checkpoint, ByteSize::from_mib(8));
		assert_eq!(
			retry.snapshot_load,
			FlowSnapshotLoad::Restored(snapshot_cursor),
			"the interrupted attempt must have published nothing: the newest generation must still \
			 be the one taken before the crash, or the retry resumes from a cursor whose state was \
			 never fully rebuilt"
		);
		h.await_catch_up(&store, checkpoint);
		assert_batch_equivalent("catch-up retry after a crash", &reference, &h.arena_state(&retry.substrate));
		drop(retry);
	}

	#[test]
	fn an_unrecoverable_gap_poisons_the_flow() {
		// If CDC no longer covers the snapshot's cursor there is no way to rebuild the gap. The
		// flow must freeze loudly with its views still readable rather than resume from state
		// that is missing every version in the hole. Falsified by falling through to a normal
		// boot when the truncation floor is above the snapshot cursor.
		let (h, _store) = aggregate_harness();
		let fill = aggregate_fill();
		let gap = aggregate_gap();
		let (_reference, checkpoint, actor) = drive_to_snapshot_and_gap(&h, &as_rql(&fill), &as_rql(&gap));
		let rows_before = h.view_rows();
		h.stop(&actor);

		h.engine.cdc_store()
			.drop_before(checkpoint, usize::MAX)
			.expect("truncate cdc past the snapshot cursor");

		let restart = h.restart(checkpoint, ByteSize::from_mib(8));
		assert!(
			h.poll_until(seconds(10), || (!restart.health.poisoned().is_empty()).then_some(())).is_some(),
			"a flow whose catch-up window has been truncated away must poison, not resume"
		);
		assert_eq!(
			h.view_rows(),
			rows_before,
			"a poisoned flow keeps serving what it already materialized; nothing may be rebuilt or lost"
		);
		drop(restart);
	}

	#[test]
	fn catch_up_snapshots_immediately_on_completion() {
		// A crash loop must not replay the same window on every boot: the moment catch-up
		// finishes, the rebuilt arena has to become the newest generation, stamped at the cursor
		// it is now caught up to. Falsified by removing the snapshot_now call from
		// finish_catch_up: the second restart below then reports the OLD cursor and replays the
		// whole gap again.
		let (h, store) = aggregate_harness();
		let fill = aggregate_fill();
		let gap = aggregate_gap();
		let (reference, checkpoint, actor) = drive_to_snapshot_and_gap(&h, &as_rql(&fill), &as_rql(&gap));
		h.stop(&actor);

		let first = h.restart(checkpoint, ByteSize::from_mib(8));
		let FlowSnapshotLoad::Restored(before) = first.snapshot_load else {
			panic!("precondition: the first restart must resume from a snapshot")
		};
		let ids: Vec<OperatorId> = h.flow.get_operator_ids().collect();
		assert!(
			h.poll_until(seconds(15), || {
				let cursors = store.generation_cursors(ids[0]).expect("cursors");
				cursors.first().filter(|(_, cursor)| *cursor > before).map(|_| ())
			})
			.is_some(),
			"a completed catch-up must publish a snapshot at the cursor it caught up to"
		);
		h.stop(&first.actor);
		drop(first);

		let second = h.restart(checkpoint, ByteSize::from_mib(8));
		assert_eq!(
			second.snapshot_load,
			FlowSnapshotLoad::Restored(checkpoint),
			"the second boot must resume at the checkpoint itself, with nothing left to replay"
		);
		assert_batch_equivalent(
			"post-catch-up snapshot",
			&reference,
			&h.arena_state(&second.substrate),
		);
		drop(second);
	}
}
