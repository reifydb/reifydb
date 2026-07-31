// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeSet, VecDeque},
	mem::take,
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
			object::ObjectId,
		},
		cdc::Cdc,
	},
	lifecycle::metrics::RetentionMetrics,
	state::budget::OperatorStateBudgetHandle,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_flow::transaction::substrate::FlowSubstrate;
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
use tracing::{debug, error, warn};

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
	operator::metrics::OperatorSampleRegistry,
};

const MAX_BUFFERED_INGESTS: usize = 32;

struct BufferedIngest {
	cdcs: Arc<Vec<Cdc>>,
	covers_from: CommitVersion,
	up_to: CommitVersion,
}

pub struct FlowActorParams {
	pub engine: StandardEngine,
	pub committer: ActorRef<CommitterMessage>,
	pub cdc_store: CdcStore,
	pub custom_operators: CustomOperators,
	pub substrate: FlowSubstrate,
	pub operator_samples: OperatorSampleRegistry,
	pub state_budget: OperatorStateBudgetHandle,
	pub retention_metrics: RetentionMetrics,
	pub clock: Clock,
	pub health: FlowHealthRegistry,
	pub flow_tracker: FlowPositionTracker,
	pub flow: FlowDag,
	pub source_objects: Arc<BTreeSet<ObjectId>>,
	pub cursor: CommitVersion,
	pub chunk_size: u64,
	pub checkpoint_lag: u64,
	pub checkpoint_max_age: Duration,
	pub retry_limit: u32,
	pub retry_backoff: Duration,
}

pub struct FlowActor {
	engine: StandardEngine,
	committer: ActorRef<CommitterMessage>,
	cdc_store: CdcStore,
	custom_operators: CustomOperators,
	substrate: FlowSubstrate,
	operator_samples: OperatorSampleRegistry,
	state_budget: OperatorStateBudgetHandle,
	retention_metrics: RetentionMetrics,
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
	checkpoint_max_age: Duration,
	initial_source_objects: Arc<BTreeSet<ObjectId>>,
	initial_cursor: CommitVersion,
}

pub struct FlowActorState {
	flow_engine: FlowEngineInner,
	source_objects: Arc<BTreeSet<ObjectId>>,
	cursor: CommitVersion,
	durable_cursor: CommitVersion,
	committing: bool,
	wake_pending: bool,
	buffered: VecDeque<BufferedIngest>,
	poisoned: bool,
	retry_count: u32,
	overlay: FlowWriteOverlay,
	overlay_promotes: u64,
	drain_after_commit: bool,
	last_checkpoint_at: u64,
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
			substrate: params.substrate,
			operator_samples: params.operator_samples,
			state_budget: params.state_budget,
			retention_metrics: params.retention_metrics,
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
		}
	}

	fn tick_interval(&self) -> Duration {
		self.engine.catalog().get_config_duration(ConfigKey::FlowTick)
	}

	fn sample_interval(&self) -> Option<Duration> {
		self.engine.catalog().get_config_duration_opt(ConfigKey::FlowSampleInterval)
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
		let mut engine = FlowEngineInner::new(
			self.engine.catalog(),
			self.engine.executor(),
			self.engine.event_bus().clone(),
			RuntimeContext::with_clock(self.clock.clone()),
			self.custom_operators.clone(),
			self.substrate.clone(),
			self.operator_samples.clone(),
			self.state_budget.clone(),
		);
		engine.adopt_retention_metrics(self.retention_metrics.clone());
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
		let now = self.clock.now().to_millis();
		if now.saturating_sub(state.last_checkpoint_at) < self.checkpoint_max_age.to_std().as_millis() as u64 {
			return;
		}
		state.last_checkpoint_at = now;

		let advance_to = state.cursor;
		let mut slice = FlowSlice::empty();
		slice.checkpoints.push((self.flow_id, advance_to));
		self.dispatch_commit(state, ctx, slice, advance_to, false);
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
				source_objects: &state.source_objects,
				cursor: state.cursor,
				durable_cursor: state.durable_cursor,
			},
			&self.config,
			&mut state.overlay,
		);
		match step {
			Ok(SliceStep::Idle) => {
				state.retry_count = 0;
				self.checkpoint_if_stale(state, ctx);
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
			if state.wake_pending {
				return;
			}
			if state.buffered.len() >= MAX_BUFFERED_INGESTS {
				state.buffered.clear();
				state.wake_pending = true;
				return;
			}
			state.buffered.push_back(BufferedIngest {
				cdcs,
				covers_from,
				up_to,
			});
			return;
		}
		self.consume_pushed(state, ctx, vec![cdcs], covers_from, up_to);
	}

	fn consume_pushed(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		segments: Vec<Arc<Vec<Cdc>>>,
		covers_from: CommitVersion,
		up_to: CommitVersion,
	) {
		if state.cursor >= up_to {
			return;
		}
		if state.cursor < covers_from {
			state.buffered.clear();
			let _ = ctx.self_ref().send(FlowActorMessage::Drain);
			return;
		}

		let step = self.computer.step_pushed(
			&mut state.flow_engine,
			&segments,
			SliceCursor {
				flow_id: self.flow_id,
				source_objects: &state.source_objects,
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

	fn replay_buffered(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>) {
		while !state.poisoned && !state.committing {
			let Some(first) = state.buffered.pop_front() else {
				return;
			};
			let covers_from = first.covers_from;
			let mut up_to = first.up_to;
			let mut segments = vec![first.cdcs];
			while let Some(next) = state.buffered.front() {
				if next.covers_from > up_to {
					break;
				}
				let next = state.buffered.pop_front().expect("front just checked");
				up_to = next.up_to;
				segments.push(next.cdcs);
			}
			self.consume_pushed(state, ctx, segments, covers_from, up_to);
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
			state.overlay_promotes += 1;
		}
		match result {
			Ok(()) => {
				state.retry_count = 0;
				state.cursor = advance_to;
				state.durable_cursor = advance_to;
				state.last_checkpoint_at = self.clock.now().to_millis();
				self.publish_position(advance_to);
				if state.wake_pending {
					state.wake_pending = false;
					state.buffered.clear();
					let _ = ctx.self_ref().send(FlowActorMessage::Drain);
				} else if !state.buffered.is_empty() {
					self.replay_buffered(state, ctx);
				} else if more || take(&mut state.drain_after_commit) {
					let _ = ctx.self_ref().send(FlowActorMessage::Drain);
				}
			}
			Err(e) => {
				state.buffered.clear();
				self.retry_or_poison(state, ctx, format!("slice commit failed: {e}"));
			}
		}
	}

	fn on_tick(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>) {
		if self.ticks_enabled && !state.poisoned && !state.committing {
			let timestamp = DateTime::from_millis(self.clock.now().to_millis());
			match self.computer.tick(&mut state.flow_engine, self.flow_id, timestamp, state.durable_cursor)
			{
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

		debug!(
			flow_id = self.flow_id.0,
			overlay_generations = state.overlay.generations_len(),
			overlay_entries = state.overlay.entry_count(),
			overlay_promotes = state.overlay_promotes,
			overlay_prunes = state.overlay.pruned_total(),
			"flow overlay depth"
		);

		if !state.poisoned && !state.committing {
			let _ = ctx.self_ref().send(FlowActorMessage::Drain);
		}
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
	) {
		state.committing = true;
		state.drain_after_commit = true;
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
		if let Some(interval) = self.sample_interval() {
			ctx.schedule_once(interval, || FlowActorMessage::Sample);
		}
		if !poisoned {
			let _ = ctx.self_ref().send(FlowActorMessage::Drain);
		}

		FlowActorState {
			flow_engine,
			source_objects: self.initial_source_objects.clone(),
			cursor: self.initial_cursor,
			durable_cursor: self.initial_cursor,
			committing: false,
			wake_pending: false,
			buffered: VecDeque::new(),
			poisoned,
			retry_count: 0,
			overlay: FlowWriteOverlay::new(),
			overlay_promotes: 0,
			drain_after_commit: false,
			last_checkpoint_at: self.clock.now().to_millis(),
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
mod ingest_replay {
	use std::{
		collections::HashMap,
		ops::Bound,
		thread::sleep,
		time::{Duration as StdDuration, Instant},
	};

	use reifydb_cdc::consume::watermark::CdcConsumerWatermark;
	use reifydb_core::{actors::flow::FlowActorHandle, interface::change::ChangeOrigin};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_transaction::{
		group::{GroupCommitBegin, GroupCommitHandle},
		transaction::Transaction,
	};
	use reifydb_value::value::Value;

	use super::*;
	use crate::{
		catalog::FlowCatalog,
		deferred::{
			committer::{Committer, CommitterActor, CommitterHandle},
			quiescence::FlowMaterialization,
			routing,
		},
	};

	struct Harness {
		te: TestEngine,
		engine: StandardEngine,
		tracker: FlowPositionTracker,
		committer_handle: CommitterHandle,
		flow: FlowDag,
		flow_id: FlowId,
		source_objects: Arc<BTreeSet<ObjectId>>,
	}

	// One deferred view over app::t, a real committer actor behind a 100ms group-commit linger
	// (the linger is the window that keeps the flow actor in `committing` while further pushes
	// arrive), and FLOW_TICK set to 1h so only pushes can advance the actor under test.
	fn harness() -> Harness {
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

		let mut probe = FlowEngineInner::new(
			engine.catalog(),
			engine.executor(),
			engine.event_bus().clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			CustomOperators::new(HashMap::new()),
			FlowSubstrate::with_dictionary(engine.dictionary_allocators()),
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
			"ingest-replay-committer",
			CommitterActor::new(committer, group, OperatorStateBudgetHandle::default()),
		);

		Harness {
			te,
			engine,
			tracker,
			committer_handle,
			flow,
			flow_id,
			source_objects,
		}
	}

	impl Harness {
		// `init` enqueues a Drain that runs lazily on a pool worker, so the caller cannot know when
		// it lands. Over the empty private store two of these tests use, that Drain is not a no-op:
		// it finds nothing in (cursor, safe], takes those versions to carry no relevant CDC, and
		// skips the cursor to the engine's safe watermark, silently swallowing every later push whose
		// up_to sits below it. Under load the worker's first batch runs late enough to land after the
		// test's writes are already safe, and those pushes evaporate.
		//
		// So spawn one version short of `cursor` and block until that Drain has skipped us up to it.
		// The published position is proof the Drain was consumed, and it is consumed while the safe
		// watermark is still pinned at `cursor` (nothing has been written yet), so the actor settles
		// exactly where the caller asked. Only pushes can move it from here: the tick is an hour out,
		// nothing sends Wake, and a pushed step never reports `more`.
		fn spawn_actor(&self, cdc_store: CdcStore, cursor: CommitVersion) -> FlowActorHandle {
			self.await_safe_watermark(cursor);

			let handle = self.engine.spawner().spawn_flow(
				"ingest-replay-flow",
				FlowActor::new(FlowActorParams {
					engine: self.engine.clone(),
					committer: self.committer_handle.actor_ref().clone(),
					cdc_store,
					custom_operators: CustomOperators::new(HashMap::new()),
					substrate: FlowSubstrate::with_dictionary(self.engine.dictionary_allocators()),
					operator_samples: OperatorSampleRegistry::new(),
					state_budget: OperatorStateBudgetHandle::default(),
					retention_metrics: RetentionMetrics::new(),
					clock: self.engine.clock().clone(),
					health: FlowHealthRegistry::new(),
					flow_tracker: self.tracker.clone(),
					flow: self.flow.clone(),
					source_objects: self.source_objects.clone(),
					cursor: CommitVersion(cursor.0 - 1),
					chunk_size: 1000,
					checkpoint_lag: 10_000,
					checkpoint_max_age: Duration::from_milliseconds(5_000).unwrap(),
					retry_limit: 3,
					retry_backoff: Duration::from_milliseconds(50).unwrap(),
				}),
			);

			assert_eq!(
				self.await_position(cursor, StdDuration::from_secs(10)),
				Some(cursor),
				"the init Drain must be consumed, and the cursor settled at {}, before the test \
				 writes anything a push will carry",
				cursor.0
			);
			handle
		}

		// CDC production is async; poll the engine's real store until the expected records exist.
		fn harvest(&self, from_exclusive: CommitVersion, to_inclusive: CommitVersion, want: usize) -> Vec<Cdc> {
			let store = self.engine.cdc_store();
			let deadline = Instant::now() + StdDuration::from_secs(10);
			loop {
				let batch = store
					.read_range(
						Bound::Excluded(from_exclusive),
						Bound::Included(to_inclusive),
						1000,
					)
					.expect("read range");
				if batch.items.len() >= want {
					return batch.items;
				}
				assert!(Instant::now() < deadline, "CDC producer never produced {want} records");
				sleep(StdDuration::from_millis(5));
			}
		}

		fn view_rows(&self) -> usize {
			self.te.query("FROM app::v").first().map(|f| f.row_count()).unwrap_or(0)
		}

		// One commit carrying view changes is one slice: this flow's slices never overlap, so
		// group commit cannot merge two of them into a single version.
		fn view_bearing_records(&self, up_to: CommitVersion) -> usize {
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

		// The same bound `step` reads up to. Covering `want` before the actor is spawned is what
		// makes its init Drain skip to exactly `want`: below it that Drain returns Idle, which
		// publishes no position and schedules no follow-up, so the spawn would wait forever.
		fn await_safe_watermark(&self, want: CommitVersion) {
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

		// Deadlines come off Clock::testing(), which is the real clock on a native build and a
		// simulator-driven mock under DST, so these waits stay deterministic there instead of
		// pinning the test to wall time. It must NOT be the engine's clock: that one is a mock
		// frozen at 1s which the tick test advances by hand to fire a seal timer, so an elapsed
		// check against it would never move and a regression would hang rather than fail.
		// Returns None on timeout so the caller asserts on the timeout rather than on a stale read.
		fn poll_until<T>(&self, timeout: Duration, mut probe: impl FnMut() -> Option<T>) -> Option<T> {
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

		// A Drain that skips lands on whatever is safe at that instant, which is at least the
		// caller's floor but not exactly it, so the tick test cannot compare for equality the way
		// the others do.
		fn await_position_at_least(&self, floor: CommitVersion, timeout: Duration) -> Option<CommitVersion> {
			self.poll_until(timeout, || {
				self.tracker.all().get(&self.flow_id).copied().filter(|got| *got >= floor)
			})
		}

		fn await_version_beyond(&self, floor: CommitVersion, timeout: Duration) -> Option<CommitVersion> {
			self.poll_until(timeout, || self.engine.current_version().ok().filter(|got| *got > floor))
		}
	}

	fn seconds(seconds: i64) -> Duration {
		Duration::from_seconds(seconds).expect("duration from seconds")
	}

	fn millis(milliseconds: i64) -> Duration {
		Duration::from_milliseconds(milliseconds).expect("duration from milliseconds")
	}

	fn send_ingest(actor: &FlowActorHandle, cdcs: Vec<Cdc>, covers_from: CommitVersion, up_to: CommitVersion) {
		let sent = actor
			.actor_ref()
			.send(FlowActorMessage::Ingest {
				cdcs: Arc::new(cdcs),
				covers_from,
				up_to,
			})
			.is_ok();
		assert!(sent, "send ingest");
	}

	#[test]
	fn push_during_commit_is_replayed_not_redrained() {
		// A push that lands while the actor is committing must be buffered and replayed through
		// step_pushed after CommitDone. The actor under test gets an EMPTY private CDC store, so the
		// pushed batches are the only possible source of data and the 1h tick cannot drain anything:
		// if the second push were downgraded to a post-commit Drain (the old wake_pending behavior),
		// the Drain would read the empty store, skip the cursor to the safe watermark, and the second
		// row could never materialize.
		let h = harness();
		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(CdcStore::memory(), v0);

		h.te.command("INSERT app::t [{ id: 1 }]");
		h.te.command("INSERT app::t [{ id: 2 }]");
		let target = h.engine.current_version().expect("current version");

		let items = h.harvest(v0, target, 2);
		let first = items[0].clone();
		let first_version = first.version;
		let rest: Vec<Cdc> = items[1..].to_vec();
		let last_version = rest.last().expect("second record").version;

		// The first push dispatches a slice commit; the 100ms group linger keeps the actor in
		// `committing` while the second push arrives, forcing it through the replay buffer.
		send_ingest(&actor, vec![first], v0, first_version);
		send_ingest(&actor, rest, first_version, last_version);

		let rows = h.await_view_rows(2, StdDuration::from_secs(10));
		assert_eq!(
			rows, 2,
			"a push received during a commit must be replayed after CommitDone; the actor's \
			 store is empty, so falling back to Drain loses the second push"
		);
		assert_eq!(
			h.await_position(last_version, StdDuration::from_secs(5)),
			Some(last_version),
			"the replayed push must advance the flow position to its up_to"
		);
		drop(actor);
	}

	#[test]
	fn pushes_queued_behind_a_commit_are_replayed_as_one_slice() {
		// Pushes that queue up behind an in-flight commit must be replayed as ONE slice, not one
		// slice each. A slice is not free: it pays a transaction, a DAG walk, a state flush and a
		// commit, and at ~30 versions/s fanned out over ~100 flows that per-slice envelope is the
		// bulk of the flow CPU bill. Merging what is already queued costs no latency (nothing waits
		// that was not already waiting) and it is what makes the actor degrade gracefully under a
		// burst: the busier the ingest, the more versions ride on one slice.
		//
		// The count is exact but the timing is not: this flow's slices are strictly sequential (the
		// committing flag gates the next one on CommitDone), so group commit can never merge them
		// and one view-bearing CDC record is exactly one slice. Nine pushes with no coalescing are
		// nine slices; coalesced they are two (the first push, then the eight that queued behind it).
		// The bound is loose enough to tolerate a push that races in after CommitDone and starts its
		// own slice, and still fails loudly if coalescing is gone.
		let h = harness();
		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(CdcStore::memory(), v0);

		let total = 9;
		for id in 0..total {
			h.te.command(&format!("INSERT app::t [{{ id: {id} }}]"));
		}
		let target = h.engine.current_version().expect("current version");
		let items = h.harvest(v0, target, total);

		let mut covers_from = v0;
		for item in items {
			let up_to = item.version;
			send_ingest(&actor, vec![item], covers_from, up_to);
			covers_from = up_to;
		}

		let rows = h.await_view_rows(total, StdDuration::from_secs(15));
		assert_eq!(rows, total, "coalescing must not drop a queued version");

		let slices = h.view_bearing_records(target);
		assert!(
			slices <= 4,
			"the pushes that queued behind the first commit must be replayed as one slice, not \
			 one each: expected 2 view-bearing commits, tolerated up to 4, got {slices} (with no \
			 coalescing this is {total})"
		);
		drop(actor);
	}

	#[test]
	fn buffer_overflow_falls_back_to_drain_without_loss_or_duplication() {
		// When more pushes arrive during one commit than the buffer holds, the buffer is dropped and
		// the actor falls back to a post-commit Drain of its CDC store. Here the actor shares the
		// engine's real store, so the fallback must recover every version: nothing lost to the
		// cleared buffer, nothing applied twice across replay and Drain.
		let h = harness();
		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(h.engine.cdc_store(), v0);

		let total = MAX_BUFFERED_INGESTS + 8;
		for id in 0..total {
			h.te.command(&format!("INSERT app::t [{{ id: {id} }}]"));
		}
		let target = h.engine.current_version().expect("current version");
		let items = h.harvest(v0, target, total);

		let mut covers_from = v0;
		for item in items {
			let up_to = item.version;
			send_ingest(&actor, vec![item], covers_from, up_to);
			covers_from = up_to;
		}

		let rows = h.await_view_rows(total, StdDuration::from_secs(15));
		assert_eq!(rows, total, "the Drain fallback after a buffer overflow must recover every pushed version");

		sleep(StdDuration::from_millis(200));
		assert_eq!(
			h.view_rows(),
			total,
			"no version may be applied twice across buffered replay and the Drain fallback"
		);
		drop(actor);
	}

	#[test]
	fn a_tick_that_commits_must_still_drain_afterwards() {
		// A tick that produces output sets `committing`, which suppresses on_tick's own trailing
		// Drain. The CommitDone it gets back carries more=false with nothing buffered and no wake
		// pending, so before the fix nothing sent a Drain on that path either. Generations are
		// pruned only while draining (step / step_pushed / process_items), so such a tick left the
		// generation it had just promoted in the overlay with nothing to remove it: one leaked
		// generation per tick, on precisely the flows that tick most - the quiet, settled ones.
		//
		// Overlay depth is private to the actor, so the observable stand-in is the cursor. A Drain
		// over versions carrying no relevant CDC skips the cursor to the safe watermark and
		// publishes it, whereas a tick commit publishes advance_to, which is the cursor unchanged.
		// A position that moves past the pre-tick gap is therefore proof a Drain ran, and one that
		// stays put is proof none did.
		//
		// The tumbling window is what lets the tick commit at all: only Window/Aggregate/Join/...
		// nodes report `ticks()`, and over a plain MAP view on_tick skips its whole body. The
		// engine clock is a mock frozen at 1s, so the pushed row arms a seal timer that stays
		// not-due until the test advances the clock by hand - no wall-clock sleep decides anything.
		//
		// Mutation: drop `state.drain_after_commit = true` from dispatch_tick_commit. The tick
		// still commits, so the precondition below still holds, but the position stays where the
		// push left it and the final assertion fails.
		let h = harness_with(
			"CREATE TABLE app::t { id: int4, g: int4, v: int4 }",
			r#"CREATE DEFERRED VIEW app::v { g: int4, total: int8 } AS {
				FROM app::t
					| window tumbling { total: math::sum(v) }
						with { interval: "1s", grace: "0s" }
						by { g }
			}"#,
		);
		assert!(h.flow.ticks(), "a flow whose nodes never tick would skip on_tick's body entirely");
		h.te.admin("CREATE TABLE app::unrelated { id: int4 }");

		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(h.engine.cdc_store(), v0);

		h.te.command("INSERT app::t [{ id: 1, g: 1, v: 5 }]");
		let target = h.engine.current_version().expect("current version");
		send_ingest(&actor, h.harvest(v0, target, 1), v0, target);
		assert_eq!(
			h.await_view_rows(1, seconds(10).to_std()),
			1,
			"the pushed row must reach the window, or no seal timer is ever armed"
		);
		assert_eq!(
			h.await_position(target, seconds(5).to_std()),
			Some(target),
			"the push must settle the cursor before the tick, so that any later move is the \
			 tick's doing and nothing else's"
		);

		// Versions the actor is never told about. Nothing pushes them, nothing sends Wake and the
		// scheduled tick is an hour out, so only a Drain can carry the cursor over them.
		h.te.command("INSERT app::unrelated [{ id: 1 }]");
		let gap = h.engine.current_version().expect("current version");
		h.await_safe_watermark(gap);
		assert!(gap > target, "the test needs unconsumed safe versions above the settled cursor");

		let Clock::Mock(clock) = h.engine.clock() else {
			panic!("this test arms and fires a seal timer by hand and needs the mock clock")
		};
		clock.advance_secs(3);

		let pre_tick = h.engine.current_version().expect("current version");
		assert!(actor.actor_ref().send(FlowActorMessage::Tick).is_ok(), "send tick");

		assert!(
			h.await_version_beyond(pre_tick, seconds(10)).is_some(),
			"precondition: the tick must actually reach a commit. A tick that produces no \
			 output leaves `committing` false and falls straight through to on_tick's own \
			 trailing Drain, which would satisfy the assertion below for the wrong reason"
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
}
