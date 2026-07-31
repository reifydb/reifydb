// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeSet,
	mem::take,
	panic::{AssertUnwindSafe, catch_unwind},
	process,
	sync::Arc,
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
	Result, byte_size::ByteSize, reifydb_assertions,
	value::{datetime::DateTime, duration::Duration, identity::IdentityId},
};
use tracing::{error, warn};

use crate::{
	builder::CustomOperators,
	deferred::{
		committer::{CommitterMessage, FlowSlice, SliceCommitReply, TickCommitReply},
		frontier::ControlFrontier,
		health::FlowHealthRegistry,
		loader::{LoaderMessage, LoaderReply},
		overlay::FlowWriteOverlay,
		slice::{SliceComputer, SliceConfig, SliceCursor, SliceStep},
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
	pub retention_metrics: RetentionMetrics,
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
	retention_metrics: RetentionMetrics,
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
}

pub struct FlowActorState {
	flow_engine: FlowEngineInner,
	source_objects: Arc<BTreeSet<ObjectId>>,
	cursor: CommitVersion,
	durable_cursor: CommitVersion,
	committing: bool,
	awaiting_load: bool,
	poisoned: bool,
	retry_count: u32,
	overlay: FlowWriteOverlay,
	drain_after_commit: bool,
	last_checkpoint_at: DateTime,
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

	fn on_drain(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>) {
		if state.poisoned || state.committing || state.awaiting_load {
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
		outcome: std::result::Result<(Vec<Arc<Cdc>>, CommitVersion), String>,
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
			Err(reason) => {
				self.retry_or_poison(state, ctx, format!("flow catch-up load failed: {reason}"));
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
		let reply: TickCommitReply = Box::new(move |committed| {
			let _ = self_ref.send(FlowActorMessage::TickCommitted {
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
			awaiting_load: false,
			poisoned,
			retry_count: 0,
			overlay: FlowWriteOverlay::new(),
			drain_after_commit: false,
			last_checkpoint_at: self.clock.now(),
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
					if state.committing || state.awaiting_load {
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
		thread::sleep,
		time::{Duration as StdDuration, Instant},
	};

	use reifydb_cdc::{
		consume::{checkpoint::CdcCheckpoint, watermark::CdcConsumerWatermark},
		produce::watermark::CdcProducerWatermark,
	};
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
			loader::{LoaderActor, LoaderHandle, LoaderMetrics},
			quiescence::FlowMaterialization,
			routing,
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
	}

	// One deferred view over app::t, a real committer actor behind a 100ms group-commit linger
	// (the linger is the window that keeps the flow actor in `committing` while wakes arrive),
	// and FLOW_TICK set to 1h so only wakes this test sends can advance the actor under test.
	// The backlog is the engine's own: the CDC producer feeds it, exactly as in production; the
	// only production piece missing is the supervisor, so the tests play its part by sending
	// Wake and by managing the control frontier by hand.
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
			"pull-protocol-committer",
			CommitterActor::new(committer, group, OperatorStateBudgetHandle::default()),
		);

		let loader_handle = engine
			.spawner()
			.spawn_flow(
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
		}
	}

	impl Harness {
		// `init` enqueues a Drain that runs lazily on a pool worker, so the caller cannot know
		// when it lands. That Drain is not a no-op: it pulls (cursor, safe] from the backlog,
		// finds nothing relevant, and skips the cursor to the safe watermark - silently
		// swallowing every later version at or below it. So spawn one version short of
		// `cursor` and block until that Drain has skipped us up to it: the published position
		// is proof the Drain was consumed while the safe watermark was still pinned at
		// `cursor` (nothing has been written yet), so the actor settles exactly where the
		// caller asked. Only Wakes and ticks can move it from here.
		fn spawn_actor(&self, cursor: CommitVersion) -> FlowActorHandle {
			self.control.store(CommitVersion(u64::MAX));
			self.spawn_actor_with_bounded_control(cursor)
		}

		fn spawn_actor_with_bounded_control(&self, cursor: CommitVersion) -> FlowActorHandle {
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
					pull_batch_bytes: ByteSize::from_mib(8),
					load_batch_bytes: ByteSize::from_mib(8),
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

		// One commit carrying view changes is one slice: this flow's slices never overlap, so
		// group commit cannot merge two of them into a single version.
		fn view_bearing_records(&self, up_to: CommitVersion) -> usize {
			self.engine
				.cdc_store()
				.read_range(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded, 10_000)
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

		// The same bound the actor's drain pulls up to. Covering `want` before the actor is
		// spawned is what makes its init Drain skip to exactly `want`: below it that Drain
		// finds a lower bound, publishes a lower position, and the spawn would wait forever.
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
		// frozen at 1s which the tick tests advance by hand to fire a seal timer, so an elapsed
		// check against it would never move and a regression would hang rather than fail.
		// Returns None on timeout so the caller asserts on the timeout rather than a stale read.
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

		// A drain that skips lands on whatever is safe at that instant, which is at least the
		// caller's floor but not exactly it, so the tick tests cannot compare for equality the
		// way the others do.
		fn await_position_at_least(&self, floor: CommitVersion, timeout: Duration) -> Option<CommitVersion> {
			self.poll_until(timeout, || {
				self.tracker.all().get(&self.flow_id).copied().filter(|got| *got >= floor)
			})
		}

		fn await_version_beyond(&self, floor: CommitVersion, timeout: Duration) -> Option<CommitVersion> {
			self.poll_until(timeout, || self.engine.current_version().ok().filter(|got| *got > floor))
		}

		// The DURABLE position, not the in-memory one the other waiters read. Only what
		// survives a restart gates CDC log compaction, and only a commit that actually carried
		// a checkpoint leaves any, so this is the one observable that can tell a real
		// checkpoint from a claimed one.
		fn persisted_checkpoint(&self) -> Option<CommitVersion> {
			let mut txn = self.engine.begin_query(IdentityId::system()).expect("query");
			CdcCheckpoint::fetch_opt(&mut Transaction::Query(&mut txn), &self.flow_id)
				.expect("fetch checkpoint")
		}

		fn await_checkpoint_beyond(&self, floor: CommitVersion, timeout: Duration) -> Option<CommitVersion> {
			self.poll_until(timeout, || self.persisted_checkpoint().filter(|got| *got > floor))
		}
	}

	fn seconds(seconds: i64) -> Duration {
		Duration::from_seconds(seconds).expect("duration from seconds")
	}

	fn millis(milliseconds: i64) -> Duration {
		Duration::from_milliseconds(milliseconds).expect("duration from milliseconds")
	}

	#[test]
	fn a_wake_that_lands_during_a_commit_is_not_lost() {
		// A wake that arrives while the actor is committing cannot start a pull immediately;
		// it must leave a marker the commit completion honors. The 100ms group linger keeps
		// the actor in `committing` while the second row's wake arrives; after that wake this
		// test sends nothing, and the tick is an hour out, so if the marker is dropped the
		// second row never materializes.
		//
		// Mutation: drop `state.drain_after_commit = true` from the Wake arm. The second wake
		// lands in the linger window, is ignored, and the view sticks at one row.
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
		assert_eq!(
			h.await_position(target, StdDuration::from_secs(5)),
			Some(target),
			"the follow-up pull must advance the flow position to the safe bound"
		);
		drop(actor);
	}

	#[test]
	fn a_burst_of_commits_coalesces_into_few_slices() {
		// Rows that accumulate while a commit is in flight must be pulled as ONE slice, not
		// one slice each. A slice is not free: it pays a transaction, a DAG walk, a state
		// flush and a commit, and at ~30 versions/s fanned out over ~100 flows that per-slice
		// envelope is the bulk of the flow CPU bill. The pull model makes this structural:
		// however many versions landed since the last pull ride out in one byte-budgeted
		// batch, so the busier the ingest, the more versions ride on one slice.
		//
		// The count is exact but the timing is not: this flow's slices are strictly sequential
		// (the committing flag gates the next pull on the commit reply), so group commit can
		// never merge them and one view-bearing CDC record is exactly one slice. Nine rows
		// woken with no coalescing are nine slices; coalesced they are two (the first row,
		// then the eight that accumulated behind its commit). The bound is loose enough to
		// tolerate a wake that races in after a commit and starts its own slice, and still
		// fails loudly if coalescing is gone.
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
		// When the backlog has evicted past a flow's cursor, the pull reports Behind and the
		// actor must fetch the missing range from durable CDC through the loader - once -
		// then rejoin the backlog. Nothing may be lost to the eviction and nothing applied
		// twice across the loader chunk and the backlog pulls that follow it.
		let h = harness();
		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(v0);

		let total = 12;
		for id in 0..total {
			h.te.command(&format!("INSERT app::t [{{ id: {id} }}]"));
		}
		let target = h.engine.current_version().expect("current version");
		h.await_safe_watermark(target);

		// Evict everything the flow has not consumed yet: its next pull can only be Behind,
		// so the loader is the only path by which these rows can reach the view.
		h.backlog.evict_below(target);
		h.wake(&actor);

		let rows = h.await_view_rows(total, StdDuration::from_secs(15));
		assert_eq!(rows, total, "the loader path must recover every version evicted from the backlog");
		assert_eq!(
			h.await_position(target, StdDuration::from_secs(10)),
			Some(target),
			"the catch-up must advance the flow position to the safe bound"
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
		// Flows must never advance past what the supervisor has scanned for flow DDL: a flow
		// that outruns the control frontier could consume versions carrying its own deletion
		// or a sibling's creation before the supervisor processed them. The frontier is a hard
		// bound on the pull, not advice.
		//
		// Mutation: drop `.min(self.control.get())` from safe_bound. The first assertion
		// fails because the position sails past the frontier to the safe watermark.
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
		// The CDC producer advances its watermark on its own thread after commit, so it can
		// transiently sit ahead of the command done_until. The pull bound must clamp to
		// min(producer, done_until): reading past done_until would consume versions whose
		// effects are not yet visible, and treating the overshoot as a stall would strand the
		// row until the (1h) tick.
		//
		// Mutation: drop `.min(self.engine.done_until())` from safe_bound. The row still
		// materializes, but the position overruns done_until to the overshot watermark and
		// the final assertion fails.
		let h = harness();
		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(v0);

		h.te.command("INSERT app::t [{ id: 1 }]");
		let target = h.engine.current_version().expect("current version");
		h.await_safe_watermark(target);

		let producer = h.engine.ioc().resolve::<CdcProducerWatermark>().expect("producer watermark");
		producer.advance(CommitVersion(producer.get().0 + 1));
		assert!(
			producer.get() > h.engine.done_until(),
			"test precondition: producer watermark must overshoot done_until"
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
		// A tick that produces output sets `committing`, which suppresses on_tick's own
		// trailing Drain. The commit acknowledgement carries nothing that would re-drain on
		// its own, so without the tick marking drain_after_commit nothing pulls afterwards.
		// Generations are pruned only while pulling, so such a tick left the generation it
		// had just promoted in the overlay with nothing to remove it: one leaked generation
		// per tick, on precisely the flows that tick most - the quiet, settled ones.
		//
		// Overlay depth is private to the actor, so the observable stand-in is the cursor. A
		// pull over versions carrying no relevant CDC skips the cursor to the safe watermark
		// and publishes it, whereas a tick commit publishes nothing. A position that moves
		// past the pre-tick gap is therefore proof a pull ran, and one that stays put is
		// proof none did.
		//
		// The tumbling window is what lets the tick commit at all: only Window/Aggregate/...
		// nodes report `ticks()`, and over a plain MAP view on_tick skips its whole body. The
		// engine clock is a mock frozen at 1s, so the woken row arms a seal timer that stays
		// not-due until the test advances the clock by hand - no wall-clock sleep decides
		// anything.
		//
		// Mutation: drop `state.drain_after_commit = true` from dispatch_tick_commit. The
		// tick still commits, so the precondition below still holds, but the position stays
		// where the wake left it and the final assertion fails.
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
			"the wake must settle the cursor before the tick, so that any later move is the \
			 tick's doing and nothing else's"
		);

		// Versions the actor is never woken for. Nothing wakes it and the scheduled tick is
		// an hour out, so only the tick commit's own follow-up pull can carry the cursor over
		// them.
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

	#[test]
	fn a_tick_commit_must_not_pass_itself_off_as_a_checkpoint() {
		// A tick commit persists no checkpoint, so it must never be counted as one. A flow
		// that mistakes its own tick for a checkpoint believes it is more durable than it is,
		// and stops checkpointing because it sees nothing left to record.
		//
		// The resulting silence is unbounded rather than merely late: ticks come round faster
		// than the staleness bound they would be resetting, so the bound can never expire, and
		// the fallback that checkpoints after enough version drift measures that drift against
		// the same overstated position. A ticking flow would therefore never checkpoint again
		// after its last input-driven one. Flow checkpoints are part of the minimum that gates
		// CDC log compaction, so the log would grow forever.
		//
		// Nothing here depends on wall time: the clock is a mock, and one 6s advance both ages
		// the flow past its staleness bound and seals the 1s window that gives the tick
		// something to commit. A checkpoint is due the moment the flow next goes idle - unless
		// the tick reset the bound.
		//
		// Mutation: let a tick commit advance the durable position and reset the staleness
		// bound the way a slice commit does. Every assertion up to and including the
		// tick-committed precondition still passes; the final one times out with the
		// checkpoint still sitting at the wake's version.
		let h = harness_with(
			"CREATE TABLE app::t { id: int4, g: int4, v: int4 }",
			r#"CREATE DEFERRED VIEW app::v { g: int4, total: int8 } AS {
				FROM app::t
					| window tumbling { total: math::sum(v) }
						with { interval: "1s", grace: "0s" }
						by { g }
			}"#,
		);
		assert!(h.flow.ticks(), "a flow whose nodes never tick can never commit a tick");
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

		// Versions the actor is never woken for. Nothing wakes it and the scheduled tick is an
		// hour out, so the only thing that can carry the cursor over them is the flow going
		// idle after the tick commits - which is also the only moment it decides whether to
		// checkpoint.
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
}
