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
};
use reifydb_engine::engine::StandardEngine;
use reifydb_flow::{operator::metrics::OperatorSampleRegistry, transaction::substrate::FlowSubstrate};
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
	execution::frontier::WatermarkHolds,
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
	pub clock: Clock,
	pub health: FlowHealthRegistry,
	pub flow_tracker: FlowPositionTracker,
	pub flow: FlowDag,
	pub source_objects: Arc<BTreeSet<ObjectId>>,
	pub completeness_objects: Option<Arc<BTreeSet<u64>>>,
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
	initial_completeness_objects: Option<Arc<BTreeSet<u64>>>,
	initial_cursor: CommitVersion,
}

pub struct FlowActorState {
	flow_engine: FlowEngineInner,
	source_objects: Arc<BTreeSet<ObjectId>>,
	completeness_objects: Option<Arc<BTreeSet<u64>>>,
	cursor: CommitVersion,
	durable_cursor: CommitVersion,
	committing: bool,
	awaiting_load: bool,
	poisoned: bool,
	retry_count: u32,
	overlay: FlowWriteOverlay,
	pending_holds: WatermarkHolds,
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
			initial_completeness_objects: params.completeness_objects,
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
		FlowEngineInner::new(
			self.engine.catalog(),
			self.engine.executor(),
			self.engine.event_bus().clone(),
			RuntimeContext::with_clock(self.clock.clone()),
			self.custom_operators.clone(),
			self.substrate.clone(),
			self.operator_samples.clone(),
		)
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
		self.dispatch_commit(state, ctx, slice, advance_to, false, WatermarkHolds::new());
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
				completeness_objects: state.completeness_objects.as_deref(),
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
				holds,
			}) => {
				state.retry_count = 0;
				state.cursor = advance_to;
				for hold in holds {
					self.substrate.frontiers.publish(hold.object, hold.frontier, advance_to);
				}
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
				holds,
			}) => {
				self.dispatch_commit(state, ctx, slice, advance_to, more, holds);
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
		holds: WatermarkHolds,
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
		state.pending_holds = holds;
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
		let commit_version = committed.as_ref().map(|(version, _)| *version);
		self.settle_commit(state, committed);
		let holds = take(&mut state.pending_holds);
		if result.is_ok()
			&& let Some(version) = commit_version
		{
			for hold in holds {
				self.substrate.frontiers.publish(hold.object, hold.frontier, version);
			}
		}
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

	fn on_publish_restored_frontiers(&self, state: &mut FlowActorState) {
		if state.poisoned {
			return;
		}
		match self.computer.resolved_holds(&mut state.flow_engine, self.flow_id, self.initial_cursor) {
			Ok(holds) => {
				for hold in holds {
					self.substrate.frontiers.publish(
						hold.object,
						hold.frontier,
						self.initial_cursor,
					);
				}
			}
			Err(e) => warn!(
				flow_id = self.flow_id.0,
				error = %e,
				"failed to republish restored output frontiers; consumers stay at the epoch until this flow commits a slice"
			),
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
				Ok((pending, view_changes)) => {
					let has_output =
						pending.iter_sorted().next().is_some() || !view_changes.is_empty();
					if has_output {
						self.dispatch_tick_commit(state, ctx, pending, view_changes);
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
		if self.initial_cursor > CommitVersion(0) {
			let _ = ctx.self_ref().send(FlowActorMessage::PublishRestoredFrontiers);
		}

		let state = FlowActorState {
			flow_engine,
			source_objects: self.initial_source_objects.clone(),
			completeness_objects: self.initial_completeness_objects.clone(),
			cursor: self.initial_cursor,
			durable_cursor: self.initial_cursor,
			committing: false,
			awaiting_load: false,
			poisoned,
			retry_count: 0,
			overlay: FlowWriteOverlay::new(),
			pending_holds: WatermarkHolds::new(),
			drain_after_commit: false,
			last_checkpoint_at: self.clock.now(),
		};

		if !state.poisoned {
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
			FlowActorMessage::PublishRestoredFrontiers => {
				self.on_publish_restored_frontiers(state);
				Directive::Continue
			}
			FlowActorMessage::UpdateSources {
				source_objects,
				completeness_objects,
			} => {
				state.source_objects = source_objects;
				state.completeness_objects = completeness_objects;
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

#[cfg(all(test, not(target_arch = "wasm32")))]
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
	};
	use reifydb_codec::{key::encoded::EncodedKeyRange, row::pod::EncodedPodRow};
	use reifydb_core::{
		actors::{flow::FlowActorHandle, pending::PendingLayers},
		interface::{
			catalog::{
				flow::OperatorId,
				ringbuffer::{RingBufferMetadata, decode_ringbuffer_metadata},
			},
			cdc::SystemChange,
			change::{ChangeOrigin, Diff},
		},
		key::{
			Key,
			kind::KeyKind,
			operator_state::{Keyspace, OperatorStateKey},
		},
	};
	use reifydb_flow::transaction::{DeferredParams, DepFlowTransaction};
	use reifydb_runtime::sync::waiter::WaiterHandle;
	use reifydb_store_operator::store::OperatorStore;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::{
		group::{GroupCommitBegin, GroupCommitHandle},
		multi::RangeScope,
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
		substrate: FlowSubstrate,
		health: FlowHealthRegistry,
	}

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

		let substrate = FlowSubstrate::with_dictionary(engine.dictionary_allocators(), engine.operator_state());
		let mut probe = FlowEngineInner::new(
			engine.catalog(),
			engine.executor(),
			engine.event_bus().clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			CustomOperators::new(HashMap::new()),
			substrate.clone(),
			OperatorSampleRegistry::new(),
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
			tracker.clone(),
			FlowMaterialization::new(CdcConsumerWatermark::new(), FlowPositionTracker::new()),
			substrate.operators.clone(),
		);
		let begin_engine = engine.clone();
		let begin: GroupCommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
		let group = GroupCommitHandle::spawn(
			&engine.spawner(),
			begin,
			Duration::from_milliseconds(100).unwrap(),
			256,
		);
		let committer_handle =
			engine.spawner().spawn_flow("pull-protocol-committer", CommitterActor::new(committer, group));

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
		}
	}

	impl Harness {
		fn spawn_actor(&self, cursor: CommitVersion) -> FlowActorHandle {
			self.control.store(CommitVersion(u64::MAX));
			self.spawn_actor_with_bounded_control(cursor)
		}

		fn spawn_actor_with_bounded_control(&self, cursor: CommitVersion) -> FlowActorHandle {
			self.spawn_actor_with_substrate(cursor, self.substrate.clone())
		}

		fn spawn_actor_with_substrate(
			&self,
			cursor: CommitVersion,
			substrate: FlowSubstrate,
		) -> FlowActorHandle {
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
					clock: self.engine.clock().clone(),
					health: self.health.clone(),
					flow_tracker: self.tracker.clone(),
					flow: self.flow.clone(),
					source_objects: self.source_objects.clone(),
					completeness_objects: None,
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

		fn await_position_at_least(&self, floor: CommitVersion, timeout: Duration) -> Option<CommitVersion> {
			self.poll_until(timeout, || {
				self.tracker.all().get(&self.flow_id).copied().filter(|got| *got >= floor)
			})
		}

		fn await_version_beyond(&self, floor: CommitVersion, timeout: Duration) -> Option<CommitVersion> {
			self.poll_until(timeout, || self.engine.current_version().ok().filter(|got| *got > floor))
		}

		fn persisted_checkpoint(&self) -> Option<CommitVersion> {
			let mut txn = self.engine.begin_query(IdentityId::system()).expect("query");
			CdcCheckpoint::fetch_opt(&mut Transaction::Query(&mut txn), &self.flow_id)
				.expect("fetch checkpoint")
		}

		fn await_checkpoint_beyond(&self, floor: CommitVersion, timeout: Duration) -> Option<CommitVersion> {
			self.poll_until(timeout, || self.persisted_checkpoint().filter(|got| *got > floor))
		}

		fn advance_source_watermarks(&self, substrate: &FlowSubstrate, at: DateTime) {
			let sources: Vec<OperatorId> = self
				.flow
				.get_operator_ids()
				.filter(|id| self.flow.get_operator(id).is_some_and(|op| op.ty.is_source()))
				.collect();
			assert!(!sources.is_empty(), "the flow under test must have a source to advance");
			let mut txn = DepFlowTransaction::deferred_from_parts(DeferredParams {
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
	}

	fn seconds(seconds: i64) -> Duration {
		Duration::from_seconds(seconds).expect("duration from seconds")
	}

	fn millis(milliseconds: i64) -> Duration {
		Duration::from_milliseconds(milliseconds).expect("duration from milliseconds")
	}

	#[test]
	fn a_wake_that_lands_during_a_commit_is_not_lost() {
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
		let h = harness();
		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(v0);

		let total = 12;
		for id in 0..total {
			h.te.command(&format!("INSERT app::t [{{ id: {id} }}]"));
		}
		let target = h.engine.current_version().expect("current version");
		h.await_safe_watermark(target);

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
		let h = harness();
		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(v0);

		h.te.command("INSERT app::t [{ id: 1 }]");
		let target = h.engine.current_version().expect("current version");
		h.await_safe_watermark(target);

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
		let h = harness_with(
			"CREATE TABLE app::t { id: int4, g: int4, v: int4 } with { time: processing }",
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

		h.te.command("INSERT app::unrelated [{ id: 1 }]");
		let gap = h.engine.current_version().expect("current version");
		h.await_safe_watermark(gap);
		assert!(gap > target, "the test needs unconsumed safe versions above the settled cursor");

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
		let h = harness_with(
			"CREATE TABLE app::t { id: int4, g: int4, v: int4 } with { time: processing }",
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
		harness_with(
			"CREATE TABLE app::t { id: int4, v: int4, ts: datetime } with { time: event(ts) }",
			&format!("CREATE DEFERRED RINGBUFFER VIEW app::v {{ id: int4, v: int4 }} \
				 WITH {{ capacity: 1000, row: {{ ttl: {{ duration: '1s', announce: {announce} }} }} }} \
				 AS {{ FROM app::t map {{ id, v }} }}"),
		)
	}

	fn drive_ring_ttl_tick(h: &Harness) -> (FlowActorHandle, CommitVersion) {
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
		let h = harness_with(
			"CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { time: event(ts) }",
			"CREATE DEFERRED VIEW app::v { g: int4, total: int8 } \
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
		assert!(actor
			.actor_ref()
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
			h.tracker.clone(),
			FlowMaterialization::new(CdcConsumerWatermark::new(), FlowPositionTracker::new()),
			substrate2.operators.clone(),
		);
		let begin_engine = h.engine.clone();
		let begin: GroupCommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
		let committer2_handle = h.engine.spawner().spawn_flow(
			"pull-protocol-committer-restart",
			CommitterActor::new(committer2, GroupCommitHandle::inline(begin)),
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
				clock: h.engine.clock().clone(),
				health: health2.clone(),
				flow_tracker: h.tracker.clone(),
				flow: h.flow.clone(),
				source_objects: h.source_objects.clone(),
				completeness_objects: None,
				cursor: target,
				pull_batch_bytes: ByteSize::from_mib(8),
				load_batch_bytes: ByteSize::from_mib(8),
				checkpoint_lag: 10_000,
				checkpoint_max_age: Duration::from_milliseconds(5_000).unwrap(),
				retry_limit: 3,
				retry_backoff: Duration::from_milliseconds(50).unwrap(),
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
	fn the_ring_sink_metadata_mirror_tracks_the_mvcc_row() {
		let h = harness_with(
			"CREATE TABLE app::t { id: int4, v: int4, ts: datetime } with { time: event(ts) }",
			"CREATE DEFERRED RINGBUFFER VIEW app::v { id: int4, v: int4 } \
			 WITH { capacity: 3 } AS { FROM app::t map { id, v } }",
		);
		let v0 = h.engine.current_version().expect("current version");
		let actor = h.spawn_actor(v0);

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
			decode_ringbuffer_metadata(EncodedPodRow::view(&rows[0].bytes))
				.expect("decode the metadata row")
		};

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
						OperatorStateKey::decode_inner(key.as_slice())
							.is_some_and(|(_, ks, _)| ks == Keyspace::RINGBUFFER_META)
					})
					.map(|(_, row)| {
						decode_ringbuffer_metadata(&EncodedPodRow::new(row.body()))
							.expect("decode the mirrored metadata row")
					})
					.collect::<Vec<_>>()
			})
			.collect();

		assert_eq!(mirrored.len(), 1, "the sink must keep exactly one mirror for its global metadata");
		assert_eq!(
			mirrored[0], stored,
			"the arena mirror and the mvcc row must carry the same count, head and tail: \
			 the mirror is what a replay reads, so any drift makes the replay assign different \
			 storage row numbers than the live run did"
		);
		assert!(mirrored[0].head > 1, "precondition: evictions must have moved head off its initial value");
		drop(actor);
	}
}
