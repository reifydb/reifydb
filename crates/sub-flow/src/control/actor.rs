// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeSet, HashMap},
	iter::once,
	mem::take,
	panic::{AssertUnwindSafe, catch_unwind},
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_core::{
	actors::{flow::FlowActorMessage, pending::Pending},
	commit::{
		loader::{CommitLoaderMessage, CommitLoaderReply},
		queue::{CommitPull, CommitQueue},
	},
	common::{CommitVersion, SourceVersion},
	error::diagnostic::flow::flow_step_panicked,
	flow::{dag::FlowDag, operator::OperatorDef},
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			flow::FlowId,
			id::ViewId,
			object::ObjectId,
		},
		commit::{CommitEntry, Loaded},
		operator::FlowCheckpoint,
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_flow_async::{
	engine::{FlowEngineInner, frontier::WatermarkHolds},
	operator::metrics::OperatorSampleRegistry,
	transaction::{LookupContext, substrate::FlowSubstrate},
};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::ActorConfig,
		traits::{Actor, Directive},
	},
	context::{RuntimeContext, clock::Clock},
	fatal::describe_payload,
	sync::mutex::Mutex,
};
use reifydb_store_operator::store::pin::CheckpointPin;
use reifydb_transaction::{error::TransactionError, multi::lease::VersionLeaseGuard};
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	error::Error,
	reifydb_assertions,
	value::{datetime::DateTime, duration::Duration, identity::IdentityId},
};
use reifydb_wal::lsn::Lsn;
use tracing::{error, warn};

use crate::{
	builder::CustomOperators,
	commit::{
		backfill::{Snapshot, UpstreamCut, backfill_reads, compute_unit_backfill, upstream_write_after},
		committer::{CommitterMessage, FlowSlice, SliceCommitReply, TickCommitReply, produced_output},
		merge::{HeldReads, ReadCache, ReadStream, StepScope, StreamRead, UpstreamRead, UpstreamReads, merge},
		slice::{HoldCache, SliceComputer, SliceConfig, SliceCursor, SliceStep},
	},
	control::{health::FlowHealthRegistry, supervisor::FlowSupervisorMessage},
	marker::MarkerMap,
	operator::provider::StandardOperatorProvider,
	progress::{
		frontier::ControlFrontier,
		tracker::{FlowPositionTracker, FlowUpstreams},
	},
};

pub struct FlowActorParams {
	pub engine: StandardEngine,
	pub supervisor: ActorRef<FlowSupervisorMessage>,
	pub committer: ActorRef<CommitterMessage>,
	pub terminal_committer: ActorRef<CommitterMessage>,
	pub queue: CommitQueue,
	pub loader: ActorRef<CommitLoaderMessage>,
	pub control: ControlFrontier,
	pub custom_operators: CustomOperators,
	pub substrate: FlowSubstrate,
	pub operator_samples: OperatorSampleRegistry,
	pub clock: Clock,
	pub health: FlowHealthRegistry,
	pub flow_tracker: FlowPositionTracker,
	pub flow: FlowDag,
	pub members: Vec<(FlowDag, ViewId)>,
	pub source_objects: Arc<BTreeSet<ObjectId>>,
	pub completeness_objects: Option<Arc<BTreeSet<u64>>>,
	pub cursor: CommitVersion,
	pub cursor_lsn: Lsn,
	pub pull_batch_bytes: ByteSize,
	pub load_batch_bytes: ByteSize,
	pub checkpoint_lag: u64,
	pub checkpoint_max_age: Duration,
	pub retry_limit: u32,
	pub retry_backoff: Duration,
	pub wake_coalesce: Duration,
	pub batch_size: usize,
}

pub struct FlowActor {
	engine: StandardEngine,
	supervisor: ActorRef<FlowSupervisorMessage>,
	committer: ActorRef<CommitterMessage>,
	terminal_committer: ActorRef<CommitterMessage>,
	queue: CommitQueue,
	loader: ActorRef<CommitLoaderMessage>,
	control: ControlFrontier,
	custom_operators: CustomOperators,
	substrate: FlowSubstrate,
	operator_samples: OperatorSampleRegistry,
	clock: Clock,
	health: FlowHealthRegistry,
	flow_tracker: FlowPositionTracker,
	flow: FlowDag,
	flow_id: FlowId,
	members: Vec<(FlowDag, ViewId)>,
	ticks_enabled: bool,
	has_lookup: bool,
	computer: SliceComputer,
	config: SliceConfig,
	pull_batch_bytes: ByteSize,
	load_batch_bytes: ByteSize,
	retry_limit: u32,
	retry_backoff: Duration,
	wake_coalesce: Duration,
	batch_size: usize,
	checkpoint_max_age: Duration,
	initial_source_objects: Arc<BTreeSet<ObjectId>>,
	initial_completeness_objects: Option<Arc<BTreeSet<u64>>>,
	initial_cursor: CommitVersion,
	initial_cursor_lsn: Lsn,
	wake_pending: Arc<AtomicBool>,
	backfill: bool,
	create_snapshot: Mutex<Option<(CommitVersion, VersionLeaseGuard, CheckpointPin)>>,
}

pub struct FlowActorState {
	flow_engine: FlowEngineInner,
	pending_stop: Option<(bool, Box<dyn FnOnce() + Send>)>,
	source_objects: Arc<BTreeSet<ObjectId>>,
	completeness_objects: Option<Arc<BTreeSet<u64>>>,
	cursor: CommitVersion,
	cursor_lsn: Lsn,
	pending_lsn: Lsn,
	frontier: (CommitVersion, Lsn),
	durable_cursor: CommitVersion,
	committing: bool,
	awaiting_load: bool,
	poisoned: bool,
	retry_count: u32,
	pending_holds: WatermarkHolds,
	drain_after_commit: bool,
	last_checkpoint_at: DateTime,
	checkpoint_replies: Vec<Box<dyn FnOnce() + Send>>,
	view_cursors: HashMap<FlowId, CommitVersion>,
	pending_view_cursors: HashMap<FlowId, CommitVersion>,
	asked: HashMap<FlowId, CommitVersion>,
	read_cache: ReadCache,
	held_reads: HeldReads,
	loading_from: CommitVersion,
	backfilling: bool,
	snapshot: Option<Snapshot>,
	lookup_lease: Option<VersionLeaseGuard>,
	hold_cache: Option<HoldCache>,
	backlog: bool,
}

impl FlowActor {
	pub fn new(params: FlowActorParams) -> Self {
		let flow_id = params.flow.id;
		let unit: Vec<&FlowDag> = once(&params.flow).chain(params.members.iter().map(|(m, _)| m)).collect();
		let ticks_enabled = unit.iter().any(|flow| flow.ticks());
		let has_lookup = unit.iter().any(|flow| {
			flow.get_operator_ids().any(|id| {
				flow.get_operator(&id).is_some_and(|node| matches!(node.ty, OperatorDef::Lookup { .. }))
			})
		});
		let fed: Vec<(FlowId, ViewId)> =
			params.members.iter().map(|(member, view)| (member.id, *view)).collect();
		Self {
			computer: SliceComputer::new(params.engine.clone()).with_members(Arc::from(fed)),
			config: SliceConfig {
				checkpoint_lag: params.checkpoint_lag,
			},
			pull_batch_bytes: params.pull_batch_bytes,
			load_batch_bytes: params.load_batch_bytes,
			engine: params.engine,
			supervisor: params.supervisor,
			committer: params.committer,
			terminal_committer: params.terminal_committer,
			queue: params.queue,
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
			members: params.members,
			ticks_enabled,
			has_lookup,
			retry_limit: params.retry_limit,
			retry_backoff: params.retry_backoff,
			wake_coalesce: params.wake_coalesce,
			batch_size: params.batch_size,
			checkpoint_max_age: params.checkpoint_max_age,
			initial_source_objects: params.source_objects,
			initial_completeness_objects: params.completeness_objects,
			initial_cursor: params.cursor,
			initial_cursor_lsn: params.cursor_lsn,
			wake_pending: Arc::new(AtomicBool::new(false)),
			backfill: false,
			create_snapshot: Mutex::new(None),
		}
	}

	pub(crate) fn with_markers(mut self, markers: Option<MarkerMap>) -> Self {
		self.computer = self.computer.with_markers(markers);
		self
	}

	pub fn backfilling(
		params: FlowActorParams,
		create_snapshot: (CommitVersion, VersionLeaseGuard, CheckpointPin),
	) -> Self {
		Self {
			backfill: true,
			create_snapshot: Mutex::new(Some(create_snapshot)),
			..Self::new(params)
		}
	}

	pub(crate) fn wake_pending(&self) -> Arc<AtomicBool> {
		Arc::clone(&self.wake_pending)
	}

	fn tick_interval(&self) -> Duration {
		self.engine.catalog().get_config_duration(ConfigKey::FlowTick)
	}

	fn sample_interval(&self) -> Option<Duration> {
		self.engine.catalog().get_config_duration_opt(ConfigKey::FlowSampleInterval)
	}

	fn poison(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>, reason: String) {
		error!(flow_id = self.flow_id.0, reason = %reason, "poisoning flow");
		self.report_poisoned(ctx, reason);
		state.poisoned = true;
		*self.create_snapshot.lock() = None;
	}

	fn report_poisoned(&self, ctx: &Context<FlowActorMessage>, reason: String) {
		let sent = self.supervisor.send(FlowSupervisorMessage::Poisoned {
			flows: self.unit_ids(),
			reason,
		});
		if sent.is_err() && !ctx.is_cancelled() {
			panic!("flow {} could not report its poison: the flow supervisor is stopped", self.flow_id.0);
		}
	}

	fn publish_position(&self, cursor: CommitVersion) {
		for flow_id in self.unit_ids() {
			self.flow_tracker.update(flow_id, cursor);
		}
	}

	fn unit_ids(&self) -> Vec<FlowId> {
		once(self.flow_id).chain(self.members.iter().map(|(member, _)| member.id)).collect()
	}

	fn unit_has_readers(&self) -> bool {
		self.unit_ids().into_iter().any(|flow_id| self.flow_tracker.has_readers(flow_id))
	}

	fn restore_lookup_lease(&self) -> Result<Option<VersionLeaseGuard>> {
		if !self.has_lookup {
			return Ok(None);
		}
		if self.initial_cursor > CommitVersion::BEFORE_FIRST {
			return Ok(Some(self.engine.multi().restore_lease(self.initial_cursor)));
		}
		match self.engine.acquire_version_lease(self.initial_cursor) {
			Ok(lease) => Ok(Some(lease)),
			Err(e) if e.0.code == TransactionError::SNAPSHOT_EVICTED => {
				let lease = self.engine.multi().acquire_cutoff_lease()?;
				warn!(
					flow_id = self.flow_id.0,
					cursor = self.initial_cursor.0,
					cutoff = lease.version().0,
					"lookup lease below the gc cutoff, holding from the cutoff"
				);
				Ok(Some(lease))
			}
			Err(e) => Err(e),
		}
	}

	fn lookup_computer(&self, state: &FlowActorState) -> SliceComputer {
		self.computer.with_lookup(state.lookup_lease.as_ref().map(|lease| LookupContext {
			versions: Arc::new(self.flow_tracker.lookup_versions(self.flow_id)),
			floor: lease.version(),
		}))
	}

	fn move_lookup_lease(&self, state: &mut FlowActorState) {
		let Some(held) = state.lookup_lease.as_ref().map(VersionLeaseGuard::version) else {
			return;
		};
		let Some(target) = self.lookup_lease_target(state.cursor) else {
			return;
		};
		if target <= held {
			return;
		}
		let lease = match self.engine.acquire_version_lease(target) {
			Ok(lease) => lease,
			Err(e) => panic!(
				"flow {} could not move its lookup lease from {} to {}: {e}",
				self.flow_id.0, held.0, target.0
			),
		};
		state.lookup_lease = Some(lease);
	}

	fn lookup_lease_target(&self, cursor: CommitVersion) -> Option<CommitVersion> {
		let unit: BTreeSet<FlowId> = self.unit_ids().into_iter().collect();
		let mut producers: BTreeSet<FlowId> = BTreeSet::new();
		for flow_id in &unit {
			producers.extend(self.flow_tracker.upstreams(*flow_id).keys().copied());
			producers.extend(self.flow_tracker.lookup_producers_of(*flow_id).into_values());
		}
		let mut target = cursor;
		for producer in producers.into_iter().filter(|producer| !unit.contains(producer)) {
			let source = self.flow_tracker.source_through_commit(producer, cursor)?;
			target = target.min(CommitVersion(source.0 + 1));
		}
		Some(target)
	}

	fn retry_or_poison(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>, reason: String) {
		if state.retry_count >= self.retry_limit {
			self.poison(state, ctx, reason);
			return;
		}
		state.retry_count += 1;
		let mut flow_engine = self.build_flow_engine();
		if let Err(e) = self.register_flow(&mut flow_engine) {
			self.poison(
				state,
				ctx,
				format!("flow engine rebuild failed after error: {e} (original: {reason})"),
			);
			return;
		}
		state.flow_engine = flow_engine;
		state.hold_cache = None;
		let backoff = match self.retry_backoff.try_mul(1i64 << state.retry_count.min(16)) {
			Ok(backoff) => backoff,
			Err(e) => {
				self.poison(state, ctx, format!("retry backoff overflowed: {e} (original: {reason})"));
				return;
			}
		};
		warn!(
			flow_id = self.flow_id.0,
			attempt = state.retry_count,
			reason = %reason,
			"flow error, rebuilt operators and retrying after backoff"
		);
		ctx.schedule_once(backoff, || FlowActorMessage::Drain);
	}

	fn commit_target(&self) -> &ActorRef<CommitterMessage> {
		if self.unit_has_readers() {
			&self.committer
		} else {
			&self.terminal_committer
		}
	}

	fn build_flow_engine(&self) -> FlowEngineInner {
		FlowEngineInner::new(
			self.engine.catalog(),
			self.engine.executor().routines.clone(),
			RuntimeContext::builder(self.engine.runtime_context().spawner.clone())
				.clock(self.engine.runtime_context().clock.clone())
				.rng(self.engine.runtime_context().rng.fork(b"sub-flow/build-flow-engine"))
				.build(),
			Arc::new(StandardOperatorProvider::new(self.custom_operators.clone())),
			self.substrate.clone(),
			self.operator_samples.clone(),
		)
	}

	fn register_flow(&self, flow_engine: &mut FlowEngineInner) -> Result<()> {
		let mut txn = self.engine.begin_command(IdentityId::system())?;
		flow_engine.register(&mut txn, self.flow.clone())?;
		for (member, _) in &self.members {
			flow_engine.register(&mut txn, member.clone())?;
		}
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
		self.checkpoint_cursor(state, ctx);
	}

	fn checkpoint_cursor(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>) {
		let advance_to = state.cursor;
		let mut slice = FlowSlice::empty();
		match self.computer.resolved_holds(&mut state.flow_engine, self.flow_id, advance_to) {
			Ok((_, pending)) => slice.combined = pending,
			Err(e) => {
				self.retry_or_poison(state, ctx, format!("flow checkpoint failed: {e}"));
				return;
			}
		}
		slice.checkpoints.push((
			self.flow_id,
			FlowCheckpoint {
				version: advance_to,
				lsn: state.cursor_lsn,
			},
		));
		self.dispatch_commit(state, ctx, slice, advance_to, state.cursor_lsn, false, WatermarkHolds::new());
	}

	fn on_drain(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>) {
		let woken = self.wake_pending.swap(false, Ordering::SeqCst);
		state.frontier = self.queue.released();
		let safe = state.frontier.0.min(self.engine.done_until()).min(self.control.get());
		if state.poisoned || state.committing || state.awaiting_load {
			if woken && !state.poisoned {
				state.drain_after_commit = true;
			}
			return;
		}
		if state.backfilling {
			self.drain_backfill(state, ctx, safe);
			return;
		}
		state.read_cache.retain_after(state.cursor);
		if safe <= state.cursor {
			state.backlog = false;
			self.checkpoint_if_stale(state, ctx);
			return;
		}
		let upstreams = self.flow_tracker.upstreams(self.flow_id);
		if !upstreams.is_empty() {
			self.drain_merged(state, ctx, safe, &upstreams);
			return;
		}
		let cursor = state.cursor;
		let Some(read) = self.read_range(state, ctx, cursor, safe) else {
			return;
		};
		self.apply_items(state, ctx, &read.items, read.read_to, read.more)
	}

	fn drain_backfill(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>, safe: CommitVersion) {
		let upstreams = self.flow_tracker.upstreams(self.flow_id);
		let mut snapshot = match state.snapshot.take() {
			Some(snapshot) => snapshot,
			None => {
				let create_snapshot = self
					.create_snapshot
					.lock()
					.as_ref()
					.map(|(version, lease, _)| (*version, lease.clone()));
				let Some((version, lease)) = create_snapshot else {
					panic!("flow {} is backfilling without a create snapshot", self.flow_id.0);
				};
				Snapshot {
					version,
					lsn: self.initial_cursor_lsn,
					lease,
					cuts: HashMap::new(),
				}
			}
		};
		if !self.resolve_cuts(state, ctx, safe, &upstreams, &mut snapshot) {
			state.snapshot = Some(snapshot);
			return;
		}

		state.flow_engine.remove_flow(self.flow_id);
		let mut flow_engine = self.build_flow_engine();
		if let Err(e) = self.register_flow(&mut flow_engine) {
			self.retry_or_poison(state, ctx, format!("flow engine reset before backfill failed: {e}"));
			return;
		}
		state.flow_engine = flow_engine;
		state.hold_cache = None;
		let reads = backfill_reads(&self.flow, &state.flow_engine, &snapshot, &upstreams);

		let computed = catch_unwind(AssertUnwindSafe(|| {
			compute_unit_backfill(
				&self.engine,
				&mut state.flow_engine,
				&self.flow,
				&self.members,
				&snapshot,
				&reads,
			)
		}))
		.unwrap_or_else(|payload| Err(Error(Box::new(flow_step_panicked(describe_payload(&payload))))));
		let (version, lsn) = (snapshot.version, snapshot.lsn);
		drop(snapshot);
		match computed {
			Ok((slice, holds)) => self.dispatch_commit(state, ctx, slice, version, lsn, true, holds),
			Err(e) => {
				self.retry_or_poison(state, ctx, format!("flow backfill failed: {e}"));
			}
		}
	}

	fn resolve_cuts(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		safe: CommitVersion,
		upstreams: &FlowUpstreams,
		snapshot: &mut Snapshot,
	) -> bool {
		let version = snapshot.version;
		for (producer, views) in upstreams {
			let mut cut = snapshot.cuts.get(producer).copied().unwrap_or(UpstreamCut {
				read_to: version,
				at: None,
			});
			while cut.at.is_none() && cut.read_to < safe {
				let from = cut.read_to;
				let Some(read) = self.read_range(state, ctx, from, safe) else {
					snapshot.cuts.insert(*producer, cut);
					return false;
				};
				cut.at = upstream_write_after(&read, from, views, version);
				cut.read_to = read.read_to;
				if !read.more || read.read_to <= from {
					break;
				}
			}
			if cut.at.is_none()
				&& self.flow_tracker
					.upstream_complete_through(*producer, cut.read_to)
					.is_some_and(|position| position >= version)
			{
				cut.at = Some(cut.read_to);
			}
			snapshot.cuts.insert(*producer, cut);
		}
		upstreams.keys().all(|producer| snapshot.cuts.get(producer).is_some_and(|cut| cut.at.is_some()))
	}

	fn drain_merged(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		safe: CommitVersion,
		upstreams: &FlowUpstreams,
	) {
		let cursor = state.cursor;
		let Some(tables) = self.read_stream(state, ctx, ReadStream::Tables, cursor, safe) else {
			return;
		};
		let mut upstream_reads = UpstreamReads {
			reads: HashMap::with_capacity(upstreams.len()),
		};
		let mut froms: HashMap<FlowId, CommitVersion> = HashMap::with_capacity(upstreams.len());
		for (producer, views) in upstreams {
			if views.is_empty() {
				upstream_reads.reads.insert(
					*producer,
					UpstreamRead {
						views: views.clone(),
						position: self.flow_tracker.upstream_complete_through(*producer, safe),
						read: StreamRead {
							items: Vec::new().into(),
							read_to: safe,
							more: false,
						},
					},
				);
				continue;
			}
			let from = state.view_cursors.get(producer).copied().unwrap_or(cursor).max(cursor);
			let Some(read) = self.read_stream(state, ctx, ReadStream::Upstream(*producer), from, safe)
			else {
				hold_reads(state, cursor, tables, upstream_reads, &froms);
				return;
			};
			froms.insert(*producer, from);
			let mut upstream = UpstreamRead {
				views: views.clone(),
				position: None,
				read,
			};
			while upstream.needs_extension(cursor) {
				let Some(next) = self.read_range(state, ctx, upstream.read.read_to, safe) else {
					upstream_reads.reads.insert(*producer, upstream);
					hold_reads(state, cursor, tables, upstream_reads, &froms);
					return;
				};
				upstream.read.items = upstream
					.read
					.items
					.iter()
					.chain(next.items.iter())
					.cloned()
					.collect::<Vec<_>>()
					.into();
				upstream.read.read_to = next.read_to;
				upstream.read.more = next.more;
			}
			upstream.position =
				self.flow_tracker.upstream_complete_through(*producer, upstream.read.read_to);
			upstream_reads.reads.insert(*producer, upstream);
		}
		state.held_reads.clear();
		let source_objects = Arc::clone(&state.source_objects);
		let scope = StepScope {
			source_objects: &source_objects,
		};
		let merged = merge(cursor, &tables, &upstream_reads.reads, &scope);
		if merged.target <= cursor {
			let lagging: Vec<FlowId> = upstream_reads
				.reads
				.iter()
				.filter(|(_, upstream)| upstream.position.is_none_or(|position| position < safe))
				.map(|(producer, _)| *producer)
				.filter(|producer| state.asked.get(producer).is_none_or(|asked| *asked < safe))
				.collect();
			for producer in &lagging {
				state.asked.insert(*producer, safe);
			}
			if !lagging.is_empty() {
				self.flow_tracker.wake_flows_now(lagging);
			}
			let moved = self.advance_view_cursors(state, &upstream_reads, cursor);
			if moved && merged.more {
				let _ = ctx.self_ref().send(FlowActorMessage::Drain);
			} else {
				if upstream_reads.reads.keys().any(|producer| {
					self.flow_tracker.last_commit(*producer).is_some_and(|commit| commit > safe)
				}) {
					ctx.schedule_once(self.wake_coalesce, || FlowActorMessage::Drain);
				}
				self.checkpoint_if_stale(state, ctx);
			}
			return;
		}
		state.backlog = merged.more;
		let step = self.compute_step(state, &merged.items, merged.target, merged.more);
		self.apply_step(state, ctx, step, Some(&upstream_reads));
	}

	fn read_range(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		from: CommitVersion,
		up_to: CommitVersion,
	) -> Option<StreamRead> {
		if let Some(read) = state.read_cache.get(from, up_to) {
			return Some(read);
		}
		match self.queue.pull(from, up_to, self.pull_batch_bytes) {
			CommitPull::Hit {
				entries,
				advance_to,
				more,
			} => {
				let items = state.read_cache.insert(from, entries, advance_to);
				Some(StreamRead {
					items,
					read_to: advance_to,
					more,
				})
			}
			CommitPull::Behind => {
				self.request_load(state, ctx, from, up_to);
				None
			}
		}
	}

	fn read_stream(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		stream: ReadStream,
		from: CommitVersion,
		up_to: CommitVersion,
	) -> Option<StreamRead> {
		if let Some(read) = state.held_reads.take(stream, from, up_to) {
			return Some(read);
		}
		self.read_range(state, ctx, from, up_to)
	}

	fn advance_view_cursors(
		&self,
		state: &mut FlowActorState,
		reads: &UpstreamReads,
		advance_to: CommitVersion,
	) -> bool {
		let mut moved = false;
		for (producer, next) in reads.cursors_after(advance_to) {
			let current = state.view_cursors.entry(producer).or_insert(state.cursor);
			if next > *current {
				*current = next;
				moved = true;
			}
		}
		moved
	}

	fn apply_items(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		items: &[Arc<CommitEntry>],
		advance_to: CommitVersion,
		more: bool,
	) {
		state.backlog = more;
		let step = self.compute_step(state, items, advance_to, more);
		self.apply_step(state, ctx, step, None);
	}

	fn compute_step(
		&self,
		state: &mut FlowActorState,
		items: &[Arc<CommitEntry>],
		advance_to: CommitVersion,
		more: bool,
	) -> Result<SliceStep> {
		let computer = self.lookup_computer(state);
		catch_unwind(AssertUnwindSafe(|| {
			computer.compute_pulled(
				&mut state.flow_engine,
				items,
				SliceCursor {
					flow_id: self.flow_id,
					source_objects: &state.source_objects,
					completeness_objects: state.completeness_objects.as_deref(),
					cursor: state.cursor,
					lsn: state.cursor_lsn,
					frontier: state.frontier,
					durable_cursor: state.durable_cursor,
					fold_shape: self.flow_tracker.fold_shape(self.flow_id),
				},
				advance_to,
				more,
				&self.config,
				&mut state.hold_cache,
			)
		}))
		.unwrap_or_else(|payload| Err(Error(Box::new(flow_step_panicked(describe_payload(&payload))))))
	}

	fn apply_step(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		step: Result<SliceStep>,
		reads: Option<&UpstreamReads>,
	) {
		match step {
			Ok(SliceStep::Skip {
				advance_to,
				lsn,
				more,
				holds,
			}) => {
				state.retry_count = 0;
				state.flow_engine.accept_watermarks(false);
				if let Some(reads) = reads {
					self.advance_view_cursors(state, reads, advance_to);
				}
				state.cursor = advance_to;
				state.cursor_lsn = lsn;
				for hold in holds {
					self.substrate.frontiers.publish(hold.object, hold.frontier, advance_to);
				}
				self.move_lookup_lease(state);
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
				lsn,
				more,
				holds,
			}) => {
				state.pending_view_cursors =
					reads.map(|reads| reads.cursors_after(advance_to)).unwrap_or_default();
				self.dispatch_commit(state, ctx, slice, advance_to, lsn, more, holds);
			}
			Err(e) => {
				self.retry_or_poison(state, ctx, format!("flow step failed: {e}"));
			}
		}
	}

	fn request_load(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		from: CommitVersion,
		up_to: CommitVersion,
	) {
		state.awaiting_load = true;
		state.loading_from = from;
		let self_ref = ctx.self_ref().clone();
		let reply: CommitLoaderReply = Box::new(move |outcome| {
			let _ = self_ref.send(FlowActorMessage::Loaded {
				outcome,
			});
		});
		if self.loader
			.send(CommitLoaderMessage::Fetch {
				after: (from, state.cursor_lsn),
				up_to,
				budget: self.load_batch_bytes,
				reply,
			})
			.is_err()
		{
			state.awaiting_load = false;
			if !ctx.is_cancelled() {
				panic!("flow {} loader stopped outside shutdown", self.flow_id.0);
			}
		}
	}

	fn on_loaded(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>, outcome: Result<Loaded>) {
		state.awaiting_load = false;
		if state.poisoned {
			return;
		}
		match outcome {
			Ok(loaded) => {
				state.read_cache.insert(state.loading_from, loaded.entries, loaded.advance_to);
				if state.committing {
					state.drain_after_commit = true;
					return;
				}
				let _ = ctx.self_ref().send(FlowActorMessage::Drain);
			}
			Err(_) if state.committing => {
				state.drain_after_commit = true;
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
		lsn: Lsn,
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
			for (key, _) in slice.combined.iter_sorted() {
				assert!(
					reifydb_flow_async::transaction::read::read_from(key)
						!= reifydb_flow_async::transaction::read::ReadFrom::Query,
					"flow {:?} committed {:?}, a key it reads back through the query pinned at its cursor, which never sees this commit",
					self.flow_id,
					reifydb_core::key::tag::KeyTag::of(key)
				);
			}
		}
		state.hold_cache = None;
		state.committing = true;
		state.pending_lsn = lsn;
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
		if self.commit_target()
			.send(CommitterMessage::Slice {
				slice,
				reply,
			})
			.is_err()
		{
			state.committing = false;
			if !ctx.is_cancelled() {
				panic!("flow {} committer stopped outside shutdown", self.flow_id.0);
			}
		}
	}

	fn on_slice_committed(
		&self,
		state: &mut FlowActorState,
		ctx: &Context<FlowActorMessage>,
		advance_to: CommitVersion,
		more: bool,
		result: Result<()>,
		committed: Option<CommitVersion>,
	) {
		state.committing = false;
		let holds = take(&mut state.pending_holds);
		if result.is_ok()
			&& let Some(version) = committed
		{
			let at = if version > CommitVersion(0) {
				version
			} else {
				advance_to
			};
			for hold in holds {
				self.substrate.frontiers.publish(hold.object, hold.frontier, at);
			}
		}
		match result {
			Ok(()) => {
				state.retry_count = 0;
				state.flow_engine.accept_watermarks(true);
				state.backfilling = false;
				*self.create_snapshot.lock() = None;
				for (producer, cursor) in take(&mut state.pending_view_cursors) {
					let current = state.view_cursors.entry(producer).or_insert(cursor);
					if cursor > *current {
						*current = cursor;
					}
				}
				state.cursor = advance_to;
				state.cursor_lsn = state.pending_lsn;
				state.durable_cursor = advance_to;
				state.last_checkpoint_at = self.clock.now();
				for reply in take(&mut state.checkpoint_replies) {
					reply();
				}
				self.move_lookup_lease(state);
				self.publish_position(advance_to);
				self.resume_after_commit(state, ctx, more);
			}
			Err(e) => {
				state.pending_view_cursors.clear();
				state.checkpoint_replies.clear();
				self.retry_or_poison(state, ctx, format!("slice commit failed: {e}"));
			}
		}
	}

	fn on_tick_committed(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>, result: Result<()>) {
		state.committing = false;
		match result {
			Ok(()) => {
				state.retry_count = 0;
				state.flow_engine.accept_watermarks(true);
				self.move_lookup_lease(state);
				self.flow_tracker.record_tick(self.flow_id);
				self.resume_after_commit(state, ctx, false);
			}
			Err(e) => {
				self.retry_or_poison(state, ctx, format!("tick commit failed: {e}"));
			}
		}
	}

	fn on_publish_restored_frontiers(&self, state: &mut FlowActorState) {
		if state.poisoned {
			return;
		}
		let resolved = self.computer.resolved_holds(&mut state.flow_engine, self.flow_id, self.initial_cursor);
		state.flow_engine.discard_staged_watermarks();
		match resolved {
			Ok((holds, _)) => {
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
		state.backlog = more;
		if more || take(&mut state.drain_after_commit) {
			let _ = ctx.self_ref().send(FlowActorMessage::Drain);
		}
	}

	fn on_tick(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>) {
		let mut retrying = false;
		if self.ticks_enabled && !state.poisoned && !state.committing && !state.backfilling {
			let ticked =
				self.lookup_computer(state).tick(&mut state.flow_engine, self.flow_id, state.cursor);
			match ticked {
				Ok((pending, emitted)) => {
					state.retry_count = 0;
					if produced_output(&pending, emitted) {
						self.dispatch_tick_commit(state, ctx, pending);
					} else {
						state.flow_engine.accept_watermarks(false);
						self.flow_tracker.record_tick(self.flow_id);
					}
				}
				Err(e) => {
					self.retry_or_poison(state, ctx, format!("flow tick failed: {e}"));
					retrying = true;
				}
			}
		}

		ctx.schedule_once(self.tick_interval(), || FlowActorMessage::Tick);

		if !retrying && !state.poisoned && !state.committing {
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

	fn dispatch_tick_commit(&self, state: &mut FlowActorState, ctx: &Context<FlowActorMessage>, pending: Pending) {
		reifydb_assertions! {
			for (key, _) in pending.iter_sorted() {
				assert!(
					reifydb_flow_async::transaction::read::read_from(key)
						!= reifydb_flow_async::transaction::read::ReadFrom::Query,
					"flow {:?} committed {:?} on a tick, a key it reads back through the query pinned at its cursor, which never sees this commit",
					self.flow_id,
					reifydb_core::key::tag::KeyTag::of(key)
				);
			}
		}
		state.hold_cache = None;
		state.committing = true;
		state.drain_after_commit = true;
		let self_ref = ctx.self_ref().clone();
		let reply: TickCommitReply = Box::new(move |result| {
			let _ = self_ref.send(FlowActorMessage::TickCommitted {
				result,
			});
		});
		if self.commit_target()
			.send(CommitterMessage::Tick {
				flow_id: self.flow_id,
				members: self.members.iter().map(|(member, _)| member.id).collect(),
				source: SourceVersion(state.cursor.0 + 1),
				pending,
				reply,
			})
			.is_err()
		{
			state.committing = false;
			if !ctx.is_cancelled() {
				panic!("flow {} committer stopped outside shutdown", self.flow_id.0);
			}
		}
	}

	fn stop_if_pending(&self, state: &mut FlowActorState) -> Directive {
		if state.committing {
			return Directive::Continue;
		}
		let Some((delete_checkpoint, reply)) = state.pending_stop.take() else {
			return Directive::Continue;
		};
		state.flow_engine.forget_operator_samples();
		self.on_stop(delete_checkpoint);
		self.health.clear(self.flow_id);
		(reply)();
		Directive::Stop
	}

	fn on_stop(&self, delete_checkpoint: bool) {
		if delete_checkpoint {
			let mut slice = FlowSlice::empty();
			slice.checkpoint_deletes.extend(self.unit_ids());
			let reply: SliceCommitReply = Box::new(|_| {});
			let _ = self.committer.send(CommitterMessage::Slice {
				slice,
				reply,
			});
		}
	}
}

fn hold_reads(
	state: &mut FlowActorState,
	cursor: CommitVersion,
	tables: StreamRead,
	reads: UpstreamReads,
	froms: &HashMap<FlowId, CommitVersion>,
) {
	state.held_reads.hold(ReadStream::Tables, cursor, tables);
	for (producer, upstream) in reads.reads {
		if let Some(from) = froms.get(&producer) {
			state.held_reads.hold(ReadStream::Upstream(producer), *from, upstream.read);
		}
	}
}

impl Actor for FlowActor {
	type State = FlowActorState;
	type Message = FlowActorMessage;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		let mut flow_engine = self.build_flow_engine();
		let mut poisoned = match self.register_flow(&mut flow_engine) {
			Ok(()) => false,
			Err(e) => {
				error!(flow_id = self.flow_id.0, error = %e, "failed to register flow, poisoning");
				self.report_poisoned(ctx, format!("registration failed: {e}"));
				true
			}
		};
		let lookup_lease = match self.restore_lookup_lease() {
			Ok(lease) => lease,
			Err(e) => {
				error!(flow_id = self.flow_id.0, error = %e, "failed to take the lookup lease, poisoning");
				self.report_poisoned(ctx, format!("lookup lease failed: {e}"));
				poisoned = true;
				None
			}
		};

		self.publish_position(self.initial_cursor);

		ctx.schedule_once(self.tick_interval(), || FlowActorMessage::Tick);
		if let Some(interval) = self.sample_interval() {
			ctx.schedule_once(interval, || FlowActorMessage::Sample);
		}
		if self.initial_cursor > CommitVersion::BEFORE_FIRST {
			let _ = ctx.self_ref().send(FlowActorMessage::PublishRestoredFrontiers);
		}

		let state = FlowActorState {
			flow_engine,
			pending_stop: None,
			source_objects: self.initial_source_objects.clone(),
			completeness_objects: self.initial_completeness_objects.clone(),
			cursor: self.initial_cursor,
			cursor_lsn: self.initial_cursor_lsn,
			pending_lsn: self.initial_cursor_lsn,
			frontier: (CommitVersion(0), Lsn::new(0)),
			durable_cursor: self.initial_cursor,
			committing: false,
			awaiting_load: false,
			poisoned,
			retry_count: 0,
			pending_holds: WatermarkHolds::new(),
			drain_after_commit: false,
			last_checkpoint_at: self.clock.now(),
			checkpoint_replies: Vec::new(),
			view_cursors: HashMap::new(),
			pending_view_cursors: HashMap::new(),
			asked: HashMap::new(),
			read_cache: ReadCache::default(),
			held_reads: HeldReads::default(),
			loading_from: self.initial_cursor,
			backfilling: self.backfill,
			snapshot: None,
			lookup_lease,
			hold_cache: None,
			backlog: false,
		};

		if !state.poisoned {
			let _ = ctx.self_ref().send(FlowActorMessage::Drain);
		}

		state
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			FlowActorMessage::Drain => {
				self.on_drain(state, ctx);
				Directive::Continue
			}
			FlowActorMessage::Wake => {
				if !state.poisoned {
					ctx.schedule_once(self.wake_coalesce, || FlowActorMessage::TrailingWake);
					if state.committing || state.awaiting_load {
						state.drain_after_commit = true;
					} else {
						let _ = ctx.self_ref().send(FlowActorMessage::Drain);
					}
				}
				Directive::Continue
			}
			FlowActorMessage::TrailingWake => {
				if !state.poisoned && self.wake_pending.load(Ordering::SeqCst) {
					ctx.schedule_once(self.wake_coalesce, || FlowActorMessage::TrailingWake);
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
				self.stop_if_pending(state)
			}
			FlowActorMessage::TickCommitted {
				result,
			} => {
				self.on_tick_committed(state, ctx, result);
				self.stop_if_pending(state)
			}
			FlowActorMessage::CheckpointNow {
				reply,
			} => {
				if state.poisoned || state.backfilling || state.awaiting_load {
					return Directive::Continue;
				}
				if state.committing {
					state.checkpoint_replies.push(reply);
				} else if state.cursor > state.durable_cursor {
					state.checkpoint_replies.push(reply);
					state.last_checkpoint_at = self.clock.now();
					self.checkpoint_cursor(state, ctx);
				}
				Directive::Continue
			}
			FlowActorMessage::Stop {
				delete_checkpoint,
				reply,
			} => {
				state.pending_stop = Some((delete_checkpoint, reply));
				self.stop_if_pending(state)
			}
		}
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().batch_size(self.batch_size)
	}
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod pull_protocol {
	use std::{
		collections::HashMap,
		sync::{Arc, mpsc},
		thread::sleep,
	};

	use reifydb_codec::row::{bytes::EncodedBytes, pod::EncodedPodRow};
	use reifydb_core::{
		actors::{flow::FlowActorHandle, pending::Pending},
		commit::{
			loader::{CommitLoader, CommitLoaderHandle, CommitLoaderMetrics},
			queue::{CommitQueueFloor, FlowQueueFloor},
			watermark::ReaderWatermark,
		},
		delta::{Delta, RemoveAnnounce, SetPre},
		interface::{
			catalog::{
				flow::OperatorId,
				ringbuffer::{RingBufferMetadata, decode_ringbuffer_metadata},
			},
			change::{Change, ChangeOrigin, Diff},
			commit::{CommitEntry, FlowWal},
		},
		key::{
			any::TaggedKey,
			bound::TaggedKeyBoundRange,
			operator::state::{KeyspaceId, OperatorStateKey},
			tag::KeyTag,
		},
		metrics::{collect::MetricsCollector, sample::Reading},
	};
	use reifydb_engine::lift::{entry_changed_objects, lift_entry_changes, tagged_row_target};
	use reifydb_flow_async::{
		operator::provider::EmptyOperatorProvider,
		transaction::{
			DeferredParams, FlowTransaction, deferred::DeferredTransaction, watermark::SourceWatermarks,
		},
	};
	use reifydb_runtime::{
		context::{RuntimeContext, spawner::Spawner, transaction::TransactionContext},
		sync::waiter::WaiterHandle,
	};
	use reifydb_store_operator::store::OperatorStore;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::{
		commit::{CommitBegin, CommitHandle},
		multi::RangeScope,
		transaction::Transaction,
	};
	use reifydb_value::{
		byte_size::ByteSize,
		value::{Value, system_columns::row_numbers},
	};
	use reifydb_wal::sync::WalMode;

	use super::{
		tick_failures::{PoisonRecorder, idle},
		*,
	};
	use crate::{
		commit::{
			committer::{Committer, CommitterActor, CommitterHandle},
			persist,
			quiescence::FlowMaterialization,
		},
		discovery::routing,
		progress::tracker::{FLOW_WAKE_COALESCE_NANOS, FlowWaker},
	};

	fn system_ctx() -> Arc<TransactionContext> {
		Arc::new(TransactionContext::system(RuntimeContext::testing(0, 0)))
	}

	struct Harness {
		te: TestEngine,
		engine: StandardEngine,
		queue: CommitQueue,
		flow_floor: CommitQueueFloor,
		control: ControlFrontier,
		tracker: FlowPositionTracker,
		committer_handle: CommitterHandle,
		loader_handle: CommitLoaderHandle,
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
		let te = TestEngine::builder().with_commit_queue().with_wal(|wal| wal.mode(WalMode::default())).build();
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

		let mut query = engine.begin_query(IdentityId::system()).expect("query");
		let flows = engine.catalog().list_flows_all(&mut Transaction::Query(&mut query)).expect("list flows");
		let flow_id = flows.first().expect("one flow").id;
		drop(query);

		let substrate = FlowSubstrate::with_dictionary(engine.dictionary_allocators(), engine.operator_state());
		let mut probe = FlowEngineInner::new(
			engine.catalog(),
			engine.executor().routines.clone(),
			RuntimeContext::builder(Spawner::inline()).clock(engine.clock().clone()).build(),
			Arc::new(EmptyOperatorProvider),
			substrate.clone(),
			OperatorSampleRegistry::new(),
		);
		let mut txn = engine.begin_command(IdentityId::system()).expect("command");
		let flow =
			engine.catalog().get_flow_dag(&mut Transaction::Command(&mut txn), flow_id).expect("load flow");
		probe.register(&mut txn, flow.clone()).expect("register probe");
		txn.rollback().expect("rollback probe");

		let source_objects = {
			let graph = probe.get_dependency_graph();
			let registered = |f: FlowId| f == flow_id;
			let view_kind = |vid| engine.catalog().cache().find_view(vid).map(|v| v.kind());
			Arc::new(routing::flow_source_objects(&graph, flow_id, &registered, &view_kind))
		};

		let tracker = FlowPositionTracker::new();
		let committer = Committer::new(
			tracker.clone(),
			FlowMaterialization::new(ReaderWatermark::new(), FlowPositionTracker::new()),
			substrate.operators.clone().expect("the test substrate carries an operator store"),
		);
		let begin_engine = engine.clone();
		let begin: CommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
		let committer_handle = engine.spawner().spawn_flow(
			"pull-protocol-committer",
			CommitterActor::new(committer, CommitHandle::new(begin)),
		);

		let flow_wal = engine.ioc().resolve::<FlowWal>().expect("test harness must register the flow wal");
		let loader_handle = engine.spawner().spawn_flow(
			"pull-protocol-loader",
			CommitLoader::new(flow_wal.log.clone(), CommitLoaderMetrics::default()),
		);

		let queue = engine.ioc().resolve::<CommitQueue>().expect("test harness must register the commit queue");
		let FlowQueueFloor(flow_floor) = engine
			.ioc()
			.resolve::<FlowQueueFloor>()
			.expect("test harness must register the flow queue floor");

		Harness {
			te,
			engine,
			queue,
			flow_floor,
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
			self.spawn_flow_actor(self.flow.clone(), self.source_objects.clone(), cursor, substrate)
		}

		fn spawn_flow_actor(
			&self,
			flow: FlowDag,
			source_objects: Arc<BTreeSet<ObjectId>>,
			cursor: CommitVersion,
			substrate: FlowSubstrate,
		) -> FlowActorHandle {
			self.spawn_flow_actor_on(self.queue.clone(), flow, source_objects, cursor, substrate)
		}

		fn spawn_flow_actor_on(
			&self,
			queue: CommitQueue,
			flow: FlowDag,
			source_objects: Arc<BTreeSet<ObjectId>>,
			cursor: CommitVersion,
			substrate: FlowSubstrate,
		) -> FlowActorHandle {
			let flow_id = flow.id;
			self.await_safe_watermark(cursor);

			let handle = self.engine.spawner().spawn_flow(
				"pull-protocol-flow",
				FlowActor::new(FlowActorParams {
					engine: self.engine.clone(),
					supervisor: idle(&self.engine, "pull-protocol-supervisor"),
					committer: self.committer_handle.actor_ref().clone(),
					terminal_committer: self.committer_handle.actor_ref().clone(),
					queue,
					loader: self.loader_handle.actor_ref().clone(),
					control: self.control.clone(),
					custom_operators: CustomOperators::new(HashMap::new()),
					substrate,
					operator_samples: OperatorSampleRegistry::new(),
					clock: self.engine.clock().clone(),
					health: self.health.clone(),
					flow_tracker: self.tracker.clone(),
					flow,
					members: Vec::new(),
					source_objects,
					completeness_objects: None,
					cursor: CommitVersion(cursor.0 - 1),
					cursor_lsn: Lsn::new(0),
					pull_batch_bytes: ByteSize::from_mib(8),
					load_batch_bytes: ByteSize::from_mib(8),
					checkpoint_lag: 10_000,
					checkpoint_max_age: Duration::from_milliseconds(5_000).unwrap(),
					retry_limit: 3,
					retry_backoff: Duration::from_milliseconds(50).unwrap(),
					wake_coalesce: Duration::from_nanoseconds_const(FLOW_WAKE_COALESCE_NANOS),
					batch_size: 8,
				}),
			);

			assert_eq!(
				self.await_flow_position(flow_id, cursor, seconds(10)),
				Some(cursor),
				"the init Drain must be consumed, and the cursor settled at {}, before the test \
				 writes anything a wake will deliver",
				cursor.0
			);
			handle
		}

		fn spawn_flow_actor_with_waker(
			&self,
			flow: FlowDag,
			source_objects: Arc<BTreeSet<ObjectId>>,
			cursor: CommitVersion,
			substrate: FlowSubstrate,
		) -> FlowActorHandle {
			let flow_id = flow.id;
			self.await_safe_watermark(cursor);

			let actor = FlowActor::new(FlowActorParams {
				engine: self.engine.clone(),
				supervisor: idle(&self.engine, "pull-protocol-supervisor"),
				committer: self.committer_handle.actor_ref().clone(),
				terminal_committer: self.committer_handle.actor_ref().clone(),
				queue: self.queue.clone(),
				loader: self.loader_handle.actor_ref().clone(),
				control: self.control.clone(),
				custom_operators: CustomOperators::new(HashMap::new()),
				substrate,
				operator_samples: OperatorSampleRegistry::new(),
				clock: self.engine.clock().clone(),
				health: self.health.clone(),
				flow_tracker: self.tracker.clone(),
				flow,
				members: Vec::new(),
				source_objects,
				completeness_objects: None,
				cursor: CommitVersion(cursor.0 - 1),
				cursor_lsn: Lsn::new(0),
				pull_batch_bytes: ByteSize::from_mib(8),
				load_batch_bytes: ByteSize::from_mib(8),
				checkpoint_lag: 10_000,
				checkpoint_max_age: Duration::from_milliseconds(5_000).unwrap(),
				retry_limit: 3,
				retry_backoff: Duration::from_milliseconds(50).unwrap(),
				wake_coalesce: Duration::from_nanoseconds_const(FLOW_WAKE_COALESCE_NANOS),
				batch_size: 8,
			});
			let pending = actor.wake_pending();
			let handle = self.engine.spawner().spawn_flow("pull-protocol-flow", actor);
			self.tracker.set_waker(
				flow_id,
				FlowWaker::new(handle.actor_ref().clone(), pending, self.engine.clock().clone()),
			);

			assert_eq!(
				self.await_flow_position(flow_id, cursor, seconds(10)),
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
			self.logged_entries()
				.iter()
				.filter(|entry| entry.version.commit > up_to)
				.filter(|entry| {
					entry_changed_objects(entry)
						.iter()
						.any(|object| matches!(object, ObjectId::View(_)))
				})
				.count()
		}

		fn await_view_rows(&self, want: usize, timeout: Duration) -> usize {
			let deadline = Clock::Real.instant() + timeout;
			loop {
				let got = self.view_rows();
				if got >= want || Clock::Real.instant() >= deadline {
					return got;
				}
				sleep(millis(10).to_std());
			}
		}

		fn await_safe_watermark(&self, want: CommitVersion) {
			let deadline = Clock::Real.instant() + seconds(10);
			loop {
				let safe = self.queue.released_through().min(self.engine.done_until());
				if safe >= want {
					return;
				}
				assert!(Clock::Real.instant() < deadline, "safe watermark never reached {}", want.0);
				sleep(millis(5).to_std());
			}
		}

		fn await_position(&self, want: CommitVersion, timeout: Duration) -> Option<CommitVersion> {
			self.await_flow_position(self.flow_id, want, timeout)
		}

		fn await_flow_position(
			&self,
			flow_id: FlowId,
			want: CommitVersion,
			timeout: Duration,
		) -> Option<CommitVersion> {
			let deadline = Clock::Real.instant() + timeout;
			loop {
				let got = self.tracker.all().get(&flow_id).copied();
				if got == Some(want) || Clock::Real.instant() >= deadline {
					return got;
				}
				sleep(millis(10).to_std());
			}
		}

		fn poll_until<T>(&self, timeout: Duration, mut probe: impl FnMut() -> Option<T>) -> Option<T> {
			let started = Clock::Real.instant();
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
			self.engine
				.operator_state()
				.checkpoint_get(self.flow_id)
				.unwrap()
				.map(|checkpoint| checkpoint.version)
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
			let mut txn = DeferredTransaction::new(DeferredParams {
				ctx: system_ctx(),
				version: self.engine.current_version().expect("current version"),
				pending: Pending::new(),
				query: Some(self
					.engine
					.multi()
					.begin_query(self.engine.multi().generate_transaction_id())
					.expect("query")),
				state_query: Some(self
					.engine
					.multi()
					.begin_query(self.engine.multi().generate_transaction_id())
					.expect("state query")),
				catalog: self.engine.catalog(),
				interceptors: self.engine.create_interceptors(),
				clock: self.engine.clock().clone(),
				substrate: substrate.clone(),
				lookup: None,
			});
			for source in sources {
				SourceWatermarks::advance(source, &mut txn, at).expect("advance watermark");
			}

			let advanced = txn.take_pending();
			let store = substrate.operators.as_ref().expect("the test substrate carries an operator store");
			persist(store, &advanced);
		}

		fn logged_entries(&self) -> Vec<Arc<CommitEntry>> {
			let released = self.te.await_released();
			let wal = self.engine.ioc().resolve::<FlowWal>().expect("the harness registers the flow wal");
			wal.log.read((CommitVersion(0), Lsn::new(0)), released, ByteSize::from_bytes(u64::MAX))
				.expect("read the commit log")
				.entries
		}

		fn view_change_beyond(&self, floor: CommitVersion) -> Option<Change> {
			let catalog = self.engine.catalog();
			let mut query = self.engine.begin_query(IdentityId::system()).expect("query transaction");
			let mut txn = Transaction::Query(&mut query);
			self.logged_entries()
				.into_iter()
				.filter(|entry| entry.version.commit > floor)
				.flat_map(|entry| {
					lift_entry_changes(&entry, &catalog, &mut txn, |_| true).expect("lift changes")
				})
				.find(|change| matches!(change.origin, ChangeOrigin::Object(ObjectId::View(_))))
		}

		fn reader_flow(&self) -> (FlowDag, Arc<BTreeSet<ObjectId>>, FlowUpstreams) {
			let mut query = self.engine.begin_query(IdentityId::system()).expect("query");
			let reader_id = self
				.engine
				.catalog()
				.list_flows_all(&mut Transaction::Query(&mut query))
				.expect("list flows")
				.into_iter()
				.map(|flow| flow.id)
				.find(|id| *id != self.flow_id)
				.expect("a second flow reads the producer's view");
			drop(query);

			let mut probe = FlowEngineInner::new(
				self.engine.catalog(),
				self.engine.executor().routines.clone(),
				RuntimeContext::builder(Spawner::inline()).clock(self.engine.clock().clone()).build(),
				Arc::new(EmptyOperatorProvider),
				self.substrate.clone(),
				OperatorSampleRegistry::new(),
			);
			let mut txn = self.engine.begin_command(IdentityId::system()).expect("command");
			let reader = self
				.engine
				.catalog()
				.get_flow_dag(&mut Transaction::Command(&mut txn), reader_id)
				.expect("load reader");
			probe.register(&mut txn, self.flow.clone()).expect("register producer probe");
			probe.register(&mut txn, reader.clone()).expect("register reader probe");
			txn.rollback().expect("rollback probe");

			let graph = probe.get_dependency_graph();
			let registered = |f: FlowId| f == self.flow_id || f == reader_id;
			let view_kind = |vid| self.engine.catalog().cache().find_view(vid).map(|v| v.kind());
			let source_objects =
				Arc::new(routing::flow_source_objects(&graph, reader_id, &registered, &view_kind));
			let upstreams = routing::flow_upstreams(&graph, reader_id, &view_kind);
			(reader, source_objects, upstreams)
		}

		fn flow_position(&self, flow_id: FlowId) -> Option<CommitVersion> {
			self.tracker.all().get(&flow_id).copied()
		}

		fn await_query_rows(&self, rql: &str, want: usize, timeout: Duration) -> Option<usize> {
			self.poll_until(timeout, || {
				Some(self.te.query(rql).first().map(|f| f.row_count()).unwrap_or(0))
					.filter(|got| *got >= want)
			})
		}

		fn pull_behinds(&self) -> Reading {
			let mut samples = Vec::new();
			self.queue.collect(&mut samples);
			samples.into_iter()
				.find(|sample| sample.metric == "pull_behinds")
				.expect("the commit queue reports its behind count")
				.reading
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

		let rows = h.await_view_rows(2, seconds(10));
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
	fn a_burst_of_commits_loses_no_version_and_commits_none_twice() {
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

		let rows = h.await_view_rows(total, seconds(15));
		assert_eq!(rows, total, "a burst must not drop an accumulated version");

		h.await_safe_watermark(h.engine.current_version().expect("current version"));
		let commits = h.view_bearing_records(v0);
		assert!(
			commits > 0 && commits <= total,
			"a flow with no readers folds pending source versions into one commit, so {total} inserts must \
			 yield between one and {total} commits: more means a version was committed twice, got {commits}"
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

		h.flow_floor.advance(target);
		h.wake(&actor);

		let rows = h.await_view_rows(total, seconds(15));
		assert_eq!(rows, total, "the loader path must recover every version evicted from the backlog");
		assert!(
			h.await_position_at_least(target, seconds(10)).is_some(),
			"the catch-up must advance the flow position to at least the safe bound it \
			 recovered through"
		);

		sleep(millis(200).to_std());
		assert_eq!(
			h.view_rows(),
			total,
			"no version may be applied twice across the loader chunk and later backlog pulls"
		);
		drop(actor);
	}

	#[test]
	fn a_gated_reader_resumes_from_its_published_position_after_the_backlog_is_evicted_there() {
		let h = harness();
		h.te.admin("CREATE DEFERRED VIEW app::w { id: int4 } AS { FROM app::v MAP { id } }");
		let (reader, reader_sources, upstreams) = h.reader_flow();
		let reader_id = reader.id;
		assert!(
			upstreams.contains_key(&h.flow_id),
			"precondition: the reader must gate on the producer, otherwise it never takes the merged path"
		);
		h.tracker.set_upstreams(reader_id, upstreams);

		let v0 = h.engine.current_version().expect("current version");
		let producer = h.spawn_actor(v0);
		let consumer = h.spawn_flow_actor(reader, reader_sources, v0, h.substrate.clone());

		h.te.command("INSERT app::t [{ id: 1 }]");
		let first = h.engine.current_version().expect("current version");
		h.await_safe_watermark(first);
		h.wake(&producer);
		assert_eq!(h.await_view_rows(1, seconds(10)), 1, "the producer must emit the first row");
		let producer_position = h
			.await_position_at_least(first, seconds(10))
			.expect("the producer must publish its first commit");

		h.te.command("INSERT app::t [{ id: 2 }]");
		let read_end = h.engine.current_version().expect("current version");
		assert!(
			producer_position < read_end,
			"precondition: the producer must lag the version the reader reads through, otherwise the gate \
			 does not hold the reader below its read end"
		);
		h.await_safe_watermark(read_end);
		h.wake(&consumer);
		assert_eq!(
			h.await_query_rows("FROM app::w", 1, seconds(10)),
			Some(1),
			"the reader must handle the view row stamped with the first insert"
		);
		let floor = h
			.poll_until(seconds(10), || h.flow_position(reader_id).filter(|position| *position >= first))
			.expect("the reader must publish the position its first commit reached");

		let behinds = h.pull_behinds();
		h.flow_floor.advance(floor);

		h.wake(&producer);
		assert_eq!(h.await_view_rows(2, seconds(10)), 2, "the producer must emit the second row");
		assert!(
			h.await_position_at_least(read_end, seconds(10)).is_some(),
			"the producer must publish the commit carrying the second row"
		);
		h.await_safe_watermark(h.engine.current_version().expect("current version"));
		h.wake(&consumer);

		assert_eq!(
			h.await_query_rows("FROM app::w", 2, seconds(15)),
			Some(2),
			"the reader must handle the view row stamped after its published position"
		);
		assert_eq!(
			h.pull_behinds(),
			behinds,
			"a pull from the published position fell behind an eviction at that position, so the position \
			 passed a version the reader had not handled"
		);
		drop(consumer);
		drop(producer);
	}

	#[test]
	fn a_gated_reader_held_below_its_safe_bound_wakes_its_lagging_producer() {
		let h = harness();
		h.te.admin("CREATE DEFERRED VIEW app::w { id: int4 } AS { FROM app::v MAP { id } }");
		let (reader, reader_sources, upstreams) = h.reader_flow();
		let reader_id = reader.id;
		assert!(
			upstreams.contains_key(&h.flow_id),
			"precondition: the reader must gate on the producer, otherwise it never takes the merged path"
		);
		h.tracker.set_upstreams(reader_id, upstreams);
		let v0 = h.engine.current_version().expect("current version");
		h.control.store(CommitVersion(u64::MAX));
		let producer = h.spawn_flow_actor_with_waker(
			h.flow.clone(),
			h.source_objects.clone(),
			v0,
			h.substrate.clone(),
		);
		let consumer = h.spawn_flow_actor_with_waker(reader, reader_sources, v0, h.substrate.clone());

		h.te.command("INSERT app::t [{ id: 1 }]");
		let safe = h.engine.current_version().expect("current version");
		h.await_safe_watermark(safe);
		assert_eq!(
			h.flow_position(h.flow_id),
			Some(v0),
			"precondition: the producer must still sit at v0, or the reader is never held on it"
		);
		h.wake(&consumer);

		assert!(
			h.await_position_at_least(safe, seconds(2)).is_some(),
			"the held reader must wake its producer, or the producer stays at v0 until a full wake that never comes"
		);
		assert_eq!(
			h.await_query_rows("FROM app::w", 1, seconds(2)),
			Some(1),
			"the producer's confirmed position must release the reader, so the row reaches app::w"
		);
		drop(consumer);
		drop(producer);
	}

	#[test]
	fn a_held_reader_wakes_its_lagging_producer_once_per_safe_bound() {
		struct DrainingRecorder {
			pending: Arc<AtomicBool>,
			received: mpsc::Sender<()>,
		}

		impl Actor for DrainingRecorder {
			type State = ();
			type Message = FlowActorMessage;

			fn init(&self, _ctx: &Context<Self::Message>) {}

			fn handle(
				&self,
				_state: &mut (),
				msg: Self::Message,
				_ctx: &Context<Self::Message>,
			) -> Directive {
				if matches!(msg, FlowActorMessage::Wake) {
					self.pending.store(false, Ordering::SeqCst);
					self.received.send(()).expect("the test still counts wakes");
				}
				Directive::Continue
			}

			fn config(&self) -> ActorConfig {
				ActorConfig::new()
			}
		}

		let h = harness();
		h.te.admin("CREATE DEFERRED VIEW app::w { id: int4 } AS { FROM app::v MAP { id } }");
		let (reader, reader_sources, upstreams) = h.reader_flow();
		let reader_id = reader.id;
		assert!(
			upstreams.contains_key(&h.flow_id),
			"precondition: the reader must gate on the producer, otherwise it never takes the merged path"
		);
		h.tracker.set_upstreams(reader_id, upstreams);
		h.control.store(CommitVersion(u64::MAX));
		let v0 = h.engine.current_version().expect("current version");
		let producer = h.spawn_actor(v0);
		let consumer = h.spawn_flow_actor(reader, reader_sources, v0, h.substrate.clone());

		let (sender, received) = mpsc::channel();
		let pending = Arc::new(AtomicBool::new(false));
		let recorder =
			h.engine.spawner()
				.spawn_ephemeral(
					"draining-recorder",
					DrainingRecorder {
						pending: Arc::clone(&pending),
						received: sender,
					},
				)
				.actor_ref()
				.clone();
		h.tracker.set_waker(h.flow_id, FlowWaker::new(recorder, pending, h.engine.clock().clone()));

		h.te.command("INSERT app::t [{ id: 1 }]");
		let safe = h.engine.current_version().expect("current version");
		h.await_safe_watermark(safe);
		for _ in 0..5 {
			assert!(consumer.actor_ref().send(FlowActorMessage::Drain).is_ok(), "send drain");
			sleep(millis(30).to_std());
		}
		sleep(millis(100).to_std());

		let wakes = received.try_iter().count();
		assert_eq!(
			wakes, 1,
			"five drains held at one safe bound must wake the producer exactly once, or each drain buys it an idle drain"
		);
		drop(consumer);
		drop(producer);
	}

	#[test]
	fn a_gated_reader_far_behind_the_backlog_converges_through_the_loader() {
		let h = harness_with(
			"CREATE TABLE app::t { id: int4, pad: utf8 }",
			"CREATE DEFERRED VIEW app::v { id: int4, pad: utf8 } AS { FROM app::t MAP { id, pad } }",
		);
		h.te.admin("CREATE DEFERRED VIEW app::w { id: int4, pad: utf8 } AS { FROM app::v MAP { id, pad } }");
		let (reader, reader_sources, upstreams) = h.reader_flow();
		let reader_id = reader.id;
		h.tracker.set_upstreams(reader_id, upstreams);

		let v0 = h.engine.current_version().expect("current version");
		let producer = h.spawn_actor(v0);
		let consumer = h.spawn_flow_actor(reader, reader_sources, v0, h.substrate.clone());

		let total = 16;
		let pad = "x".repeat(700 * 1024);
		for id in 0..total {
			h.te.command(&format!("INSERT app::t [{{ id: {id}, pad: \"{pad}\" }}]"));
		}
		let target = h.engine.current_version().expect("current version");
		h.await_safe_watermark(target);
		h.wake(&producer);
		assert!(
			h.await_position_at_least(target, seconds(30)).is_some(),
			"the producer must consume every insert before the reader is exercised"
		);
		let produced = h.engine.current_version().expect("current version");
		h.await_safe_watermark(produced);
		h.wake(&producer);
		assert!(
			h.await_position_at_least(produced, seconds(30)).is_some(),
			"precondition: the producer must publish a position through every version the reader reads, or \
			 the reader parks below it and this harness has no waker to move it on"
		);
		assert_eq!(
			h.flow_position(reader_id),
			Some(v0),
			"precondition: the reader must not have consumed anything yet, or the eviction below proves nothing"
		);

		h.flow_floor.advance(produced);
		h.wake(&consumer);

		assert!(
			h.poll_until(seconds(30), || h
				.flow_position(reader_id)
				.filter(|position| *position >= produced))
				.is_some(),
			"a reader far behind the backlog must converge through the loader; a merged drain that drops \
			 its partial reads on every loader round trip reloads the same windows forever and never \
			 advances its cursor"
		);
		drop(consumer);
		drop(producer);
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

		sleep(millis(300).to_std());
		assert_eq!(
			h.tracker.all().get(&h.flow_id).copied(),
			Some(v0),
			"a flow must hold at the control frontier even though the safe watermark is beyond it"
		);
		assert_eq!(h.view_rows(), 0, "no row above the control frontier may materialize");

		h.control.store(target);
		h.wake(&actor);
		assert_eq!(
			h.await_view_rows(2, seconds(10)),
			2,
			"raising the frontier and waking must release exactly the held-back versions"
		);
		assert_eq!(h.await_position(target, seconds(5)), Some(target));
		drop(actor);
	}

	#[test]
	fn a_producer_watermark_overshoot_does_not_stall_or_overrun_the_pull() {
		// A queue released past done_until must never let the pull overrun the done mark.
		let h = harness();
		let v0 = h.engine.current_version().expect("current version");
		h.control.store(CommitVersion(u64::MAX));
		let ahead = CommitQueue::new(ByteSize::from_mib(8), true, CommitVersion(h.engine.done_until().0 + 15));
		let actor = h.spawn_flow_actor_on(
			ahead.clone(),
			h.flow.clone(),
			h.source_objects.clone(),
			v0,
			h.substrate.clone(),
		);

		h.te.command("INSERT app::t [{ id: 1 }]");
		let target = h.engine.current_version().expect("current version");
		h.await_safe_watermark(target);

		assert!(
			ahead.released_through().0 >= h.engine.done_until().0 + 5,
			"test precondition: producer watermark must overshoot done_until by more than the 			 test's own commits"
		);

		h.wake(&actor);
		assert_eq!(
			h.await_view_rows(1, seconds(10)),
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
						with { duration: 1s, lateness: 0s }
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
				h.await_view_rows(expected_rows, seconds(10)),
				expected_rows,
				"each arrival must land in its own bucket, or the tick has no sealed group to commit"
			);
			assert_eq!(
				h.await_position(target, seconds(5)),
				Some(target),
				"the wake must settle the cursor before the tick, so that any later move is \
				 the tick's doing and nothing else's"
			);
		}

		h.te.command("INSERT app::unrelated [{ id: 1 }]");
		let gap = h.engine.current_version().expect("current version");
		h.await_safe_watermark(gap);
		assert!(gap > target, "the test needs unconsumed safe versions above the settled cursor");

		assert!(actor.actor_ref().send(FlowActorMessage::Tick).is_ok(), "send tick");

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
						with { duration: 1s, lateness: 0s }
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
			h.await_view_rows(1, seconds(10)),
			1,
			"the woken row must reach the window, or no seal timer is ever armed"
		);
		assert_eq!(
			h.await_position(target, seconds(5)),
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
			h.await_version_beyond(pre_tick, seconds(2)).is_some(),
			"a tick that writes only operator state still commits its own record (OP4), so a version must move \
			 here; the checkpoint check below must still not credit that commit"
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

	fn ring_harness() -> Harness {
		harness_with(
			"CREATE TABLE app::t { id: int4, v: int4, ts: datetime } with { time: event(ts) }",
			"CREATE DEFERRED RINGBUFFER VIEW app::v { id: int4, v: int4 } \
			 WITH { capacity: 1000, row: { ttl: 1s } } \
			 AS { FROM app::t map { id, v } }",
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
			h.await_view_rows(1, seconds(10)),
			1,
			"the row must land in the ring before its ttl can have anything to evict"
		);
		assert_eq!(
			h.await_position(target, seconds(5)),
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
	fn a_tick_fired_ttl_eviction_never_lands_in_the_commits_change_stream() {
		let h = ring_harness();
		let (_actor, pre_tick) = drive_ring_ttl_tick(&h);

		assert_eq!(
			h.poll_until(seconds(10), || (h.view_rows() == 0).then_some(())),
			Some(()),
			"precondition: the ttl eviction must still remove the expired row from storage, \
			 otherwise there is no eviction whose silence could be asserted"
		);
		assert!(
			h.view_change_beyond(pre_tick).is_none(),
			"a ttl eviction must leave no view change record on the tick commit"
		);
	}

	#[test]
	fn a_tick_evictions_storage_delete_carries_the_slice_paths_pre_image() {
		let h = ring_harness();
		let (_actor, pre_tick) = drive_ring_ttl_tick(&h);

		let (evicted_key, evicted_pre) = h
			.poll_until(seconds(10), || {
				h.logged_entries()
					.into_iter()
					.filter(|entry| entry.version.commit > pre_tick)
					.flat_map(|entry| entry.deltas.clone())
					.find_map(|delta| match delta {
						Delta::Remove {
							key,
							announce:
								RemoveAnnounce::Announced {
									pre,
								}
								| RemoveAnnounce::Unobserved {
									pre,
								},
						} if key.kind() == KeyTag::Row => Some((key, Some(pre))),
						_ => None,
					})
			})
			.expect("the tick commit must delete the expired ring row");

		let inserted_post = h
			.logged_entries()
			.into_iter()
			.filter(|entry| entry.version.commit <= pre_tick)
			.flat_map(|entry| entry.deltas.clone())
			.find_map(|delta| match delta {
				Delta::Set {
					key,
					bytes,
					pre: SetPre::Absent,
				} if key == evicted_key => Some(bytes),
				_ => None,
			})
			.expect("the wake's slice commit must have inserted the ring row the tick later evicts");

		assert_eq!(
			evicted_pre,
			Some(inserted_post),
			"a tick-path Row remove must announce the same pre-image the slice path stored"
		);
	}

	fn capacity_ring_harness() -> Harness {
		harness_with(
			"CREATE TABLE app::t { id: int4, v: int4, ts: datetime } with { time: event(ts) }",
			"CREATE DEFERRED RINGBUFFER VIEW app::v { id: int4, v: int4 } \
			 WITH { capacity: 2, row: { ttl: 1h } } \
			 AS { FROM app::t map { id, v } }",
		)
	}

	#[test]
	fn only_the_capacity_overwrite_of_the_three_changes_survives_the_lift_as_a_remove_then_an_insert() {
		let h = capacity_ring_harness();
		let substrate = h.substrate.clone();
		let v0 = h.engine.current_version().expect("current version");
		h.control.store(CommitVersion(u64::MAX));
		let actor = h.spawn_actor_with_substrate(v0, substrate.clone());

		h.te.command(
			r#"INSERT app::t [{ id: 1, v: 10, ts: "1970-01-01T00:01:00Z" },
			                  { id: 2, v: 20, ts: "1970-01-01T00:01:01Z" }]"#,
		);
		let filled = h.engine.current_version().expect("current version");
		h.await_safe_watermark(filled);
		h.wake(&actor);
		assert_eq!(
			h.await_view_rows(2, seconds(10)),
			2,
			"the ring must be filled to capacity before an overflow can evict anything"
		);
		assert_eq!(
			h.await_position(filled, seconds(5)),
			Some(filled),
			"the fill must settle first, so every delete counted below is an eviction"
		);
		let stable = h.engine.current_version().expect("current version");

		h.te.command(r#"INSERT app::t [{ id: 3, v: 30, ts: "1970-01-01T00:01:02Z" }]"#);
		let overflow = h.engine.current_version().expect("current version");
		h.await_safe_watermark(overflow);
		h.wake(&actor);
		assert_eq!(
			h.await_position(overflow, seconds(10)),
			Some(overflow),
			"the overflow row must be applied, or capacity never evicts the oldest row"
		);

		h.advance_source_watermarks(&substrate, DateTime::from_millis(7_200_000));
		let pre_tick = h.engine.current_version().expect("current version");
		assert!(actor.actor_ref().send(FlowActorMessage::Tick).is_ok(), "send tick");
		let tick_version =
			h.await_version_beyond(pre_tick, seconds(10)).expect("the tick must commit the ttl eviction");
		h.await_safe_watermark(tick_version);

		let deletes = h
			.poll_until(seconds(10), || {
				let found: Vec<(TaggedKey, Option<EncodedBytes>, bool)> = h
					.logged_entries()
					.into_iter()
					.filter(|entry| entry.version.commit > stable)
					.flat_map(|entry| entry.deltas.clone())
					.filter_map(|delta| match delta {
						Delta::Remove {
							key,
							announce: RemoveAnnounce::Announced {
								pre,
							},
						} if key.kind() == KeyTag::Row => Some((key, Some(pre), true)),
						Delta::Remove {
							key,
							announce: RemoveAnnounce::Unobserved {
								pre,
							},
						} if key.kind() == KeyTag::Row => Some((key, Some(pre), false)),
						_ => None,
					})
					.collect();
				(found.len() == 2).then_some(found)
			})
			.expect("the two ttl expiries must reach the change stream");

		assert!(
			deletes.iter().all(|(_, pre, visible)| pre.is_some() && !*visible),
			"both ttl expiries are hidden and keep their pre-image for replication"
		);
		let overwrites: Vec<TaggedKey> = h
			.logged_entries()
			.into_iter()
			.filter(|entry| entry.version.commit > stable)
			.flat_map(|entry| entry.deltas.clone())
			.filter_map(|delta| match delta {
				Delta::Set {
					key,
					pre: SetPre::Present(_),
					..
				} if key.kind() == KeyTag::Row => Some(key),
				_ => None,
			})
			.collect();
		assert_eq!(overwrites.len(), 1, "the capacity overflow overwrites the oldest slot as one update");

		let catalog = h.engine.catalog();
		let mut query = h.engine.begin_query(IdentityId::system()).expect("query transaction");
		let mut txn = Transaction::Query(&mut query);
		let lifted: Vec<Diff> = h
			.logged_entries()
			.into_iter()
			.filter(|entry| entry.version.commit > stable)
			.flat_map(|entry| {
				lift_entry_changes(&entry, &catalog, &mut txn, |_| true).expect("lift changes")
			})
			.filter(|change| matches!(change.origin, ChangeOrigin::Object(ObjectId::View(_))))
			.flat_map(|change| change.diffs)
			.collect();

		assert_eq!(
			lifted.len(),
			2,
			"the lift must carry the overwrite as a remove then an insert and drop both ttl expiries"
		);
		let slot = tagged_row_target(&overwrites[0]).expect("row target").row;
		let Diff::Remove {
			pre,
			..
		} = &lifted[0]
		else {
			panic!("the overwrite must lift as a remove first, got {lifted:?}");
		};
		assert_eq!(
			row_numbers(pre).expect("#rownum").to_vec(),
			vec![slot],
			"the lifted remove names the overwritten slot"
		);
		let Diff::Insert {
			post,
			..
		} = &lifted[1]
		else {
			panic!("the overwrite must lift as an insert second, got {lifted:?}");
		};
		assert_eq!(
			row_numbers(post).expect("#rownum").to_vec(),
			vec![slot],
			"the lifted insert names the same slot"
		);
		drop(actor);
	}

	#[test]
	fn operator_state_lives_in_the_operator_store_and_never_reaches_the_multi_store() {
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
		assert_eq!(h.await_view_rows(2, seconds(10)), 2, "the aggregate must materialize its two groups");
		assert_eq!(h.await_position(target, seconds(5)), Some(target));

		assert!(
			h.substrate
				.operators
				.as_ref()
				.expect("the harness substrate carries an operator store")
				.total_bytes()
				.unwrap() > ByteSize::ZERO,
			"the aggregate's operator state must land in the shared operator store"
		);

		let query = h.engine.multi().begin_query(h.engine.multi().generate_transaction_id()).expect("query");
		for operator in h.flow.get_operator_ids() {
			let leaked = query
				.range(&system_ctx(), OperatorStateKey::node_range(operator), RangeScope::All, 1024)
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

		let logged_state_keys = h
			.logged_entries()
			.into_iter()
			.flat_map(|entry| entry.deltas.clone())
			.filter(|delta| delta.key().kind() == KeyTag::OperatorState)
			.count();
		assert_eq!(logged_state_keys, 0, "no logged commit entry may carry an OperatorState key either");

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

		let (restarted_store, _restarted_guard) = OperatorStore::testing_memory_with_persistent_sqlite();
		let substrate2 = FlowSubstrate::with_dictionary(h.engine.dictionary_allocators(), restarted_store);
		let operators2 = substrate2.operators.clone().expect("the test substrate carries an operator store");
		assert_eq!(
			operators2.total_bytes().unwrap(),
			ByteSize::ZERO,
			"the restarted operator store starts empty"
		);
		let committer2 = Committer::new(
			h.tracker.clone(),
			FlowMaterialization::new(ReaderWatermark::new(), FlowPositionTracker::new()),
			operators2.clone(),
		);
		let begin_engine = h.engine.clone();
		let begin: CommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
		let committer2_handle = h.engine.spawner().spawn_flow(
			"pull-protocol-committer-restart",
			CommitterActor::new(committer2, CommitHandle::new(begin)),
		);
		let health2 = FlowHealthRegistry::new();
		let (poisoned, reports) = mpsc::channel();
		let supervisor2 =
			h.engine.spawner()
				.spawn_ephemeral(
					"pull-protocol-restart-poison-recorder",
					PoisonRecorder {
						poisoned,
					},
				)
				.actor_ref()
				.clone();
		let actor2 = h.engine.spawner().spawn_flow(
			"pull-protocol-flow-restart",
			FlowActor::new(FlowActorParams {
				engine: h.engine.clone(),
				supervisor: supervisor2,
				committer: committer2_handle.actor_ref().clone(),
				terminal_committer: committer2_handle.actor_ref().clone(),
				queue: h.queue.clone(),
				loader: h.loader_handle.actor_ref().clone(),
				control: h.control.clone(),
				custom_operators: CustomOperators::new(HashMap::new()),
				substrate: substrate2.clone(),
				operator_samples: OperatorSampleRegistry::new(),
				clock: h.engine.clock().clone(),
				health: health2.clone(),
				flow_tracker: h.tracker.clone(),
				flow: h.flow.clone(),
				members: Vec::new(),
				source_objects: h.source_objects.clone(),
				completeness_objects: None,
				cursor: target,
				cursor_lsn: Lsn::new(0),
				pull_batch_bytes: ByteSize::from_mib(8),
				load_batch_bytes: ByteSize::from_mib(8),
				checkpoint_lag: 10_000,
				checkpoint_max_age: Duration::from_milliseconds(5_000).unwrap(),
				retry_limit: 3,
				retry_backoff: Duration::from_milliseconds(50).unwrap(),
				wake_coalesce: Duration::from_nanoseconds_const(FLOW_WAKE_COALESCE_NANOS),
				batch_size: 8,
			}),
		);

		h.te.command(r#"INSERT app::t [{id: 4, g: 3, ts: "1970-01-01T00:03:00Z"}]"#);
		let after_restart = h.engine.current_version().expect("current version");
		h.await_safe_watermark(after_restart);
		h.wake(&actor2);

		assert!(
			h.await_position_at_least(after_restart, seconds(10)).is_some(),
			"the restarted actor must boot against an empty operator store and keep consuming \
			 (position is {:?})",
			h.tracker.all().get(&h.flow_id).copied()
		);
		let reported = reports.try_recv();
		assert!(reported.is_err(), "an empty operator store at boot must not poison the flow: {reported:?}");
		assert!(
			operators2.total_bytes().unwrap() > ByteSize::ZERO,
			"the restarted flow must rebuild its state in its own operator store"
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
				h.await_position(target, seconds(10)),
				Some(target),
				"each row must settle, or the eviction it triggers has not run yet"
			);
		}
		assert_eq!(h.view_rows(), 3, "precondition: the ring must be at capacity, so head has moved");

		let stored = {
			let query =
				h.engine.multi()
					.begin_query(h.engine.multi().generate_transaction_id())
					.expect("query");
			let rows: Vec<_> = query
				.range(&system_ctx(), TaggedKeyBoundRange::all(), RangeScope::All, 100_000)
				.collect::<Result<Vec<_>>>()
				.expect("scan for the ringbuffer metadata row")
				.into_iter()
				.filter(|row| row.key.kind() == KeyTag::RingBufferMetadata)
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
					.as_ref()
					.expect("the harness substrate carries an operator store")
					.range_batch(OperatorStateKey::node_range(operator), u64::MAX)
					.unwrap()
					.items
					.into_iter()
					.filter(|(key, _)| {
						OperatorStateKey::decode_inner(key.as_slice())
							.is_some_and(|(_, ks, _)| ks == KeyspaceId::RINGBUFFER_META)
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
			"the operator state mirror and the mvcc row must carry the same base, sequence and count: \
			 the mirror is what a replay reads, so any drift makes the replay assign different \
			 storage row numbers than the live run did"
		);
		assert!(mirrored[0].next_sequence > 3, "precondition: the write cycle must have lapped the ring");
		drop(actor);
	}
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tick_failures {
	use std::{
		collections::HashMap,
		marker::PhantomData,
		sync::{Arc, atomic::AtomicUsize, mpsc},
	};

	use arrow_array::{ArrayRef, UInt64Array};
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::{
		actors::pending::Pending,
		common::{OperatorClass, WindowRequirements, WindowSizeDomain},
		flow::operator::{FlowNode, OperatorDef},
		interface::{
			catalog::{
				flow::{FlowEdge, OperatorId},
				id::ViewId,
			},
			change::{Change, Diff, Diffs},
			flow::OperatorCapability,
		},
		internal_error,
		operator_with::ApplyWith,
		state::timer::TimerKind,
		value::{batch::batch, column::factory::int4},
	};
	use reifydb_engine::engine::StandardEngine;
	use reifydb_flow_async::{
		operator::{BoxedHostOperator, HostOperator, TimedOutput, host::HostContext},
		timer::{Timer, wheel::TimerWheel},
		transaction::{
			DeferredParams, FlowTransaction, deferred::DeferredTransaction, watermark::SourceWatermarks,
		},
	};
	use reifydb_runtime::{
		actor::{
			context::Context,
			system::ActorConfig,
			testing::TestHarness,
			traits::{Actor, Directive},
		},
		context::{RuntimeContext, clock::MockClock, transaction::TransactionContext},
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::{
		Result as ValueResult,
		error::{Diagnostic, Error},
		factory::time::at_millis,
		value::{
			container::temporal_array::datetime_array,
			system_columns::{SystemColumn, with_system_column},
		},
	};

	use super::*;
	use crate::{
		builder::{CustomOperatorEntry, CustomOperators},
		commit::persist,
		progress::tracker::{FLOW_WAKE_COALESCE_NANOS, FlowWaker},
	};

	fn system_ctx() -> Arc<TransactionContext> {
		Arc::new(TransactionContext::system(RuntimeContext::testing(0, 0)))
	}

	const FLOW: FlowId = FlowId(9_000);
	const SOURCE: OperatorId = OperatorId(9_001);
	const TIMED: OperatorId = OperatorId(9_002);
	const DUE_MS: i64 = 5_000;
	const WATERMARK_MS: i64 = 10_000;
	const RETRY_LIMIT: u32 = 3;
	const SINK: OperatorId = OperatorId(9_003);
	const MEMBER: FlowId = FlowId(9_100);
	const MEMBER_SOURCE: OperatorId = OperatorId(9_101);
	const MEMBER_SEEN: OperatorId = OperatorId(9_102);

	struct Boom {
		id: OperatorId,
	}

	impl HostOperator for Boom {
		fn id(&self) -> OperatorId {
			self.id
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> ValueResult<Change> {
			Ok(change)
		}

		fn on_timer(&mut self, _host: &mut dyn HostContext, _timer: Timer) -> ValueResult<Option<TimedOutput>> {
			Err(Error(Box::new(Diagnostic {
				code: "TEST_TIMER_HANDLER_FAILED".to_string(),
				message: "the timer handler failed".to_string(),
				..Default::default()
			})))
		}
	}

	struct Silent {
		id: OperatorId,
	}

	impl HostOperator for Silent {
		fn id(&self) -> OperatorId {
			self.id
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> ValueResult<Change> {
			Ok(change)
		}

		fn on_timer(&mut self, _host: &mut dyn HostContext, _timer: Timer) -> ValueResult<Option<TimedOutput>> {
			Ok(None)
		}
	}

	struct Ring {
		id: OperatorId,
	}

	impl HostOperator for Ring {
		fn id(&self) -> OperatorId {
			self.id
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> ValueResult<Change> {
			Ok(change)
		}

		fn on_timer(&mut self, _host: &mut dyn HostContext, timer: Timer) -> ValueResult<Option<TimedOutput>> {
			let user = batch(vec![int4("id", [1])]).unwrap();
			let system: [(SystemColumn, ArrayRef); 4] = [
				(SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![1u64]))),
				(SystemColumn::CreatedAt, Arc::new(datetime_array([timer.due]))),
				(SystemColumn::UpdatedAt, Arc::new(datetime_array([timer.due]))),
				(SystemColumn::Time, Arc::new(datetime_array([timer.due]))),
			];
			let row = system.into_iter().fold(user, |columns, (column, array)| {
				with_system_column(columns, column, array).unwrap()
			});
			let mut diffs = Diffs::new();
			diffs.push(Diff::insert(row));
			Ok(Some(TimedOutput::new(diffs, timer.due)))
		}
	}

	struct Seen {
		id: OperatorId,
		rows: Arc<AtomicUsize>,
	}

	impl HostOperator for Seen {
		fn id(&self) -> OperatorId {
			self.id
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> ValueResult<Change> {
			self.rows.fetch_add(change.row_count(), Ordering::SeqCst);
			Ok(change)
		}

		fn on_timer(&mut self, _host: &mut dyn HostContext, _timer: Timer) -> ValueResult<Option<TimedOutput>> {
			Ok(None)
		}
	}

	struct Idle<M> {
		_marker: PhantomData<fn() -> M>,
	}

	impl<M: Send + 'static> Actor for Idle<M> {
		type State = ();
		type Message = M;

		fn init(&self, _ctx: &Context<Self::Message>) {}

		fn handle(&self, _state: &mut (), _msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
			Directive::Continue
		}

		fn config(&self) -> ActorConfig {
			ActorConfig::new()
		}
	}

	pub(super) fn idle<M: Send + 'static>(engine: &StandardEngine, name: &str) -> ActorRef<M> {
		engine.spawner()
			.spawn_ephemeral(
				name,
				Idle::<M> {
					_marker: PhantomData,
				},
			)
			.actor_ref()
			.clone()
	}

	pub(super) struct PoisonRecorder {
		pub(super) poisoned: mpsc::Sender<(Vec<FlowId>, String)>,
	}

	impl Actor for PoisonRecorder {
		type State = ();
		type Message = FlowSupervisorMessage;

		fn init(&self, _ctx: &Context<Self::Message>) {}

		fn handle(&self, _state: &mut (), msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
			if let FlowSupervisorMessage::Poisoned {
				flows,
				reason,
			} = msg
			{
				self.poisoned.send((flows, reason)).expect("the test still listens for poison reports");
			}
			Directive::Continue
		}

		fn config(&self) -> ActorConfig {
			ActorConfig::new()
		}
	}

	struct TickRecorder {
		sources: mpsc::Sender<SourceVersion>,
	}

	impl Actor for TickRecorder {
		type State = ();
		type Message = CommitterMessage;

		fn init(&self, _ctx: &Context<Self::Message>) {}

		fn handle(&self, _state: &mut (), msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
			if let CommitterMessage::Tick {
				source,
				..
			} = msg
			{
				self.sources.send(source).expect("the test still listens for tick commits");
			}
			Directive::Continue
		}

		fn config(&self) -> ActorConfig {
			ActorConfig::new()
		}
	}

	fn recorder(engine: &StandardEngine, sources: mpsc::Sender<SourceVersion>) -> ActorRef<CommitterMessage> {
		engine.spawner()
			.spawn_ephemeral(
				"tick-stamp-committer",
				TickRecorder {
					sources,
				},
			)
			.actor_ref()
			.clone()
	}

	fn timer_operators() -> CustomOperators {
		let mut map = HashMap::new();
		map.insert(
			"boom".to_string(),
			CustomOperatorEntry {
				factory: Arc::new(|id, _params, _with| {
					Ok(Box::new(Boom {
						id,
					}) as BoxedHostOperator)
				}),
				abi: None,
				version: "0.0.1".to_string(),
				description: "test-only operator whose timer handler always fails".to_string(),
				capabilities: 0,
				input: Vec::new(),
				output: Vec::new(),
				class: OperatorClass::Unmanaged,
				window: WindowRequirements {
					takes_window: false,
					kinds: &[],
					domain: WindowSizeDomain::Time,
					needs_pane: false,
				},
				unmanaged_because: None,
			},
		);
		map.insert(
			"silent".to_string(),
			CustomOperatorEntry {
				factory: Arc::new(|id, _params, _with| {
					Ok(Box::new(Silent {
						id,
					}) as BoxedHostOperator)
				}),
				abi: None,
				version: "0.0.1".to_string(),
				description: "test-only operator whose timer handler succeeds and emits nothing"
					.to_string(),
				capabilities: 0,
				input: Vec::new(),
				output: Vec::new(),
				class: OperatorClass::Unmanaged,
				window: WindowRequirements {
					takes_window: false,
					kinds: &[],
					domain: WindowSizeDomain::Time,
					needs_pane: false,
				},
				unmanaged_because: None,
			},
		);
		CustomOperators::new(map)
	}

	fn test_entry(factory: Arc<dyn Fn(OperatorId) -> BoxedHostOperator + Send + Sync>) -> CustomOperatorEntry {
		CustomOperatorEntry {
			factory: Arc::new(move |id, _params, _with| Ok(factory(id))),
			abi: None,
			version: "0.0.1".to_string(),
			description: "test-only operator for a fused unit".to_string(),
			capabilities: 0,
			input: Vec::new(),
			output: Vec::new(),
			class: OperatorClass::Unmanaged,
			window: WindowRequirements {
				takes_window: false,
				kinds: &[],
				domain: WindowSizeDomain::Time,
				needs_pane: false,
			},
			unmanaged_because: None,
		}
	}

	fn unit_operators(rows: Arc<AtomicUsize>) -> CustomOperators {
		let mut map = HashMap::new();
		map.insert(
			"ring".to_string(),
			test_entry(Arc::new(|id| {
				Box::new(Ring {
					id,
				}) as BoxedHostOperator
			})),
		);
		map.insert(
			"seen".to_string(),
			test_entry(Arc::new(move |id| {
				Box::new(Seen {
					id,
					rows: rows.clone(),
				}) as BoxedHostOperator
			})),
		);
		CustomOperators::new(map)
	}

	fn failing_unit_operators() -> CustomOperators {
		let mut map = HashMap::new();
		map.insert(
			"boom".to_string(),
			test_entry(Arc::new(|id| {
				Box::new(Boom {
					id,
				}) as BoxedHostOperator
			})),
		);
		map.insert(
			"seen".to_string(),
			test_entry(Arc::new(|id| {
				Box::new(Seen {
					id,
					rows: Arc::new(AtomicUsize::new(0)),
				}) as BoxedHostOperator
			})),
		);
		CustomOperators::new(map)
	}

	fn view_named(engine: &StandardEngine, name: &str) -> ViewId {
		let mut query = engine.begin_query(IdentityId::system()).expect("query");
		let mut txn = Transaction::Query(&mut query);
		let namespace = engine
			.catalog()
			.find_namespace_by_name(&mut txn, "app")
			.expect("namespace")
			.expect("the test declares namespace app");
		engine.catalog()
			.find_view_by_name(&mut txn, namespace.id(), name)
			.expect("view lookup")
			.expect("the test declares the view")
			.id()
	}

	fn apply_node(id: OperatorId, operator: &str) -> FlowNode {
		FlowNode::new(
			id,
			OperatorDef::Apply {
				operator: operator.to_string(),
				params: Vec::new(),
				with: ApplyWith::default(),
			},
		)
	}

	fn unit_actor(rows: Arc<AtomicUsize>) -> (TestEngine, FlowActor) {
		unit_actor_with("ring", unit_operators(rows), |engine| idle(engine, "unit-tick-supervisor"))
	}

	fn unit_actor_with(
		timed: &str,
		operators: CustomOperators,
		supervisor: impl FnOnce(&StandardEngine) -> ActorRef<FlowSupervisorMessage>,
	) -> (TestEngine, FlowActor) {
		let te = TestEngine::builder().with_commit_queue().build();
		let engine = te.inner().clone();

		te.admin("CREATE NAMESPACE app");
		te.admin("CREATE TABLE app::t { id: int4 }");
		te.admin("CREATE DEFERRED VIEW app::v { id: int4 } AS { FROM app::t MAP { id } }");
		te.admin("CREATE DEFERRED VIEW app::w { id: int4 } AS { FROM app::v MAP { id } }");
		let v = view_named(&engine, "v");
		let w = view_named(&engine, "w");

		let substrate = FlowSubstrate::with_dictionary(engine.dictionary_allocators(), engine.operator_state());
		arm_due_timer(&engine, &substrate);

		let mut head = FlowDag::builder(FLOW);
		head.add_node(FlowNode::new(
			SOURCE,
			OperatorDef::SourceView {
				view: v,
			},
		));
		head.add_node(apply_node(TIMED, timed));
		head.add_node(FlowNode::new(
			SINK,
			OperatorDef::SinkTableView {
				view: w,
			},
		));
		head.add_edge(FlowEdge::new(1, head.id(), SOURCE, TIMED)).expect("edge");
		head.add_edge(FlowEdge::new(2, head.id(), TIMED, SINK)).expect("edge");
		let head = head.build();

		let mut member = FlowDag::builder(MEMBER);
		member.add_node(FlowNode::new(
			MEMBER_SOURCE,
			OperatorDef::SourceView {
				view: w,
			},
		));
		member.add_node(apply_node(MEMBER_SEEN, "seen"));
		member.add_edge(FlowEdge::new(3, member.id(), MEMBER_SOURCE, MEMBER_SEEN)).expect("edge");
		let member = member.build();

		let committer = idle(&engine, "unit-tick-committer");
		let actor = FlowActor::new(FlowActorParams {
			engine: engine.clone(),
			supervisor: supervisor(&engine),
			committer: committer.clone(),
			terminal_committer: committer,
			queue: CommitQueue::new(ByteSize::from_mib(8), false, CommitVersion(0)),
			loader: idle(&engine, "unit-tick-loader"),
			control: ControlFrontier::new(),
			custom_operators: operators,
			substrate,
			operator_samples: OperatorSampleRegistry::new(),
			clock: engine.clock().clone(),
			health: FlowHealthRegistry::new(),
			flow_tracker: FlowPositionTracker::new(),
			flow: head,
			members: vec![(member, w)],
			source_objects: Arc::new(BTreeSet::new()),
			completeness_objects: None,
			cursor: CommitVersion(0),
			cursor_lsn: Lsn::new(0),
			pull_batch_bytes: ByteSize::from_mib(8),
			load_batch_bytes: ByteSize::from_mib(8),
			checkpoint_lag: 10_000,
			checkpoint_max_age: Duration::from_milliseconds(5_000).unwrap(),
			retry_limit: RETRY_LIMIT,
			retry_backoff: Duration::from_milliseconds(50).unwrap(),
			wake_coalesce: Duration::from_nanoseconds_const(FLOW_WAKE_COALESCE_NANOS),
			batch_size: 8,
		});

		(te, actor)
	}

	fn source_view(engine: &StandardEngine) -> ViewId {
		let mut query = engine.begin_query(IdentityId::system()).expect("query");
		let mut txn = Transaction::Query(&mut query);
		let namespace = engine
			.catalog()
			.find_namespace_by_name(&mut txn, "app")
			.expect("namespace")
			.expect("the test declares namespace app");
		engine.catalog()
			.find_view_by_name(&mut txn, namespace.id(), "v")
			.expect("view lookup")
			.expect("the test declares view v")
			.id()
	}

	fn arm_due_timer(engine: &StandardEngine, substrate: &FlowSubstrate) {
		let (version, lease) = engine.acquire_current_snapshot_lease().expect("lease");
		let query = engine
			.multi()
			.begin_query_at_version(engine.multi().generate_transaction_id(), &lease)
			.expect("query");
		let state_query = engine
			.multi()
			.begin_query_at_version(engine.multi().generate_transaction_id(), &lease)
			.expect("state query");
		let mut txn = DeferredTransaction::new(DeferredParams {
			ctx: system_ctx(),
			version,
			pending: Pending::new(),
			query: Some(query),
			state_query: Some(state_query),
			catalog: engine.catalog(),
			interceptors: engine.create_interceptors(),
			clock: engine.clock().clone(),
			substrate: substrate.clone(),
			lookup: None,
		});
		SourceWatermarks::advance(SOURCE, &mut txn, at_millis(WATERMARK_MS)).expect("advance watermark");
		TimerWheel::arm(
			TIMED,
			&mut txn,
			&Timer {
				due: at_millis(DUE_MS),
				kind: TimerKind::Seal,
				key: EncodedKey::new(b"bucket"),
			},
		)
		.expect("arm timer");
		let pending = txn.take_pending();
		persist(&engine.operator_state(), &pending);
	}

	fn ticking_actor() -> (TestEngine, FlowHealthRegistry, FlowActor) {
		actor_with_due_timer(true)
	}

	fn quiet_actor() -> (TestEngine, FlowHealthRegistry, FlowActor) {
		actor_with_due_timer(false)
	}

	fn actor_with_due_timer(armed: bool) -> (TestEngine, FlowHealthRegistry, FlowActor) {
		actor_with(armed, "boom", |engine| idle(engine, "tick-failures-committer"))
	}

	fn actor_with(
		armed: bool,
		operator: &str,
		committer: impl FnOnce(&StandardEngine) -> ActorRef<CommitterMessage>,
	) -> (TestEngine, FlowHealthRegistry, FlowActor) {
		let te = TestEngine::builder().with_commit_queue().build();
		let engine = te.inner().clone();

		te.admin("CREATE NAMESPACE app");
		te.admin("CREATE TABLE app::t { id: int4 }");
		te.admin("CREATE DEFERRED VIEW app::v { id: int4 } AS { FROM app::t MAP { id } }");

		let substrate = FlowSubstrate::with_dictionary(engine.dictionary_allocators(), engine.operator_state());
		if armed {
			arm_due_timer(&engine, &substrate);
		}

		let mut builder = FlowDag::builder(FLOW);
		builder.add_node(FlowNode::new(
			SOURCE,
			OperatorDef::SourceView {
				view: source_view(&engine),
			},
		));
		builder.add_node(FlowNode::new(
			TIMED,
			OperatorDef::Apply {
				operator: operator.to_string(),
				params: Vec::new(),
				with: ApplyWith::default(),
			},
		));
		builder.add_edge(FlowEdge::new(1, builder.id(), SOURCE, TIMED)).expect("edge");
		let flow = builder.build();
		assert!(flow.ticks(), "precondition: the flow must tick, otherwise on_tick returns without working");

		let health = FlowHealthRegistry::new();
		let committer_ref = committer(&engine);
		let actor = FlowActor::new(FlowActorParams {
			engine: engine.clone(),
			supervisor: idle(&engine, "tick-failures-supervisor"),
			committer: committer_ref.clone(),
			terminal_committer: committer_ref,
			queue: CommitQueue::new(ByteSize::from_mib(8), false, CommitVersion(0)),
			loader: idle(&engine, "tick-failures-loader"),
			control: ControlFrontier::new(),
			custom_operators: timer_operators(),
			substrate,
			operator_samples: OperatorSampleRegistry::new(),
			clock: engine.clock().clone(),
			health: health.clone(),
			flow_tracker: FlowPositionTracker::new(),
			flow,
			members: Vec::new(),
			source_objects: Arc::new(BTreeSet::new()),
			completeness_objects: None,
			cursor: CommitVersion(0),
			cursor_lsn: Lsn::new(0),
			pull_batch_bytes: ByteSize::from_mib(8),
			load_batch_bytes: ByteSize::from_mib(8),
			checkpoint_lag: 10_000,
			checkpoint_max_age: Duration::from_milliseconds(5_000).unwrap(),
			retry_limit: RETRY_LIMIT,
			retry_backoff: Duration::from_milliseconds(50).unwrap(),
			wake_coalesce: Duration::from_nanoseconds_const(FLOW_WAKE_COALESCE_NANOS),
			batch_size: 8,
		});

		(te, health, actor)
	}

	fn tick(harness: &mut TestHarness<FlowActor>) {
		harness.send(FlowActorMessage::Tick);
		assert_eq!(harness.process_one(), Some(Directive::Continue), "the tick must be handled");
	}

	#[test]
	fn a_failed_tick_counts_a_retry_instead_of_being_logged_away() {
		let (_te, _health, actor) = ticking_actor();
		let mut harness = TestHarness::new(actor);
		assert!(!harness.state().poisoned, "precondition: registration must succeed");

		tick(&mut harness);

		assert_eq!(harness.state().retry_count, 1);
		assert!(!harness.state().poisoned, "one failure is under the limit");
	}

	#[test]
	fn ticks_that_keep_failing_poison_the_flow_once_past_the_retry_limit() {
		let (_te, _health, actor) = ticking_actor();
		let mut harness = TestHarness::new(actor);
		assert!(!harness.state().poisoned, "precondition: registration must succeed");

		for attempt in 1..=RETRY_LIMIT {
			tick(&mut harness);
			assert_eq!(harness.state().retry_count, attempt);
			assert!(!harness.state().poisoned, "attempt {attempt} is still within the limit");
		}

		tick(&mut harness);

		assert!(harness.state().poisoned);
	}

	#[test]
	fn a_unit_past_the_retry_limit_reports_every_member_and_the_reason_to_the_supervisor() {
		// A member missing from the report stays active in the catalog and respawns at restart on its dead
		// head.
		let (poisoned, reports) = mpsc::channel();
		let (_te, actor) = unit_actor_with("boom", failing_unit_operators(), |engine| {
			engine.spawner()
				.spawn_ephemeral(
					"unit-poison-recorder",
					PoisonRecorder {
						poisoned,
					},
				)
				.actor_ref()
				.clone()
		});
		let mut harness = TestHarness::new(actor);
		assert!(!harness.state().poisoned, "precondition: registration must succeed");

		for _ in 0..=RETRY_LIMIT {
			tick(&mut harness);
		}
		assert!(harness.state().poisoned, "precondition: the unit must poison past the retry limit");

		let (flows, reason) = reports
			.recv_timeout(Duration::from_seconds(10).unwrap().to_std())
			.expect("a poisoned unit must report to the supervisor");
		assert_eq!(flows, vec![FLOW, MEMBER], "the head and every member must be reported in one message");
		assert!(!reason.is_empty(), "the report must carry the poison reason");
		assert!(reports.try_recv().is_err(), "one poison must be reported once");
	}

	#[test]
	fn an_idle_drain_does_not_clear_a_strike() {
		let (_te, _health, actor) = ticking_actor();
		let mut harness = TestHarness::new(actor);

		tick(&mut harness);
		assert_eq!(harness.state().retry_count, 1, "precondition: the failed tick must count a strike");

		harness.send(FlowActorMessage::Drain);
		assert_eq!(harness.process_one(), Some(Directive::Continue), "the drain must be handled");

		assert_eq!(
			harness.state().retry_count,
			1,
			"an idle drain advanced no cursor and committed nothing, so it is no evidence the flow recovered"
		);
	}

	#[test]
	fn a_failed_tick_commit_counts_a_retry_instead_of_being_cleared() {
		let (_te, _health, actor) = quiet_actor();
		let mut harness = TestHarness::new(actor);
		harness.state_mut().committing = true;

		harness.send(FlowActorMessage::TickCommitted {
			result: Err(internal_error!("commit rejected")),
		});
		assert_eq!(harness.process_one(), Some(Directive::Continue), "the reply must be handled");

		assert!(!harness.state().committing, "the latch must clear, otherwise the flow never drains again");
		assert_eq!(
			harness.state().retry_count,
			1,
			"a discarded tick commit must count a strike, or repeated ones never rebuild and never poison"
		);
	}

	#[test]
	fn a_tick_commit_that_lands_clears_the_strike() {
		let (_te, _health, actor) = quiet_actor();
		let mut harness = TestHarness::new(actor);
		harness.state_mut().committing = true;
		harness.state_mut().retry_count = RETRY_LIMIT;

		harness.send(FlowActorMessage::TickCommitted {
			result: Ok(()),
		});
		assert_eq!(harness.process_one(), Some(Directive::Continue), "the reply must be handled");

		assert_eq!(harness.state().retry_count, 0, "a landed commit is progress");
		assert!(!harness.state().poisoned, "a flow that just committed must not be one strike from poison");
	}

	#[test]
	fn a_tick_commit_that_lands_counts_a_completed_tick() {
		// A flow that emits on every tick only completes ticks here, otherwise a wait for its next tick never
		// ends.
		let (_te, _health, actor) = quiet_actor();
		let tracker = actor.flow_tracker.clone();
		let mut harness = TestHarness::new(actor);
		harness.state_mut().committing = true;

		harness.send(FlowActorMessage::TickCommitted {
			result: Ok(()),
		});
		assert_eq!(harness.process_one(), Some(Directive::Continue), "the reply must be handled");

		assert_eq!(tracker.ticks(), vec![(FLOW, 1)], "a landed tick commit must count as one completed tick");
	}

	#[test]
	fn a_checkpoint_request_saves_a_fresh_cursor_and_replies_once_it_lands() {
		// A lagging member that waits out the checkpoint age before saving stalls its unit join past any test
		// wait.
		let (_te, _health, actor) = quiet_actor();
		let now = actor.clock.now();
		let mut harness = TestHarness::new(actor);
		harness.state_mut().backfilling = false;
		harness.state_mut().cursor = CommitVersion(7);
		harness.state_mut().durable_cursor = CommitVersion(3);
		harness.state_mut().last_checkpoint_at = now;
		let replied = Arc::new(AtomicBool::new(false));
		let flag = replied.clone();

		harness.send(FlowActorMessage::CheckpointNow {
			reply: Box::new(move || flag.store(true, Ordering::SeqCst)),
		});
		assert_eq!(harness.process_one(), Some(Directive::Continue), "the request must be handled");
		assert!(
			harness.state().committing,
			"a cursor ahead of its checkpoint must save now, not after the age"
		);
		assert!(
			!replied.load(Ordering::SeqCst),
			"the reply must wait for the commit, otherwise the join reads a stale checkpoint"
		);

		harness.send(FlowActorMessage::SliceCommitted {
			advance_to: CommitVersion(7),
			more: false,
			result: Ok(()),
			committed: Some(CommitVersion(8)),
		});
		harness.process_one();
		assert_eq!(
			harness.state().durable_cursor,
			CommitVersion(7),
			"the landed save must be the requested cursor"
		);
		assert!(
			replied.load(Ordering::SeqCst),
			"a landed save must reply, otherwise the join waits for the stall tick"
		);
	}

	#[test]
	fn a_timer_fired_on_a_tick_reaches_the_fused_member() {
		// Without the member feed on the tick path, a fused child never sees rows its head emits from a timer.
		let rows = Arc::new(AtomicUsize::new(0));
		let (_te, actor) = unit_actor(rows.clone());
		let mut harness = TestHarness::new(actor);
		assert!(!harness.state().poisoned, "precondition: both members must register");

		tick(&mut harness);

		assert!(!harness.state().poisoned, "the unit tick must not fail");
		assert_eq!(
			rows.load(Ordering::SeqCst),
			1,
			"the member must see exactly the one row the head's timer emitted"
		);
	}

	#[test]
	fn a_commit_conflict_from_a_concurrent_view_drop_neither_panics_nor_wedges_the_flow() {
		// The slice loses to the drop, so a panic here kills the actor and a wedge stalls the supervisor's
		// stop.
		let (te, _health, actor) = quiet_actor();
		let mut harness = TestHarness::new(actor);
		harness.state_mut().backfilling = false;
		harness.state_mut().committing = true;
		te.admin("DROP VIEW app::v");

		harness.send(FlowActorMessage::SliceCommitted {
			advance_to: CommitVersion(7),
			more: false,
			result: Err(internal_error!("the drop won the conflict")),
			committed: None,
		});
		assert_eq!(
			harness.process_one(),
			Some(Directive::Continue),
			"a lost commit must not stop the actor alone"
		);
		assert!(harness.state().poisoned, "a flow whose view is gone cannot rebuild, so it must stop working");
		assert!(!harness.state().committing, "a lost commit must release the stop it could be holding");

		let replied = Arc::new(AtomicBool::new(false));
		let flag = replied.clone();
		harness.send(FlowActorMessage::Stop {
			delete_checkpoint: false,
			reply: Box::new(move || flag.store(true, Ordering::SeqCst)),
		});
		assert_eq!(harness.process_one(), Some(Directive::Stop), "the supervisor's stop must still land");
		assert!(replied.load(Ordering::SeqCst), "the supervisor waits on this reply");
	}

	#[test]
	fn a_stop_during_a_commit_waits_for_the_commit_to_land() {
		// Without the wait, a respawn starts while the old commit is in flight and applies its version twice.
		let (_te, _health, actor) = quiet_actor();
		let mut harness = TestHarness::new(actor);
		harness.state_mut().committing = true;
		let replied = Arc::new(AtomicBool::new(false));
		let flag = replied.clone();

		harness.send(FlowActorMessage::Stop {
			delete_checkpoint: false,
			reply: Box::new(move || flag.store(true, Ordering::SeqCst)),
		});
		assert_eq!(harness.process_one(), Some(Directive::Continue), "a stop mid-commit must be held");
		assert!(!replied.load(Ordering::SeqCst), "the reply must not fire before the in-flight commit lands");

		harness.send(FlowActorMessage::TickCommitted {
			result: Ok(()),
		});
		assert_eq!(
			harness.process_one(),
			Some(Directive::Stop),
			"the held stop must run once the commit lands"
		);
		assert!(
			replied.load(Ordering::SeqCst),
			"the supervisor waits on this reply and never respawns without it"
		);
	}

	fn closed_committer(engine: &StandardEngine) -> ActorRef<CommitterMessage> {
		let scope = engine.spawner().scope();
		let handle = scope.spawn_ephemeral(
			"tick-closed-committer",
			Idle::<CommitterMessage> {
				_marker: PhantomData,
			},
		);
		let committer = handle.actor_ref().clone();
		scope.shutdown();
		handle.join().expect("the closed committer must stop");
		committer
	}

	#[test]
	fn a_closed_committer_in_shutdown_stops_the_flow_without_poison() {
		// A closed channel during shutdown says nothing about the view, so it must never poison it.
		let (_te, _health, actor) = actor_with(true, "silent", closed_committer);
		let mut harness = TestHarness::new(actor);
		assert!(!harness.state().poisoned, "precondition: registration must succeed");
		harness.cancel();

		tick(&mut harness);

		assert!(!harness.state().poisoned, "a shutdown must leave the flow active for the next boot");
		assert!(!harness.state().committing, "the dropped tick commit must not leave the flow waiting on it");
	}

	#[test]
	fn a_closed_committer_outside_shutdown_panics_naming_the_worker() {
		// A committer gone while the system runs is a crash, and poisoning the view instead would hide it.
		let (_te, _health, actor) = actor_with(true, "silent", closed_committer);
		let mut harness = TestHarness::new(actor);
		assert!(!harness.state().poisoned, "precondition: registration must succeed");

		let outcome = catch_unwind(AssertUnwindSafe(|| tick(&mut harness)));

		let payload = outcome.expect_err("a closed committer outside shutdown must panic");
		let message = payload
			.downcast_ref::<String>()
			.cloned()
			.or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string()))
			.unwrap_or_default();
		assert!(
			message.contains("committer stopped outside shutdown"),
			"the panic must name the worker: {message}"
		);
	}

	#[test]
	fn a_tick_that_completes_clears_the_strike() {
		let (_te, _health, actor) = quiet_actor();
		let mut harness = TestHarness::new(actor);
		harness.state_mut().retry_count = RETRY_LIMIT;

		tick(&mut harness);

		assert_eq!(harness.state().retry_count, 0, "a tick that dispatched its timers is evidence of recovery");
		assert!(!harness.state().poisoned, "a recovered flow must not be one strike from poison");
	}

	#[test]
	fn a_tick_commit_is_stamped_one_past_the_cursor_it_ticked_from() {
		let (sent, sources) = mpsc::channel();
		let (_te, _health, actor) = actor_with(true, "silent", |engine| recorder(engine, sent));
		let mut harness = TestHarness::new(actor);
		assert!(!harness.state().poisoned, "precondition: registration must succeed");
		harness.state_mut().cursor = CommitVersion(7);

		tick(&mut harness);

		assert_eq!(
			harness.state().retry_count,
			0,
			"precondition: the timer must fire cleanly, otherwise no tick commit is sent"
		);
		assert_eq!(
			sources.recv_timeout(Duration::from_seconds(5).unwrap().to_std())
				.expect("a fired timer must send a tick commit"),
			SourceVersion(8),
			"a reader level with this flow skips view rows at or below cursor 7, so a tick stamped at the cursor \
			 is never read"
		);
	}

	#[test]
	fn a_tick_that_dispatches_a_commit_drops_the_cached_hold_check() {
		// The tick commit writes flow state, so a hold check cached before it would report stale holds.
		let (sent, sources) = mpsc::channel();
		let (_te, _health, actor) = actor_with(true, "silent", |engine| recorder(engine, sent));
		let mut harness = TestHarness::new(actor);
		assert!(!harness.state().poisoned, "precondition: registration must succeed");
		harness.state_mut().hold_cache = Some(HoldCache {
			frontiers: Vec::new(),
			holds: WatermarkHolds::new(),
		});

		tick(&mut harness);

		sources.recv_timeout(Duration::from_seconds(5).unwrap().to_std())
			.expect("precondition: a fired timer must send a tick commit");
		assert!(harness.state().hold_cache.is_none(), "a tick commit left a stale hold check cached");
	}

	#[test]
	fn a_tick_while_a_commit_is_in_flight_computes_nothing() {
		let (_te, _health, actor) = ticking_actor();
		let mut harness = TestHarness::new(actor);
		harness.state_mut().committing = true;

		tick(&mut harness);

		assert_eq!(
			harness.state().retry_count,
			0,
			"the latch must keep the tick from computing; a strike proves a compute ran beside an \
			 in-flight commit"
		);

		harness.state_mut().committing = false;
		tick(&mut harness);

		assert_eq!(
			harness.state().retry_count,
			1,
			"the same tick must compute once the latch clears, or the assertion above passed only \
			 because there was nothing to compute"
		);
	}

	#[test]
	fn a_wake_while_a_commit_is_in_flight_defers_the_drain_instead_of_starting_one() {
		let (_te, _health, actor) = quiet_actor();
		let mut harness = TestHarness::new(actor);
		harness.state_mut().committing = true;

		harness.send(FlowActorMessage::Wake);
		assert_eq!(harness.process_one(), Some(Directive::Continue), "the wake must be handled");

		assert!(
			harness.state().drain_after_commit,
			"a wake during a commit must be deferred, not dropped, or the flow stalls until the next \
			 timer"
		);

		harness.state_mut().committing = false;
		harness.state_mut().drain_after_commit = false;
		harness.send(FlowActorMessage::Wake);
		assert_eq!(harness.process_one(), Some(Directive::Continue), "the wake must be handled");

		assert!(
			!harness.state().drain_after_commit,
			"an unfenced wake drains at once and has nothing to defer, or the assertion above says \
			 nothing about the latch"
		);
	}

	struct WakeRecorder {
		received: mpsc::Sender<&'static str>,
	}

	impl Actor for WakeRecorder {
		type State = ();
		type Message = FlowActorMessage;

		fn init(&self, _ctx: &Context<Self::Message>) {}

		fn handle(&self, _state: &mut (), msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
			let kind = match msg {
				FlowActorMessage::Wake => "wake",
				FlowActorMessage::Sample => "sample",
				FlowActorMessage::Tick => "tick",
				_ => "other",
			};
			self.received.send(kind).expect("the test still listens for flow messages");
			Directive::Continue
		}

		fn config(&self) -> ActorConfig {
			ActorConfig::new()
		}
	}

	fn wake_recorder(engine: &StandardEngine, received: mpsc::Sender<&'static str>) -> ActorRef<FlowActorMessage> {
		engine.spawner()
			.spawn_ephemeral(
				"wake-recorder",
				WakeRecorder {
					received,
				},
			)
			.actor_ref()
			.clone()
	}

	fn next_message(received: &mpsc::Receiver<&'static str>) -> &'static str {
		received.recv_timeout(Duration::from_seconds(10).unwrap().to_std())
			.expect("the recorder must receive the next message")
	}

	#[test]
	fn wakes_sent_before_the_flow_drains_merge_into_one_message() {
		let (te, _health, actor) = quiet_actor();
		let (sender, received) = mpsc::channel();
		let recorder = wake_recorder(te.inner(), sender);
		let clock = Clock::Mock(MockClock::from_millis(0));
		let waker = FlowWaker::new(recorder.clone(), actor.wake_pending(), clock.clone());

		waker.wake();
		waker.wake();
		assert!(recorder.send(FlowActorMessage::Sample).is_ok(), "send marker");
		assert_eq!(next_message(&received), "wake", "the first wake must be delivered");
		assert_eq!(
			next_message(&received),
			"sample",
			"a second wake before the drain must be merged into the first"
		);

		let mut harness = TestHarness::new(actor);
		harness.send(FlowActorMessage::Drain);
		assert_eq!(harness.process_one(), Some(Directive::Continue), "the drain must be handled");

		clock.as_mock().expect("the harness drives a mock clock").advance_millis(21);
		waker.wake();
		assert!(recorder.send(FlowActorMessage::Tick).is_ok(), "send marker");
		assert_eq!(
			next_message(&received),
			"wake",
			"a wake after the drain started must be delivered, or the flow misses data that landed after it read"
		);
		assert_eq!(next_message(&received), "tick", "exactly one wake must follow the drain");
	}

	#[test]
	fn a_wake_merged_while_a_commit_is_in_flight_drains_after_the_commit() {
		let (_te, _health, actor) = quiet_actor();
		let pending = actor.wake_pending();
		let mut harness = TestHarness::new(actor);
		harness.state_mut().committing = true;

		harness.send(FlowActorMessage::Drain);
		assert_eq!(harness.process_one(), Some(Directive::Continue), "the drain must be handled");
		assert!(
			!harness.state().drain_after_commit,
			"a busy drain with no pending wake has nothing to defer, or the assertion below says nothing"
		);

		pending.store(true, Ordering::SeqCst);
		harness.send(FlowActorMessage::Drain);
		assert_eq!(harness.process_one(), Some(Directive::Continue), "the drain must be handled");
		assert!(
			harness.state().drain_after_commit,
			"a pending wake consumed during a commit must drain after it, or the flow stalls until the next wake"
		);
		assert!(
			!pending.load(Ordering::SeqCst),
			"the drain must clear the pending wake, or every later wake is merged away and the flow never wakes"
		);
	}

	#[test]
	fn a_tick_with_nothing_due_dispatches_no_commit() {
		// otherwise every idle flow costs a commit and a reader wake on every interval
		let (_te, _health, actor) = actor_with(false, "silent", |engine| idle(engine, "idle-tick-committer"));
		let mut harness = TestHarness::new(actor);
		assert!(!harness.state().poisoned, "precondition: registration must succeed");

		tick(&mut harness);

		assert_eq!(harness.state().retry_count, 0, "precondition: the tick must compute cleanly");
		assert!(!harness.state().committing, "a tick that fired nothing and wrote nothing dispatched a commit");
	}
}
