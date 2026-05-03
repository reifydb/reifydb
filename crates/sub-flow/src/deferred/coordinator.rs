// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{cmp::min, collections, collections::BTreeMap, fmt, mem, ops::Bound, sync::Arc, time::Duration};

use reifydb_cdc::{
	consume::{checkpoint::CdcCheckpoint, consumer::CdcConsume},
	storage::CdcStore,
};
use reifydb_core::{
	actors::{
		flow::{FlowInstruction, FlowPoolMessage, PoolResponse, WorkerBatch},
		pending::{Pending, PendingWrite},
	},
	common::CommitVersion,
	encoded::shape::RowShape,
	interface::{
		catalog::{flow::FlowId, id::ViewId, shape::ShapeId},
		cdc::{Cdc, CdcBatch, SystemChange},
		change::{Change, ChangeOrigin},
	},
	internal,
	key::{Key, kind::KeyKind},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_rql::flow::{
	analyzer::{FlowDependencyGraph, FlowGraphAnalyzer},
	flow::FlowDag,
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
use reifydb_transaction::transaction::{Transaction, command::CommandTransaction};
use reifydb_type::{
	Result,
	error::Error,
	value::{datetime::DateTime, identity::IdentityId},
};
use tracing::{Span, debug, field, info, instrument, warn};

use super::{state::FlowStates, tracker::ShapeVersionTracker};
use crate::catalog::FlowCatalog;

pub(crate) struct FlowConsumeRef {
	pub actor_ref: ActorRef<FlowCoordinatorMessage>,
}

impl CdcConsume for FlowConsumeRef {
	fn consume(&self, cdcs: Vec<Cdc>, reply: Box<dyn FnOnce(Result<()>) + Send>) {
		let current_version = cdcs.last().map(|c| c.version).unwrap_or(CommitVersion(0));

		let result = self.actor_ref.send(FlowCoordinatorMessage::Consume {
			cdcs,
			current_version,
			reply,
		});

		if let Err(send_err) = result
			&& let FlowCoordinatorMessage::Consume {
				reply,
				..
			} = send_err.into_inner()
		{
			reply(Err(Error(Box::new(internal!("Coordinator actor stopped")))));
		}
	}
}

use reifydb_core::actors::flow::FlowCoordinatorMessage;

#[inline]
fn record_version_range_span(cdcs: &[Cdc]) {
	if let Some(first) = cdcs.first() {
		Span::current().record("version_start", first.version.0);
	}
	if let Some(last) = cdcs.last() {
		Span::current().record("version_end", last.version.0);
	}
}

struct ConsumeContext {
	state_version: CommitVersion,
	current_version: CommitVersion,
	combined: Pending,
	pending_shapes: Vec<RowShape>,
	checkpoints: Vec<(FlowId, CommitVersion)>,
	original_reply: Box<dyn FnOnce(Result<()>) + Send>,
	consume_start: Instant,

	all_changes: Vec<Change>,

	latest_version: Option<CommitVersion>,

	downstream_flows: collections::HashSet<FlowId>,

	view_changes: Vec<Change>,
}

impl ConsumeContext {
	fn is_empty(&self) -> bool {
		self.combined.iter_sorted().next().is_none()
			&& self.view_changes.is_empty()
			&& self.checkpoints.is_empty()
			&& self.pending_shapes.is_empty()
	}
}

enum Phase {
	Idle,

	RegisteringFlows {
		flows: Vec<FlowDag>,
		ctx: ConsumeContext,
	},

	SubmittingBatches {
		ctx: ConsumeContext,
	},

	AdvancingBackfill {
		flows: Vec<FlowId>,
		ctx: ConsumeContext,
	},

	Rebalancing {
		ctx: ConsumeContext,
	},

	Ticking,
}

struct TickSchedule {
	tick: Duration,
	last_tick: Instant,
}

fn coordinator_error(msg: impl fmt::Display) -> Result<()> {
	Err(Error(Box::new(internal!("{}", msg))))
}

#[inline]
fn collect_chunk_changes(batch: &CdcBatch) -> Vec<Change> {
	let mut chunk_changes = Vec::new();
	for cdc in &batch.items {
		chunk_changes.extend(cdc.changes.iter().cloned());
	}
	chunk_changes
}

#[inline]
fn apply_pending_writes(transaction: &mut CommandTransaction, combined: &Pending) -> Result<()> {
	for (key, pw) in combined.iter_sorted() {
		match pw {
			PendingWrite::Set(value) => transaction.set(key, value.clone())?,
			PendingWrite::Remove => {
				if matches!(Key::kind(key), Some(KeyKind::Row)) {
					match transaction.get(key)? {
						Some(existing) => transaction.unset(key, existing.row)?,
						None => transaction.remove(key)?,
					}
				} else {
					transaction.remove(key)?;
				}
			}
		}
	}
	Ok(())
}

#[inline]
fn persist_flow_checkpoints(
	transaction: &mut CommandTransaction,
	checkpoints: &[(FlowId, CommitVersion)],
) -> Result<()> {
	for (flow_id, version) in checkpoints {
		CdcCheckpoint::persist(transaction, flow_id, *version)?;
	}
	Ok(())
}

#[inline]
fn collect_direct_flow_sources(
	dependency_graph: &FlowDependencyGraph,
	flow_id: FlowId,
) -> (collections::HashSet<ShapeId>, Vec<ViewId>) {
	let mut flow_sources: collections::HashSet<ShapeId> = collections::HashSet::new();
	let mut view_sources = Vec::new();

	for (table_id, flow_ids) in &dependency_graph.source_tables {
		if flow_ids.contains(&flow_id) {
			flow_sources.insert(ShapeId::Table(*table_id));
		}
	}
	for (view_id, flow_ids) in &dependency_graph.source_views {
		if flow_ids.contains(&flow_id) {
			flow_sources.insert(ShapeId::View(*view_id));
			view_sources.push(*view_id);
		}
	}
	for (rb_id, flow_ids) in &dependency_graph.source_ringbuffers {
		if flow_ids.contains(&flow_id) {
			flow_sources.insert(ShapeId::RingBuffer(*rb_id));
		}
	}
	for (series_id, flow_ids) in &dependency_graph.source_series {
		if flow_ids.contains(&flow_id) {
			flow_sources.insert(ShapeId::Series(*series_id));
		}
	}

	(flow_sources, view_sources)
}

#[inline]
fn filter_changes_by_sources(changes: &[Change], flow_sources: &collections::HashSet<ShapeId>) -> Vec<Change> {
	changes.iter()
		.filter(|change| {
			if let ChangeOrigin::Shape(source) = change.origin {
				flow_sources.contains(&source)
			} else {
				true
			}
		})
		.cloned()
		.collect()
}

pub struct CoordinatorActor {
	engine: StandardEngine,
	catalog: FlowCatalog,
	pool: ActorRef<FlowPoolMessage>,
	tracker: Arc<ShapeVersionTracker>,
	cdc_store: CdcStore,
	num_workers: usize,
	clock: Clock,
}

impl CoordinatorActor {
	pub fn new(
		engine: StandardEngine,
		catalog: FlowCatalog,
		pool_ref: ActorRef<FlowPoolMessage>,
		tracker: Arc<ShapeVersionTracker>,
		cdc_store: CdcStore,
		num_workers: usize,
		clock: Clock,
	) -> Self {
		Self {
			engine,
			catalog,
			pool: pool_ref,
			tracker,
			cdc_store,
			num_workers,
			clock,
		}
	}
}

pub struct CoordinatorState {
	states: FlowStates,
	analyzer: FlowGraphAnalyzer,
	phase: Phase,
	tick_schedules: BTreeMap<FlowId, TickSchedule>,

	flow_assignments: BTreeMap<FlowId, usize>,
}

impl Actor for CoordinatorActor {
	type State = CoordinatorState;
	type Message = FlowCoordinatorMessage;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		ctx.schedule_once(Duration::from_secs(1), || FlowCoordinatorMessage::Tick);

		CoordinatorState {
			states: FlowStates::new(),
			analyzer: FlowGraphAnalyzer::new(),
			phase: Phase::Idle,
			tick_schedules: BTreeMap::new(),
			flow_assignments: BTreeMap::new(),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			FlowCoordinatorMessage::Consume {
				cdcs,
				current_version,
				reply,
			} => {
				self.handle_consume(state, ctx, cdcs, current_version, reply);
			}
			FlowCoordinatorMessage::PoolReply(response) => {
				self.handle_pool_reply(state, ctx, response);
			}
			FlowCoordinatorMessage::Tick => {
				if matches!(state.phase, Phase::Idle) {
					self.handle_tick(state, ctx);
				}
				ctx.schedule_once(Duration::from_secs(1), || FlowCoordinatorMessage::Tick);
			}
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}

impl CoordinatorActor {
	#[instrument(name = "flow::coordinator::consume", level = "debug", skip(self, state, ctx, cdcs, reply), fields(
		cdc_count = cdcs.len(),
		version_start = field::Empty,
		version_end = field::Empty,
		batch_count = field::Empty,
		elapsed_us = field::Empty
	))]
	fn handle_consume(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		cdcs: Vec<Cdc>,
		current_version: CommitVersion,
		reply: Box<dyn FnOnce(Result<()>) + Send>,
	) {
		if !matches!(state.phase, Phase::Idle) {
			(reply)(coordinator_error("Coordinator busy"));
			return;
		}
		record_version_range_span(&cdcs);

		let consume_start = self.clock.instant();
		let latest_version = cdcs.last().map(|c| c.version);

		let state_version = match self.engine.begin_query(IdentityId::system()) {
			Ok(q) => q.version(),
			Err(e) => {
				(reply)(coordinator_error(e));
				return;
			}
		};

		let all_changes = self.update_tracker_and_collect(&cdcs);
		let consume_ctx = ConsumeContext {
			state_version,
			current_version,
			combined: Pending::new(),
			pending_shapes: Vec::new(),
			checkpoints: Vec::new(),
			original_reply: reply,
			consume_start,
			all_changes,
			latest_version,
			downstream_flows: collections::HashSet::new(),
			view_changes: Vec::new(),
		};

		let new_flows = match self.discover_and_load_new_flows(state, &cdcs) {
			Ok(flows) => flows,
			Err(e) => {
				(consume_ctx.original_reply)(coordinator_error(e));
				return;
			}
		};

		self.start_registration_or_submit(state, ctx, consume_ctx, new_flows);
	}

	#[inline]
	fn update_tracker_and_collect(&self, cdcs: &[Cdc]) -> Vec<Change> {
		let mut all_changes = Vec::new();
		for cdc in cdcs {
			let version = cdc.version;
			for change in &cdc.changes {
				if let ChangeOrigin::Shape(source) = &change.origin {
					self.tracker.update(*source, version);
				}
			}
			all_changes.extend(cdc.changes.iter().cloned());
		}
		all_changes
	}

	fn discover_and_load_new_flows(&self, state: &mut CoordinatorState, cdcs: &[Cdc]) -> Result<Vec<FlowDag>> {
		let new_flow_ids = extract_new_flow_ids(cdcs);
		let mut new_flows = Vec::new();
		if new_flow_ids.is_empty() {
			return Ok(new_flows);
		}
		let mut query = self.engine.begin_query(IdentityId::system())?;
		for flow_id in new_flow_ids {
			match self.catalog.get_or_load_flow(&mut Transaction::Query(&mut query), flow_id) {
				Ok((flow, is_new)) => {
					if is_new {
						new_flows.push(flow);
					} else {
						state.analyzer.add(flow);

						self.catalog.remove(flow_id);
					}
				}
				Err(e) => {
					warn!(
						flow_id = flow_id.0,
						error = %e,
						"failed to load flow in coordinator, skipping"
					);
					continue;
				}
			}
		}
		Ok(new_flows)
	}

	fn start_registration_or_submit(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		consume_ctx: ConsumeContext,
		mut new_flows: Vec<FlowDag>,
	) {
		if new_flows.is_empty() {
			self.proceed_to_submit(state, ctx, consume_ctx);
			return;
		}
		let flow = new_flows.remove(0);
		let flow_id = flow.id;

		state.analyzer.add(flow.clone());
		self.maybe_register_tick_schedule(state, &flow);
		if flow.is_subscription() {
			state.states.register_active(flow_id, consume_ctx.current_version);
			debug!(flow_id = flow_id.0, "registered new subscription flow as active");
		} else {
			state.states.register_backfilling(flow_id);
			debug!(flow_id = flow_id.0, "registered new flow in backfilling status");
		}

		let self_ref = ctx.self_ref().clone();
		let callback: Box<dyn FnOnce(PoolResponse) + Send> = Box::new(move |resp| {
			let _ = self_ref.send(FlowCoordinatorMessage::PoolReply(resp));
		});
		if self.pool
			.send(FlowPoolMessage::RegisterFlow {
				flow_id,
				reply: callback,
			})
			.is_err()
		{
			(consume_ctx.original_reply)(coordinator_error("Pool actor stopped"));
			return;
		}
		state.phase = Phase::RegisteringFlows {
			flows: new_flows,
			ctx: consume_ctx,
		};
	}

	fn handle_pool_reply(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		response: PoolResponse,
	) {
		let phase = mem::replace(&mut state.phase, Phase::Idle);
		match phase {
			Phase::RegisteringFlows {
				flows,
				ctx: cctx,
			} => self.continue_registering(state, ctx, response, flows, cctx),
			Phase::Rebalancing {
				ctx: cctx,
			} => self.continue_rebalancing(state, ctx, response, cctx),
			Phase::SubmittingBatches {
				ctx: cctx,
			} => self.continue_submitting(state, ctx, response, cctx),
			Phase::AdvancingBackfill {
				flows,
				ctx: cctx,
			} => self.continue_advancing_backfill(state, ctx, response, flows, cctx),
			Phase::Ticking => self.continue_ticking(response),
			Phase::Idle => {}
		}
	}

	#[inline]
	fn continue_registering(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		response: PoolResponse,
		mut remaining_flows: Vec<FlowDag>,
		mut consume_ctx: ConsumeContext,
	) {
		match response {
			PoolResponse::RegisterSuccess => {}
			PoolResponse::Success {
				pending_shapes,
				..
			} => {
				consume_ctx.pending_shapes.extend(pending_shapes);
			}
			PoolResponse::Error(e) => {
				(consume_ctx.original_reply)(coordinator_error(e));
				return;
			}
		}

		if remaining_flows.is_empty() {
			self.rebalance_flows(state, ctx, consume_ctx);
			return;
		}

		let flow = remaining_flows.remove(0);
		let flow_id = flow.id;

		state.analyzer.add(flow.clone());
		self.maybe_register_tick_schedule(state, &flow);
		if flow.is_subscription() {
			state.states.register_active(flow_id, consume_ctx.current_version);
			debug!(flow_id = flow_id.0, "registered new subscription flow as active");
		} else {
			state.states.register_backfilling(flow_id);
			debug!(flow_id = flow_id.0, "registered new flow in backfilling status");
		}

		let self_ref = ctx.self_ref().clone();
		let callback: Box<dyn FnOnce(PoolResponse) + Send> = Box::new(move |resp| {
			let _ = self_ref.send(FlowCoordinatorMessage::PoolReply(resp));
		});

		if self.pool
			.send(FlowPoolMessage::RegisterFlow {
				flow_id,
				reply: callback,
			})
			.is_err()
		{
			(consume_ctx.original_reply)(coordinator_error("Pool actor stopped"));
			return;
		}

		state.phase = Phase::RegisteringFlows {
			flows: remaining_flows,
			ctx: consume_ctx,
		};
	}

	#[inline]
	fn continue_rebalancing(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		response: PoolResponse,
		consume_ctx: ConsumeContext,
	) {
		match response {
			PoolResponse::Success {
				..
			}
			| PoolResponse::RegisterSuccess => {
				self.proceed_to_submit(state, ctx, consume_ctx);
			}
			PoolResponse::Error(e) => {
				(consume_ctx.original_reply)(coordinator_error(e));
			}
		}
	}

	#[inline]
	fn continue_submitting(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		response: PoolResponse,
		mut consume_ctx: ConsumeContext,
	) {
		match response {
			PoolResponse::Success {
				pending,
				pending_shapes,
				view_changes,
			} => {
				consume_ctx.combined = pending;
				consume_ctx.pending_shapes.extend(pending_shapes);
				consume_ctx.view_changes.extend(view_changes);
			}
			PoolResponse::RegisterSuccess => {}
			PoolResponse::Error(e) => {
				(consume_ctx.original_reply)(coordinator_error(e));
				return;
			}
		}

		if let Some(to_version) = consume_ctx.latest_version {
			for flow_id in state.states.active_flow_ids() {
				consume_ctx.checkpoints.push((flow_id, to_version));
			}
		}

		self.proceed_to_backfill(state, ctx, consume_ctx);
	}

	#[inline]
	fn continue_advancing_backfill(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		response: PoolResponse,
		remaining_flow_ids: Vec<FlowId>,
		mut consume_ctx: ConsumeContext,
	) {
		match response {
			PoolResponse::Success {
				pending,
				pending_shapes,
				view_changes,
			} => {
				consume_ctx.pending_shapes.extend(pending_shapes);
				consume_ctx.view_changes.extend(view_changes);
				for (key, value) in pending.iter_sorted() {
					match value {
						PendingWrite::Set(v) => {
							consume_ctx.combined.insert(key.clone(), v.clone());
						}
						PendingWrite::Remove => {
							consume_ctx.combined.remove(key.clone());
						}
					}
				}
			}
			PoolResponse::RegisterSuccess => {}
			PoolResponse::Error(e) => {
				(consume_ctx.original_reply)(coordinator_error(e));
				return;
			}
		}

		self.advance_next_backfill_flow(state, ctx, remaining_flow_ids, consume_ctx);
	}

	#[inline]
	fn continue_ticking(&self, response: PoolResponse) {
		match response {
			PoolResponse::Success {
				pending,
				pending_shapes,
				..
			} => {
				self.commit_tick_writes(pending, pending_shapes);
			}
			PoolResponse::Error(e) => {
				warn!(error = %e, "tick processing failed");
			}
			_ => {}
		}
	}

	fn proceed_to_submit(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		mut consume_ctx: ConsumeContext,
	) {
		let optimal = self.compute_flow_assignments(state);
		if optimal != state.flow_assignments {
			self.rebalance_flows(state, ctx, consume_ctx);
			return;
		}

		if let Some(to_version) = consume_ctx.latest_version {
			let worker_batches = self.route_and_group_changes(
				state,
				&consume_ctx.all_changes,
				to_version,
				consume_ctx.state_version,
			);

			Span::current().record("batch_count", worker_batches.len());

			if !worker_batches.is_empty() {
				let self_ref = ctx.self_ref().clone();
				let callback: Box<dyn FnOnce(PoolResponse) + Send> = Box::new(move |resp| {
					let _ = self_ref.send(FlowCoordinatorMessage::PoolReply(resp));
				});

				if self.pool
					.send(FlowPoolMessage::Submit {
						batches: worker_batches,
						reply: callback,
					})
					.is_err()
				{
					(consume_ctx.original_reply)(coordinator_error("Pool actor stopped"));
					return;
				}

				state.phase = Phase::SubmittingBatches {
					ctx: consume_ctx,
				};
				return;
			}
		} else {
			Span::current().record("batch_count", 0usize);
		}

		if let Some(to_version) = consume_ctx.latest_version {
			for flow_id in state.states.active_flow_ids() {
				if !consume_ctx.downstream_flows.contains(&flow_id) {
					consume_ctx.checkpoints.push((flow_id, to_version));
				}
			}
		}

		self.proceed_to_backfill(state, ctx, consume_ctx);
	}

	fn proceed_to_backfill(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		mut consume_ctx: ConsumeContext,
	) {
		if consume_ctx.latest_version.is_none() {
			self.finish_consume(state, consume_ctx);
			return;
		}

		let backfilling_flows: Vec<_> = state.states.backfilling_flow_ids();

		let dependency_graph = state.analyzer.get_dependency_graph();
		for (view_id, producer_flow_id) in &dependency_graph.sink_views {
			if backfilling_flows.contains(producer_flow_id)
				&& let Some(consumer_flow_ids) = dependency_graph.source_views.get(view_id)
			{
				for fid in consumer_flow_ids {
					consume_ctx.downstream_flows.insert(*fid);
				}
			}
		}

		if backfilling_flows.is_empty() {
			self.finish_consume(state, consume_ctx);
			return;
		}

		self.advance_next_backfill_flow(state, ctx, backfilling_flows, consume_ctx);
	}

	fn advance_next_backfill_flow(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		mut flows: Vec<FlowId>,
		mut consume_ctx: ConsumeContext,
	) {
		const BACKFILL_CHUNK_SIZE: u64 = 1_000;

		while let Some(flow_id) = flows.first().copied() {
			flows.remove(0);

			if consume_ctx.downstream_flows.contains(&flow_id) {
				continue;
			}

			let from_version = match self.fetch_flow_checkpoint(flow_id) {
				Ok(v) => v,
				Err(e) => {
					(consume_ctx.original_reply)(coordinator_error(e));
					return;
				}
			};
			if from_version >= consume_ctx.current_version {
				self.mark_already_caught_up(state, flow_id, consume_ctx.current_version);
				continue;
			}

			let to_version =
				CommitVersion(min(from_version.0 + BACKFILL_CHUNK_SIZE, consume_ctx.current_version.0));
			let batch = self.read_backfill_chunk(from_version, to_version, BACKFILL_CHUNK_SIZE);

			if batch.items.is_empty() {
				self.record_chunk_checkpoint(
					state,
					&mut consume_ctx,
					flow_id,
					to_version,
					"backfill complete after empty chunk, flow now active",
				);
				continue;
			}

			let chunk_changes = collect_chunk_changes(&batch);
			let flow_changes = self.filter_cdc_for_flow(state, flow_id, &chunk_changes);

			if flow_changes.is_empty() {
				self.record_chunk_checkpoint(
					state,
					&mut consume_ctx,
					flow_id,
					to_version,
					"backfill complete after no-op chunk, flow now active",
				);
				continue;
			}

			if !self.submit_backfill_chunk(state, ctx, flow_id, to_version, flow_changes, &consume_ctx) {
				(consume_ctx.original_reply)(coordinator_error("Pool actor stopped"));
				return;
			}

			consume_ctx.checkpoints.push((flow_id, to_version));
			if let Some(flow_state) = state.states.get_mut(&flow_id) {
				flow_state.update_checkpoint(to_version);
			}

			debug!(
				flow_id = flow_id.0,
				from = from_version.0,
				to = to_version.0,
				"advanced backfilling flow by one chunk"
			);

			if to_version >= consume_ctx.current_version {
				if let Some(flow_state) = state.states.get_mut(&flow_id) {
					flow_state.activate();
				}
				info!(flow_id = flow_id.0, "backfill complete, flow now active");
			}

			state.phase = Phase::AdvancingBackfill {
				flows,
				ctx: consume_ctx,
			};
			return;
		}

		self.finish_consume(state, consume_ctx);
	}

	#[inline]
	fn fetch_flow_checkpoint(&self, flow_id: FlowId) -> Result<CommitVersion> {
		let mut query = self.engine.begin_query(IdentityId::system())?;
		Ok(CdcCheckpoint::fetch(&mut Transaction::Query(&mut query), &flow_id).unwrap_or(CommitVersion(0)))
	}

	#[inline]
	fn read_backfill_chunk(
		&self,
		from_version: CommitVersion,
		to_version: CommitVersion,
		chunk_size: u64,
	) -> CdcBatch {
		self.cdc_store
			.read_range(Bound::Excluded(from_version), Bound::Included(to_version), chunk_size)
			.unwrap_or_else(|e| {
				warn!(error = %e, "Failed to read CDC range for backfill");
				CdcBatch::empty()
			})
	}

	#[inline]
	fn mark_already_caught_up(
		&self,
		state: &mut CoordinatorState,
		flow_id: FlowId,
		current_version: CommitVersion,
	) {
		if let Some(flow_state) = state.states.get_mut(&flow_id) {
			flow_state.activate();
			flow_state.update_checkpoint(current_version);
		}
		info!(flow_id = flow_id.0, "backfill complete, flow now active");
	}

	#[inline]
	fn record_chunk_checkpoint(
		&self,
		state: &mut CoordinatorState,
		consume_ctx: &mut ConsumeContext,
		flow_id: FlowId,
		to_version: CommitVersion,
		caught_up_message: &'static str,
	) {
		consume_ctx.checkpoints.push((flow_id, to_version));
		if let Some(flow_state) = state.states.get_mut(&flow_id) {
			flow_state.update_checkpoint(to_version);
			if to_version >= consume_ctx.current_version {
				flow_state.activate();
			}
		}
		if to_version >= consume_ctx.current_version {
			info!(flow_id = flow_id.0, "{}", caught_up_message);
		}
	}

	#[inline]
	fn submit_backfill_chunk(
		&self,
		state: &CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		flow_id: FlowId,
		to_version: CommitVersion,
		flow_changes: Vec<Change>,
		consume_ctx: &ConsumeContext,
	) -> bool {
		let instruction = FlowInstruction::new(flow_id, to_version, flow_changes);
		let worker_id = *state
			.flow_assignments
			.get(&flow_id)
			.expect("flow must be in flow_assignments after registration");

		let mut worker_batch = WorkerBatch::new(consume_ctx.state_version);
		worker_batch.add_instruction(instruction);

		let self_ref = ctx.self_ref().clone();
		let callback: Box<dyn FnOnce(PoolResponse) + Send> = Box::new(move |resp| {
			let _ = self_ref.send(FlowCoordinatorMessage::PoolReply(resp));
		});

		self.pool
			.send(FlowPoolMessage::SubmitToWorker {
				worker_id,
				batch: worker_batch,
				reply: callback,
			})
			.is_ok()
	}

	fn finish_consume(&self, state: &mut CoordinatorState, consume_ctx: ConsumeContext) {
		Span::current().record("elapsed_us", consume_ctx.consume_start.elapsed().as_micros() as u64);
		state.phase = Phase::Idle;

		if consume_ctx.is_empty() {
			(consume_ctx.original_reply)(Ok(()));
			return;
		}

		let ConsumeContext {
			combined,
			pending_shapes,
			checkpoints,
			original_reply,
			view_changes,
			..
		} = consume_ctx;

		let mut transaction = match self.engine.begin_command(IdentityId::system()) {
			Ok(t) => t,
			Err(e) => {
				(original_reply)(coordinator_error(e));
				return;
			}
		};

		if let Err(e) = apply_pending_writes(&mut transaction, &combined) {
			let _ = transaction.rollback();
			(original_reply)(coordinator_error(e));
			return;
		}

		for change in view_changes {
			transaction.track_flow_change(change);
		}

		if let Err(e) = persist_flow_checkpoints(&mut transaction, &checkpoints) {
			let _ = transaction.rollback();
			(original_reply)(coordinator_error(e));
			return;
		}

		if let Err(e) =
			self.catalog.persist_pending_shapes(&mut Transaction::Command(&mut transaction), pending_shapes)
		{
			let _ = transaction.rollback();
			(original_reply)(coordinator_error(e));
			return;
		}

		match transaction.commit() {
			Ok(_) => (original_reply)(Ok(())),
			Err(e) => (original_reply)(coordinator_error(e)),
		}
	}

	#[instrument(name = "flow::coordinator::filter_cdc", level = "trace", skip(self, state, changes), fields(
		input = changes.len(),
		output = field::Empty
	))]
	fn filter_cdc_for_flow(&self, state: &CoordinatorState, flow_id: FlowId, changes: &[Change]) -> Vec<Change> {
		let dependency_graph = state.analyzer.get_dependency_graph();
		let (mut flow_sources, view_sources) = collect_direct_flow_sources(dependency_graph, flow_id);
		self.add_transitive_view_sources(dependency_graph, state, &mut flow_sources, view_sources);
		let result = filter_changes_by_sources(changes, &flow_sources);
		Span::current().record("output", result.len());
		result
	}

	#[inline]
	fn add_transitive_view_sources(
		&self,
		dependency_graph: &FlowDependencyGraph,
		state: &CoordinatorState,
		flow_sources: &mut collections::HashSet<ShapeId>,
		view_sources: Vec<ViewId>,
	) {
		for view_id in view_sources {
			let Some(producer_flow_id) = dependency_graph.sink_views.get(&view_id) else {
				continue;
			};

			if state.states.contains(producer_flow_id) {
				if let Some(view) = self.catalog.find_view(view_id) {
					flow_sources.insert(view.underlying_id());
				}
				continue;
			}

			for (table_id, flow_ids) in &dependency_graph.source_tables {
				if flow_ids.contains(producer_flow_id) {
					flow_sources.insert(ShapeId::Table(*table_id));
				}
			}
			for (rb_id, flow_ids) in &dependency_graph.source_ringbuffers {
				if flow_ids.contains(producer_flow_id) {
					flow_sources.insert(ShapeId::RingBuffer(*rb_id));
				}
			}
			for (series_id, flow_ids) in &dependency_graph.source_series {
				if flow_ids.contains(producer_flow_id) {
					flow_sources.insert(ShapeId::Series(*series_id));
				}
			}
		}
	}

	#[instrument(name = "flow::coordinator::route_and_group", level = "debug", skip(self, state, changes), fields(
		changes = changes.len(),
		active_flows = field::Empty,
		batches = field::Empty,
		elapsed_us = field::Empty
	))]
	fn route_and_group_changes(
		&self,
		state: &CoordinatorState,
		changes: &[Change],
		to_version: CommitVersion,
		state_version: CommitVersion,
	) -> BTreeMap<usize, WorkerBatch> {
		let start = self.clock.instant();
		let dependency_graph = state.analyzer.get_dependency_graph();

		let active_flow_ids: Vec<_> = state.states.active_flow_ids();
		Span::current().record("active_flows", active_flow_ids.len());

		let mut flow_instructions: BTreeMap<FlowId, FlowInstruction> = BTreeMap::new();
		for flow_id in active_flow_ids {
			let flow_changes = self.filter_cdc_for_flow(state, flow_id, changes);
			if flow_changes.is_empty() {
				continue;
			}
			flow_instructions.insert(flow_id, FlowInstruction::new(flow_id, to_version, flow_changes));
		}
		let submitted: collections::HashSet<FlowId> = flow_instructions.keys().copied().collect();

		let levels = state.analyzer.calculate_execution_levels(dependency_graph);
		let ordered: Vec<FlowId> = levels
			.iter()
			.flat_map(|level| level.iter().filter(|id| submitted.contains(id)).copied())
			.collect();

		let flow_to_worker = &state.flow_assignments;
		let mut worker_batches: BTreeMap<usize, WorkerBatch> = BTreeMap::new();
		for fid in ordered {
			if let Some(instruction) = flow_instructions.remove(&fid) {
				let worker_id = *flow_to_worker
					.get(&fid)
					.expect("flow must be in flow_assignments after registration");
				let batch = worker_batches
					.entry(worker_id)
					.or_insert_with(|| WorkerBatch::new(state_version));
				batch.add_instruction(instruction);
			}
		}

		Span::current().record("batches", worker_batches.len());
		Span::current().record("elapsed_us", start.elapsed().as_micros() as u64);
		worker_batches
	}

	fn compute_flow_assignments(&self, state: &CoordinatorState) -> BTreeMap<FlowId, usize> {
		let dependency_graph = state.analyzer.get_dependency_graph();

		let mut upstream_of: BTreeMap<FlowId, FlowId> = BTreeMap::new();
		for dep in &dependency_graph.dependencies {
			upstream_of.entry(dep.target_flow).or_insert(dep.source_flow);
		}

		let mut assignments: BTreeMap<FlowId, usize> = BTreeMap::new();
		let levels = state.analyzer.calculate_execution_levels(dependency_graph);
		for level in &levels {
			for fid in level {
				let worker_id = match upstream_of.get(fid) {
					Some(upstream) => assignments
						.get(upstream)
						.copied()
						.unwrap_or_else(|| (upstream.0 as usize) % self.num_workers),
					None => (fid.0 as usize) % self.num_workers,
				};
				assignments.insert(*fid, worker_id);
			}
		}
		assignments
	}

	fn rebalance_flows(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		consume_ctx: ConsumeContext,
	) {
		let assignments = self.compute_flow_assignments(state);
		state.flow_assignments = assignments.clone();

		let mut by_worker: BTreeMap<usize, Vec<FlowId>> = BTreeMap::new();
		for (fid, wid) in &assignments {
			by_worker.entry(*wid).or_default().push(*fid);
		}

		let self_ref = ctx.self_ref().clone();
		let callback: Box<dyn FnOnce(PoolResponse) + Send> = Box::new(move |resp| {
			let _ = self_ref.send(FlowCoordinatorMessage::PoolReply(resp));
		});

		if self.pool
			.send(FlowPoolMessage::Rebalance {
				assignments: by_worker,
				reply: callback,
			})
			.is_err()
		{
			(consume_ctx.original_reply)(coordinator_error("Pool actor stopped"));
			return;
		}

		state.phase = Phase::Rebalancing {
			ctx: consume_ctx,
		};
	}

	fn maybe_register_tick_schedule(&self, state: &mut CoordinatorState, flow: &FlowDag) {
		if let Some(tick) = flow.tick() {
			state.tick_schedules.insert(
				flow.id(),
				TickSchedule {
					tick,
					last_tick: self.clock.instant(),
				},
			);
			debug!(
				flow_id = flow.id().0,
				tick_nanos = tick.as_nanos(),
				"registered tick schedule for flow"
			);
		}
	}

	fn handle_tick(&self, state: &mut CoordinatorState, ctx: &Context<FlowCoordinatorMessage>) {
		let now = self.clock.instant();
		let timestamp = DateTime::from_timestamp_millis(self.clock.now_millis()).unwrap();

		let mut due_flows: BTreeMap<usize, Vec<FlowId>> = BTreeMap::new();

		for (flow_id, schedule) in &mut state.tick_schedules {
			let tick_std = Duration::from_nanos(schedule.tick.as_nanos() as u64);
			if now.duration_since(&schedule.last_tick) >= tick_std {
				let worker_id = *state
					.flow_assignments
					.get(flow_id)
					.expect("flow must be in flow_assignments after registration");
				due_flows.entry(worker_id).or_default().push(*flow_id);
				schedule.last_tick = now.clone();
			}
		}

		if due_flows.is_empty() {
			return;
		}

		let state_version = match self.engine.begin_query(IdentityId::system()) {
			Ok(q) => q.version(),
			Err(e) => {
				warn!(error = %e, "failed to begin query for tick");
				return;
			}
		};

		let self_ref = ctx.self_ref().clone();
		let callback: Box<dyn FnOnce(PoolResponse) + Send> = Box::new(move |resp| {
			let _ = self_ref.send(FlowCoordinatorMessage::PoolReply(resp));
		});

		if self.pool
			.send(FlowPoolMessage::Tick {
				ticks: due_flows,
				timestamp,
				state_version,
				reply: callback,
			})
			.is_err()
		{
			warn!("failed to send tick to pool");
			return;
		}

		state.phase = Phase::Ticking;
	}

	fn commit_tick_writes(&self, pending: Pending, pending_shapes: Vec<RowShape>) {
		let mut transaction = match self.engine.begin_command(IdentityId::system()) {
			Ok(t) => t,
			Err(e) => {
				warn!(error = %e, "failed to begin command for tick commit");
				return;
			}
		};

		for (key, pw) in pending.iter_sorted() {
			let result = match pw {
				PendingWrite::Set(value) => transaction.set(key, value.clone()),
				PendingWrite::Remove => transaction.remove(key),
			};
			if let Err(e) = result {
				let _ = transaction.rollback();
				warn!(error = %e, "failed to apply tick write");
				return;
			}
		}

		if let Err(e) =
			self.catalog.persist_pending_shapes(&mut Transaction::Command(&mut transaction), pending_shapes)
		{
			let _ = transaction.rollback();
			warn!(error = %e, "failed to persist tick pending shapes");
			return;
		}

		if let Err(e) = transaction.commit() {
			warn!(error = %e, "failed to commit tick writes");
		}
	}
}

pub fn extract_new_flow_ids(cdcs: &[Cdc]) -> Vec<FlowId> {
	let mut flow_ids = Vec::new();

	for cdc in cdcs {
		for change in &cdc.system_changes {
			if let Some(kind) = Key::kind(change.key())
				&& kind == KeyKind::Flow && let SystemChange::Insert {
				key,
				..
			} = change && let Some(Key::Flow(flow_key)) = Key::decode(key)
			{
				flow_ids.push(flow_key.flow);
			}
		}
	}

	flow_ids
}
