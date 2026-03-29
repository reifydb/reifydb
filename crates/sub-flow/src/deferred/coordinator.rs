// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Coordinator actor that handles CDC consumption and flow orchestration.
//!
//! This module provides:
//! - [`CoordinatorActor`]: Processes CDC events and coordinates flow workers
//! - [`CoordinatorMsg`]: Messages (Consume, PoolReply)
//! - [`FlowConsumeRef`]: Thin `CdcConsume` impl that forwards to the actor

use std::{cmp::min, collections, collections::BTreeMap, fmt, mem, ops::Bound, sync::Arc, time::Duration};

use reifydb_cdc::{
	consume::{checkpoint::CdcCheckpoint, consumer::CdcConsume},
	storage::CdcStore,
};
use reifydb_core::{
	common::CommitVersion,
	encoded::key::EncodedKey,
	interface::{
		catalog::{flow::FlowId, shape::ShapeId},
		cdc::{Cdc, CdcBatch, SystemChange},
		change::{Change, ChangeOrigin},
	},
	internal,
	key::{Key, kind::KeyKind},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_rql::flow::{analyzer::FlowGraphAnalyzer, flow::FlowDag};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::ActorConfig,
		traits::{Actor, Directive},
	},
	context::clock::{Clock, Instant},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	Result,
	error::Error,
	value::{datetime::DateTime, identity::IdentityId},
};
use tracing::{Span, debug, field, info, instrument, warn};

use super::{
	instruction::{FlowInstruction, WorkerBatch},
	pool::{PoolMsg, PoolResponse},
	state::FlowStates,
	tracker::ShapeVersionTracker,
};
use crate::{
	catalog::FlowCatalog,
	transaction::pending::{Pending, PendingWrite},
};

pub(crate) struct FlowConsumeRef {
	pub actor_ref: ActorRef<CoordinatorMsg>,
	pub consumer_key: EncodedKey,
}

impl CdcConsume for FlowConsumeRef {
	fn consume(&self, cdcs: Vec<Cdc>, reply: Box<dyn FnOnce(Result<()>) + Send>) {
		let current_version = cdcs.last().map(|c| c.version).unwrap_or(CommitVersion(0));

		let result = self.actor_ref.send(CoordinatorMsg::Consume {
			cdcs,
			consumer_key: self.consumer_key.clone(),
			current_version,
			reply,
		});

		if let Err(send_err) = result {
			// Extract the reply callback from the failed message and call it with an error
			if let CoordinatorMsg::Consume {
				reply,
				..
			} = send_err.into_inner()
			{
				reply(Err(Error(internal!("Coordinator actor stopped"))));
			}
		}
	}
}

/// Messages for the coordinator actor
pub enum CoordinatorMsg {
	/// Consume CDC events and process them through flows
	Consume {
		cdcs: Vec<Cdc>,
		/// Consumer checkpoint key (for persisting the consumer-level checkpoint)
		consumer_key: EncodedKey,
		/// Current version for backfill processing
		current_version: CommitVersion,
		reply: Box<dyn FnOnce(Result<()>) + Send>,
	},
	/// Async reply from PoolActor
	PoolReply(PoolResponse),
	/// Periodic tick for time-based maintenance
	Tick,
}

/// Context needed to resume processing after an async pool reply.
struct ConsumeContext {
	state_version: CommitVersion,
	current_version: CommitVersion,
	combined: Pending,
	checkpoints: Vec<(FlowId, CommitVersion)>,
	consumer_key: EncodedKey,
	original_reply: Box<dyn FnOnce(Result<()>) + Send>,
	consume_start: Instant,
	/// All flow changes derived from CDCs (computed once during Consume)
	all_changes: Vec<Change>,
	/// Latest CDC version
	latest_version: Option<CommitVersion>,
	/// Flows downstream of view-producing flows in this batch.
	/// These flows will have new CDC events in the next cycle from view changes,
	/// so their checkpoints should NOT be advanced yet.
	downstream_flows: collections::HashSet<FlowId>,
	/// View changes accumulated from flow workers for cascading to transactional flows.
	view_changes: Vec<Change>,
}

enum Phase {
	/// Idle, ready for new work
	Idle,
	/// Registering flows with the pool one at a time
	RegisteringFlows {
		flows: Vec<FlowDag>,
		ctx: ConsumeContext,
	},
	/// Submitting batches to the pool
	SubmittingBatches {
		ctx: ConsumeContext,
	},
	/// Advancing backfill flows one at a time
	AdvancingBackfill {
		flows: Vec<FlowId>,
		ctx: ConsumeContext,
	},
	/// Waiting for tick results from pool
	Ticking,
}

struct TickSchedule {
	tick: Duration,
	last_tick: Instant,
}

/// Helper to create an error result for coordinator replies.
fn coordinator_error(msg: impl fmt::Display) -> Result<()> {
	Err(Error(internal!("{}", msg)))
}

/// Coordinator actor - processes CDC and coordinates flow workers.
pub struct CoordinatorActor {
	engine: StandardEngine,
	catalog: FlowCatalog,
	pool: ActorRef<PoolMsg>,
	tracker: Arc<ShapeVersionTracker>,
	cdc_store: CdcStore,
	num_workers: usize,
	clock: Clock,
}

impl CoordinatorActor {
	pub fn new(
		engine: StandardEngine,
		catalog: FlowCatalog,
		pool_ref: ActorRef<PoolMsg>,
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

/// Actor state - holds flow states and analyzer
pub struct CoordinatorState {
	states: FlowStates,
	analyzer: FlowGraphAnalyzer,
	phase: Phase,
	tick_schedules: BTreeMap<FlowId, TickSchedule>,
}

impl Actor for CoordinatorActor {
	type State = CoordinatorState;
	type Message = CoordinatorMsg;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		ctx.schedule_once(Duration::from_secs(1), || CoordinatorMsg::Tick);

		CoordinatorState {
			states: FlowStates::new(),
			analyzer: FlowGraphAnalyzer::new(),
			phase: Phase::Idle,
			tick_schedules: BTreeMap::new(),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			CoordinatorMsg::Consume {
				cdcs,
				consumer_key,
				current_version,
				reply,
			} => {
				self.handle_consume(state, ctx, cdcs, consumer_key, current_version, reply);
			}
			CoordinatorMsg::PoolReply(response) => {
				self.handle_pool_reply(state, ctx, response);
			}
			CoordinatorMsg::Tick => {
				if matches!(state.phase, Phase::Idle) {
					self.handle_tick(state, ctx);
				}
				ctx.schedule_once(Duration::from_secs(1), || CoordinatorMsg::Tick);
			}
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}

impl CoordinatorActor {
	/// Handle Consume message — start of the multi-phase pipeline.
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
		ctx: &Context<CoordinatorMsg>,
		cdcs: Vec<Cdc>,
		consumer_key: EncodedKey,
		current_version: CommitVersion,
		reply: Box<dyn FnOnce(Result<()>) + Send>,
	) {
		if !matches!(state.phase, Phase::Idle) {
			(reply)(coordinator_error("Coordinator busy"));
			return;
		}

		let consume_start = self.clock.instant();

		// Record version range
		if let Some(first) = cdcs.first() {
			Span::current().record("version_start", first.version.0);
		}
		if let Some(last) = cdcs.last() {
			Span::current().record("version_end", last.version.0);
		}

		let latest_version = cdcs.last().map(|c| c.version);

		// Get state_version from a read-only query
		let state_version = match self.engine.begin_query(IdentityId::system()) {
			Ok(q) => q.version(),
			Err(e) => {
				(reply)(coordinator_error(e));
				return;
			}
		};

		// Update tracker and collect changes directly from CDC
		let mut all_changes = Vec::new();
		for cdc in &cdcs {
			let version = cdc.version;

			// Update tracker for lag calculation
			for change in &cdc.changes {
				if let ChangeOrigin::Shape(source) = &change.origin {
					self.tracker.update(*source, version);
				}
			}

			// Collect changes directly (no conversion needed with columnar layout)
			all_changes.extend(cdc.changes.iter().cloned());
		}
		let consume_ctx = ConsumeContext {
			state_version,
			current_version,
			combined: Pending::new(),
			checkpoints: Vec::new(),
			consumer_key,
			original_reply: reply,
			consume_start,
			all_changes,
			latest_version,
			downstream_flows: collections::HashSet::new(),
			view_changes: Vec::new(),
		};

		// Discover and load new flows from CDC events using engine.begin_query()
		let new_flow_ids = extract_new_flow_ids(&cdcs);
		let mut new_flows = Vec::new();
		if !new_flow_ids.is_empty() {
			let mut query = match self.engine.begin_query(IdentityId::system()) {
				Ok(q) => q,
				Err(e) => {
					(consume_ctx.original_reply)(coordinator_error(e));
					return;
				}
			};
			for flow_id in new_flow_ids {
				match self.catalog.get_or_load_flow(&mut Transaction::Query(&mut query), flow_id) {
					Ok((flow, is_new)) => {
						if is_new {
							new_flows.push(flow);
						} else {
							// Flow was already cached (e.g. by the dispatcher for
							// transactional views). Add it to the analyzer so the
							// dependency graph includes its source/sink info — this
							// lets filter_cdc_for_flow resolve transitive dependencies
							// through transactional views.
							state.analyzer.add(flow);
							// Remove from FlowCatalog so the lag provider doesn't
							// include this non-deferred flow in its calculations.
							// Transactional flows have no CDC checkpoint and would
							// report perpetual lag, blocking `await` indefinitely.
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
		}

		// Start registering flows (if any), otherwise proceed to submit
		if new_flows.is_empty() {
			self.proceed_to_submit(state, ctx, consume_ctx);
		} else {
			// Register flows one at a time via async callbacks
			// Pop the first flow to register now
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
				let _ = self_ref.send(CoordinatorMsg::PoolReply(resp));
			});

			if self.pool
				.send(PoolMsg::RegisterFlow {
					flow,
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
	}

	/// Handle a PoolReply based on the current phase.
	fn handle_pool_reply(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<CoordinatorMsg>,
		response: PoolResponse,
	) {
		let phase = mem::replace(&mut state.phase, Phase::Idle);

		match phase {
			Phase::RegisteringFlows {
				flows: mut remaining_flows,
				ctx: consume_ctx,
			} => {
				// Check if registration succeeded
				match response {
					PoolResponse::RegisterSuccess
					| PoolResponse::Success {
						..
					} => {}
					PoolResponse::Error(e) => {
						(consume_ctx.original_reply)(coordinator_error(e));
						return;
					}
				}

				if remaining_flows.is_empty() {
					// All flows registered, proceed to submit
					self.proceed_to_submit(state, ctx, consume_ctx);
				} else {
					// Register next flow
					let flow = remaining_flows.remove(0);
					let flow_id = flow.id;

					state.analyzer.add(flow.clone());
					self.maybe_register_tick_schedule(state, &flow);
					if flow.is_subscription() {
						state.states.register_active(flow_id, consume_ctx.current_version);
						debug!(
							flow_id = flow_id.0,
							"registered new subscription flow as active"
						);
					} else {
						state.states.register_backfilling(flow_id);
						debug!(
							flow_id = flow_id.0,
							"registered new flow in backfilling status"
						);
					}

					let self_ref = ctx.self_ref().clone();
					let callback: Box<dyn FnOnce(PoolResponse) + Send> = Box::new(move |resp| {
						let _ = self_ref.send(CoordinatorMsg::PoolReply(resp));
					});

					if self.pool
						.send(PoolMsg::RegisterFlow {
							flow,
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
			}
			Phase::SubmittingBatches {
				ctx: mut consume_ctx,
			} => {
				match response {
					PoolResponse::Success {
						pending,
						view_changes,
					} => {
						consume_ctx.combined = pending;
						consume_ctx.view_changes.extend(view_changes);
					}
					PoolResponse::RegisterSuccess => {
						// unexpected but not an error
					}
					PoolResponse::Error(e) => {
						(consume_ctx.original_reply)(coordinator_error(e));
						return;
					}
				}

				// Collect checkpoints for active flows, skipping downstream flows
				// whose upstream view-producing flows were just submitted.
				if let Some(to_version) = consume_ctx.latest_version {
					for flow_id in state.states.active_flow_ids() {
						if !consume_ctx.downstream_flows.contains(&flow_id) {
							consume_ctx.checkpoints.push((flow_id, to_version));
						}
					}
				}

				// Proceed to advance backfilling flows
				self.proceed_to_backfill(state, ctx, consume_ctx);
			}
			Phase::AdvancingBackfill {
				flows: remaining_flow_ids,
				ctx: mut consume_ctx,
			} => {
				// Collect result from previous backfill worker submission
				match response {
					PoolResponse::Success {
						pending,
						view_changes,
					} => {
						consume_ctx.view_changes.extend(view_changes);
						for (key, value) in pending.iter_sorted() {
							match value {
								PendingWrite::Set(v) => {
									consume_ctx
										.combined
										.insert(key.clone(), v.clone());
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
			Phase::Ticking => match response {
				PoolResponse::Success {
					pending,
					..
				} => {
					self.commit_tick_writes(pending);
				}
				PoolResponse::Error(e) => {
					warn!(error = %e, "tick processing failed");
				}
				_ => {}
			},
			Phase::Idle => {}
		}
	}

	/// Transition to the submit-batches phase.
	fn proceed_to_submit(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<CoordinatorMsg>,
		mut consume_ctx: ConsumeContext,
	) {
		if let Some(to_version) = consume_ctx.latest_version {
			let mut worker_batches = self.route_and_group_changes(
				state,
				&consume_ctx.all_changes,
				to_version,
				consume_ctx.state_version,
			);

			Span::current().record("batch_count", worker_batches.len());

			// Identify flows downstream of view-producing flows in this batch.
			// Only mark consumers that were actually routed in this same batch.
			// If a downstream flow is not scheduled here, there is nothing to demote:
			// it can stay active and wait for the upstream view's own CDC.
			let dependency_graph = state.analyzer.get_dependency_graph();
			let submitted_flow_ids: collections::HashSet<FlowId> = worker_batches
				.values()
				.flat_map(|batch| batch.instructions.iter().map(|i| i.flow_id))
				.collect();

			for (view_id, producer_flow_id) in &dependency_graph.sink_views {
				if submitted_flow_ids.contains(producer_flow_id) {
					if let Some(consumer_flow_ids) = dependency_graph.source_views.get(view_id) {
						for fid in consumer_flow_ids {
							if submitted_flow_ids.contains(fid) {
								consume_ctx.downstream_flows.insert(*fid);
							}
						}
					}
				}
			}
			// Remove downstream flow instructions from batches — they would read
			// stale (uncommitted) upstream view data. Demote them to Backfilling
			// so they re-process in the next cycle with committed data.
			if !consume_ctx.downstream_flows.is_empty() {
				for batch in worker_batches.values_mut() {
					batch.instructions
						.retain(|i| !consume_ctx.downstream_flows.contains(&i.flow_id));
				}
				worker_batches.retain(|_, batch| !batch.instructions.is_empty());

				for flow_id in &consume_ctx.downstream_flows {
					if let Some(flow_state) = state.states.get_mut(flow_id) {
						if flow_state.is_active() {
							flow_state.deactivate();
						}
					}
				}
			}

			if !worker_batches.is_empty() {
				let self_ref = ctx.self_ref().clone();
				let callback: Box<dyn FnOnce(PoolResponse) + Send> = Box::new(move |resp| {
					let _ = self_ref.send(CoordinatorMsg::PoolReply(resp));
				});

				if self.pool
					.send(PoolMsg::Submit {
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

		// No batches to submit or no latest_version, skip to backfill
		// Collect checkpoints for active flows, skipping downstream flows.
		if let Some(to_version) = consume_ctx.latest_version {
			for flow_id in state.states.active_flow_ids() {
				if !consume_ctx.downstream_flows.contains(&flow_id) {
					consume_ctx.checkpoints.push((flow_id, to_version));
				}
			}
		}

		self.proceed_to_backfill(state, ctx, consume_ctx);
	}

	/// Transition to the backfill phase.
	fn proceed_to_backfill(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<CoordinatorMsg>,
		mut consume_ctx: ConsumeContext,
	) {
		if consume_ctx.latest_version.is_none() {
			// No CDC data — finish immediately
			self.finish_consume(state, consume_ctx);
			return;
		}

		let backfilling_flows: Vec<_> = state.states.backfilling_flow_ids();

		// Identify backfilling flows downstream of other backfilling view-producers.
		// These flows would read stale (uncommitted) view data if processed now,
		// so they should be skipped and will backfill in the next cycle.
		let dependency_graph = state.analyzer.get_dependency_graph();
		for (view_id, producer_flow_id) in &dependency_graph.sink_views {
			if backfilling_flows.contains(producer_flow_id) {
				if let Some(consumer_flow_ids) = dependency_graph.source_views.get(view_id) {
					for fid in consumer_flow_ids {
						consume_ctx.downstream_flows.insert(*fid);
					}
				}
			}
		}

		if backfilling_flows.is_empty() {
			self.finish_consume(state, consume_ctx);
			return;
		}

		self.advance_next_backfill_flow(state, ctx, backfilling_flows, consume_ctx);
	}

	/// Advance the next backfilling flow, or finish if none remain.
	fn advance_next_backfill_flow(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<CoordinatorMsg>,
		mut flows: Vec<FlowId>,
		mut consume_ctx: ConsumeContext,
	) {
		const BACKFILL_CHUNK_SIZE: u64 = 1_000;

		while let Some(flow_id) = flows.first().copied() {
			flows.remove(0);

			// Skip downstream flows — they'll backfill in the next cycle
			// with committed upstream view data.
			if consume_ctx.downstream_flows.contains(&flow_id) {
				continue;
			}

			// Get current checkpoint for this flow
			let from_version = match self.engine.begin_query(IdentityId::system()) {
				Ok(mut query) => CdcCheckpoint::fetch(&mut Transaction::Query(&mut query), &flow_id)
					.unwrap_or(CommitVersion(0)),
				Err(e) => {
					(consume_ctx.original_reply)(coordinator_error(e));
					return;
				}
			};
			// Check if already caught up
			if from_version >= consume_ctx.current_version {
				if let Some(flow_state) = state.states.get_mut(&flow_id) {
					flow_state.activate();
					flow_state.update_checkpoint(consume_ctx.current_version);
				}
				info!(flow_id = flow_id.0, "backfill complete, flow now active");
				continue;
			}

			// Calculate chunk range
			let to_version =
				CommitVersion(min(from_version.0 + BACKFILL_CHUNK_SIZE, consume_ctx.current_version.0));

			// Fetch CDC for this chunk from storage
			let batch = self
				.cdc_store
				.read_range(
					Bound::Excluded(from_version),
					Bound::Included(to_version),
					BACKFILL_CHUNK_SIZE,
				)
				.unwrap_or_else(|e| {
					warn!(error = %e, "Failed to read CDC range for backfill");
					CdcBatch::empty()
				});

			if batch.items.is_empty() {
				// No CDC in this range, advance checkpoint
				consume_ctx.checkpoints.push((flow_id, to_version));
				if let Some(flow_state) = state.states.get_mut(&flow_id) {
					flow_state.update_checkpoint(to_version);
					if to_version >= consume_ctx.current_version {
						flow_state.activate();
					}
				}
				if to_version >= consume_ctx.current_version {
					info!(
						flow_id = flow_id.0,
						"backfill complete after empty chunk, flow now active"
					);
				}
				continue;
			}

			// Collect changes directly from CDC
			let mut chunk_changes = Vec::new();
			for cdc in &batch.items {
				chunk_changes.extend(cdc.changes.iter().cloned());
			}

			// Filter to only changes relevant to this flow
			let flow_changes = self.filter_cdc_for_flow(state, flow_id, &chunk_changes);

			if flow_changes.is_empty() {
				// CDC exists but no relevant changes for this flow
				consume_ctx.checkpoints.push((flow_id, to_version));
				if let Some(flow_state) = state.states.get_mut(&flow_id) {
					flow_state.update_checkpoint(to_version);
					if to_version >= consume_ctx.current_version {
						flow_state.activate();
					}
				}
				if to_version >= consume_ctx.current_version {
					info!(
						flow_id = flow_id.0,
						"backfill complete after no-op chunk, flow now active"
					);
				}
				continue;
			}

			// Create instruction and send to worker via callback
			let instruction = FlowInstruction::new(flow_id, to_version, flow_changes);
			let worker_id = (flow_id.0 as usize) % self.num_workers;

			let mut worker_batch = WorkerBatch::new(consume_ctx.state_version);
			worker_batch.add_instruction(instruction);

			let self_ref = ctx.self_ref().clone();
			let callback: Box<dyn FnOnce(PoolResponse) + Send> = Box::new(move |resp| {
				let _ = self_ref.send(CoordinatorMsg::PoolReply(resp));
			});

			if self.pool
				.send(PoolMsg::SubmitToWorker {
					worker_id,
					batch: worker_batch,
					reply: callback,
				})
				.is_err()
			{
				(consume_ctx.original_reply)(coordinator_error("Pool actor stopped"));
				return;
			}

			// Record checkpoint and state updates for this flow
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

			// Check if now caught up
			if to_version >= consume_ctx.current_version {
				if let Some(flow_state) = state.states.get_mut(&flow_id) {
					flow_state.activate();
				}
				info!(flow_id = flow_id.0, "backfill complete, flow now active");
			}

			// Save remaining and wait for pool reply
			state.phase = Phase::AdvancingBackfill {
				flows,
				ctx: consume_ctx,
			};
			return;
		}

		// No more backfill flows to process — finish
		self.finish_consume(state, consume_ctx);
	}

	/// Complete the consume operation: commit writes and checkpoints directly.
	fn finish_consume(&self, state: &mut CoordinatorState, consume_ctx: ConsumeContext) {
		Span::current().record("elapsed_us", consume_ctx.consume_start.elapsed().as_micros() as u64);

		state.phase = Phase::Idle;

		// Begin a command transaction to apply all writes and checkpoints
		let mut transaction = match self.engine.begin_command(IdentityId::system()) {
			Ok(t) => t,
			Err(e) => {
				(consume_ctx.original_reply)(coordinator_error(e));
				return;
			}
		};

		// Apply pending writes directly to the transaction
		for (key, pw) in consume_ctx.combined.iter_sorted() {
			let result = match pw {
				PendingWrite::Set(value) => transaction.set(key, value.clone()),
				PendingWrite::Remove => {
					// Preserve deleted row values so CDC can emit Diff::Remove for
					// downstream deferred flows that source from views.
					if matches!(Key::kind(key), Some(KeyKind::Row)) {
						match transaction.get(key) {
							Ok(Some(existing)) => transaction.unset(key, existing.row),
							Ok(None) => transaction.remove(key),
							Err(e) => {
								let _ = transaction.rollback();
								(consume_ctx.original_reply)(coordinator_error(e));
								return;
							}
						}
					} else {
						transaction.remove(key)
					}
				}
			};
			if let Err(e) = result {
				let _ = transaction.rollback();
				(consume_ctx.original_reply)(coordinator_error(e));
				return;
			}
		}

		// Feed view changes to cascading transactional flows
		for change in consume_ctx.view_changes {
			transaction.track_flow_change(change);
		}

		// Persist per-flow checkpoints
		for (flow_id, version) in &consume_ctx.checkpoints {
			if let Err(e) = CdcCheckpoint::persist(&mut transaction, flow_id, *version) {
				let _ = transaction.rollback();
				(consume_ctx.original_reply)(coordinator_error(e));
				return;
			}
		}

		// Persist consumer checkpoint
		if let Some(latest_version) = consume_ctx.latest_version {
			if let Err(e) =
				CdcCheckpoint::persist(&mut transaction, &consume_ctx.consumer_key, latest_version)
			{
				let _ = transaction.rollback();
				(consume_ctx.original_reply)(coordinator_error(e));
				return;
			}
		}

		// Commit the transaction
		match transaction.commit() {
			Ok(_) => {
				(consume_ctx.original_reply)(Ok(()));
			}
			Err(e) => {
				(consume_ctx.original_reply)(coordinator_error(e));
			}
		}
	}

	/// Filter CDC changes to only those relevant to a specific flow.
	#[instrument(name = "flow::coordinator::filter_cdc", level = "trace", skip(self, state, changes), fields(
		input = changes.len(),
		output = field::Empty
	))]
	fn filter_cdc_for_flow(&self, state: &CoordinatorState, flow_id: FlowId, changes: &[Change]) -> Vec<Change> {
		let dependency_graph = state.analyzer.get_dependency_graph();

		// Get all sources this flow depends on
		let mut flow_sources: collections::HashSet<ShapeId> = collections::HashSet::new();

		// Add table sources
		for (table_id, flow_ids) in &dependency_graph.source_tables {
			if flow_ids.contains(&flow_id) {
				flow_sources.insert(ShapeId::Table(*table_id));
			}
		}

		// Add view sources
		let mut view_sources = Vec::new();
		for (view_id, flow_ids) in &dependency_graph.source_views {
			if flow_ids.contains(&flow_id) {
				flow_sources.insert(ShapeId::View(*view_id));
				view_sources.push(*view_id);
			}
		}

		// Add ringbuffer sources
		for (rb_id, flow_ids) in &dependency_graph.source_ringbuffers {
			if flow_ids.contains(&flow_id) {
				flow_sources.insert(ShapeId::RingBuffer(*rb_id));
			}
		}

		// Add series sources
		for (series_id, flow_ids) in &dependency_graph.source_series {
			if flow_ids.contains(&flow_id) {
				flow_sources.insert(ShapeId::Series(*series_id));
			}
		}

		// Resolve transitive dependencies through views only for non-deferred
		// producer flows. Deferred views publish their own CDC, so waking a
		// downstream deferred flow from the producer's base-table CDC creates
		// a race where the consumer runs before the upstream view commit lands.
		//
		// Transactional views are different: they never publish CDC for the
		// derived view rows, so their downstream consumers must be triggered
		// from the producer flow's primitive sources instead.
		for view_id in view_sources {
			if let Some(producer_flow_id) = dependency_graph.sink_views.get(&view_id) {
				// Deferred flows are tracked in coordinator state. If the
				// producer is tracked here, wait for its view CDC instead of
				// routing its primitive-source CDC to this consumer.
				if state.states.contains(producer_flow_id) {
					// Deferred producer writes to the view's underlying primitive.
					// Resolve it on demand and add to sources so CDC matches.
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

		// Filter changes to only those from this flow's sources
		let result: Vec<Change> = changes
			.iter()
			.filter(|change| {
				if let ChangeOrigin::Shape(source) = change.origin {
					flow_sources.contains(&source)
				} else {
					true
				}
			})
			.cloned()
			.collect();
		Span::current().record("output", result.len());
		result
	}

	/// Route CDC changes to flows and group by worker.
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
		let mut worker_batches: BTreeMap<usize, WorkerBatch> = BTreeMap::new();

		let active_flow_ids: Vec<_> = state.states.active_flow_ids();
		Span::current().record("active_flows", active_flow_ids.len());

		for flow_id in active_flow_ids {
			let flow_changes = self.filter_cdc_for_flow(state, flow_id, changes);

			if flow_changes.is_empty() {
				continue;
			}

			let worker_id = (flow_id.0 as usize) % self.num_workers;

			let batch = worker_batches.entry(worker_id).or_insert_with(|| WorkerBatch::new(state_version));

			batch.add_instruction(FlowInstruction::new(flow_id, to_version, flow_changes));
		}

		Span::current().record("batches", worker_batches.len());
		Span::current().record("elapsed_us", start.elapsed().as_micros() as u64);
		worker_batches
	}

	/// Register a tick schedule for a flow if it has a tick duration configured.
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

	fn handle_tick(&self, state: &mut CoordinatorState, ctx: &Context<CoordinatorMsg>) {
		let now = self.clock.instant();
		let timestamp = DateTime::from_timestamp_millis(self.clock.now_millis()).unwrap();

		let mut due_flows: BTreeMap<usize, Vec<FlowId>> = BTreeMap::new();

		for (flow_id, schedule) in &mut state.tick_schedules {
			let tick_std = Duration::from_nanos(schedule.tick.as_nanos() as u64);
			if now.duration_since(schedule.last_tick.clone()) >= tick_std {
				let worker_id = (flow_id.0 as usize) % self.num_workers;
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
			let _ = self_ref.send(CoordinatorMsg::PoolReply(resp));
		});

		if self.pool
			.send(PoolMsg::Tick {
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

	fn commit_tick_writes(&self, pending: Pending) {
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

		if let Err(e) = transaction.commit() {
			warn!(error = %e, "failed to commit tick writes");
		}
	}
}

/// Extract new flow IDs from CDC events.
///
/// This is a helper to detect flow registrations
/// so the coordinator can load them from the catalog before processing.
pub fn extract_new_flow_ids(cdcs: &[Cdc]) -> Vec<FlowId> {
	let mut flow_ids = Vec::new();

	for cdc in cdcs {
		for change in &cdc.system_changes {
			if let Some(kind) = Key::kind(change.key()) {
				if kind == KeyKind::Flow {
					if let SystemChange::Insert {
						key,
						..
					} = change
					{
						if let Some(Key::Flow(flow_key)) = Key::decode(key) {
							flow_ids.push(flow_key.flow);
						}
					}
				}
			}
		}
	}

	flow_ids
}
