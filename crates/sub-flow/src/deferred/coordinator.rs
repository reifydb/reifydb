// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Coordinator actor that handles CDC consumption and flow orchestration.
//!
//! This module provides:
//! - [`CoordinatorActor`]: Processes CDC events and coordinates flow workers
//! - [`CoordinatorMsg`]: Messages (Consume, PoolReply)
//! - [`FlowConsumeRef`]: Thin `CdcConsume` impl that forwards to the actor

use std::{cmp::min, collections::HashMap, ops::Bound, sync::Arc};

use reifydb_cdc::{
	consume::{checkpoint::CdcCheckpoint, consumer::CdcConsume},
	storage::CdcStore,
};
use reifydb_core::{
	common::CommitVersion,
	encoded::key::EncodedKey,
	interface::{
		catalog::{flow::FlowId, primitive::PrimitiveId},
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
	clock::Clock,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, error::Error};
use tracing::{Span, debug, info, instrument};

use super::{
	instruction::{FlowInstruction, WorkerBatch},
	pool::{PoolMsg, PoolResponse},
	state::FlowStates,
	tracker::PrimitiveVersionTracker,
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
}

/// Context needed to resume processing after an async pool reply.
struct ConsumeContext {
	state_version: CommitVersion,
	current_version: CommitVersion,
	combined: Pending,
	checkpoints: Vec<(FlowId, CommitVersion)>,
	consumer_key: EncodedKey,
	original_reply: Box<dyn FnOnce(Result<()>) + Send>,
	consume_start: reifydb_runtime::clock::Instant,
	/// All flow changes derived from CDCs (computed once during Consume)
	all_changes: Vec<Change>,
	/// Latest CDC version
	latest_version: Option<CommitVersion>,
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
}

/// Helper to create an error result for coordinator replies.
fn coordinator_error(msg: impl std::fmt::Display) -> Result<()> {
	Err(Error(internal!("{}", msg)))
}

/// Coordinator actor - processes CDC and coordinates flow workers.
pub struct CoordinatorActor {
	engine: StandardEngine,
	catalog: FlowCatalog,
	pool: ActorRef<PoolMsg>,
	tracker: Arc<PrimitiveVersionTracker>,
	cdc_store: CdcStore,
	num_workers: usize,
	clock: Clock,
}

impl CoordinatorActor {
	pub fn new(
		engine: StandardEngine,
		catalog: FlowCatalog,
		pool_ref: ActorRef<PoolMsg>,
		tracker: Arc<PrimitiveVersionTracker>,
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
}

impl Actor for CoordinatorActor {
	type State = CoordinatorState;
	type Message = CoordinatorMsg;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		CoordinatorState {
			states: FlowStates::new(),
			analyzer: FlowGraphAnalyzer::new(),
			phase: Phase::Idle,
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
		version_start = tracing::field::Empty,
		version_end = tracing::field::Empty,
		batch_count = tracing::field::Empty,
		elapsed_us = tracing::field::Empty
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
		let state_version = match self.engine.begin_query() {
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
				if let ChangeOrigin::Primitive(source) = &change.origin {
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
		};

		// Discover and load new flows from CDC events using engine.begin_query()
		let new_flow_ids = extract_new_flow_ids(&cdcs);
		let mut new_flows = Vec::new();
		if !new_flow_ids.is_empty() {
			let mut query = match self.engine.begin_query() {
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
						(consume_ctx.original_reply)(coordinator_error(e));
						return;
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
		let phase = std::mem::replace(&mut state.phase, Phase::Idle);

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
					} => {
						consume_ctx.combined = pending;
					}
					PoolResponse::RegisterSuccess => {
						// unexpected but not an error
					}
					PoolResponse::Error(e) => {
						(consume_ctx.original_reply)(coordinator_error(e));
						return;
					}
				}

				// Collect checkpoints for active flows
				if let Some(to_version) = consume_ctx.latest_version {
					for flow_id in state.states.active_flow_ids() {
						consume_ctx.checkpoints.push((flow_id, to_version));
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
						mut pending,
					} => {
						consume_ctx.combined.extend_view_changes(pending.take_view_changes());
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
			Phase::Idle => {}
		}
	}

	/// Transition to the submit-batches phase.
	fn proceed_to_submit(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<CoordinatorMsg>,
		consume_ctx: ConsumeContext,
	) {
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
		// Collect checkpoints for active flows
		let mut consume_ctx = consume_ctx;
		if let Some(to_version) = consume_ctx.latest_version {
			for flow_id in state.states.active_flow_ids() {
				consume_ctx.checkpoints.push((flow_id, to_version));
			}
		}

		self.proceed_to_backfill(state, ctx, consume_ctx);
	}

	/// Transition to the backfill phase.
	fn proceed_to_backfill(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<CoordinatorMsg>,
		consume_ctx: ConsumeContext,
	) {
		if consume_ctx.latest_version.is_none() {
			// No CDC data — finish immediately
			self.finish_consume(state, consume_ctx);
			return;
		}

		let backfilling_flows: Vec<_> = state.states.backfilling_flow_ids();
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

			// Get current checkpoint for this flow
			let from_version = match self.engine.begin_query() {
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
					tracing::warn!(error = %e, "Failed to read CDC range for backfill");
					CdcBatch::empty()
				});

			if batch.items.is_empty() {
				// No CDC in this range, advance checkpoint
				consume_ctx.checkpoints.push((flow_id, to_version));
				if let Some(flow_state) = state.states.get_mut(&flow_id) {
					flow_state.update_checkpoint(to_version);
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
	fn finish_consume(&self, state: &mut CoordinatorState, mut consume_ctx: ConsumeContext) {
		Span::current().record("elapsed_us", consume_ctx.consume_start.elapsed().as_micros() as u64);

		state.phase = Phase::Idle;

		// Begin a command transaction to apply all writes and checkpoints
		let mut transaction = match self.engine.begin_command() {
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
				PendingWrite::Remove => transaction.remove(key),
			};
			if let Err(e) = result {
				let _ = transaction.rollback();
				(consume_ctx.original_reply)(coordinator_error(e));
				return;
			}
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

		// Track view changes so that transactional flows sourcing those views
		// are triggered by the TransactionalFlowPreCommitInterceptor in the same commit.
		for change in consume_ctx.combined.take_view_changes() {
			transaction.track_flow_change(change);
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
		output = tracing::field::Empty
	))]
	fn filter_cdc_for_flow(&self, state: &CoordinatorState, flow_id: FlowId, changes: &[Change]) -> Vec<Change> {
		let dependency_graph = state.analyzer.get_dependency_graph();

		// Get all sources this flow depends on
		let mut flow_sources: std::collections::HashSet<PrimitiveId> = std::collections::HashSet::new();

		// Add table sources
		for (table_id, flow_ids) in &dependency_graph.source_tables {
			if flow_ids.contains(&flow_id) {
				flow_sources.insert(PrimitiveId::Table(*table_id));
			}
		}

		// Add view sources
		let mut view_sources = Vec::new();
		for (view_id, flow_ids) in &dependency_graph.source_views {
			if flow_ids.contains(&flow_id) {
				flow_sources.insert(PrimitiveId::View(*view_id));
				view_sources.push(*view_id);
			}
		}

		// Add ringbuffer sources
		for (rb_id, flow_ids) in &dependency_graph.source_ringbuffers {
			if flow_ids.contains(&flow_id) {
				flow_sources.insert(PrimitiveId::RingBuffer(*rb_id));
			}
		}

		// Resolve transitive dependencies through views: if this flow depends on
		// a view that is produced by another flow (e.g. a transactional view),
		// also consider that producer flow's primitive sources as triggers.
		for view_id in view_sources {
			if let Some(producer_flow_id) = dependency_graph.sink_views.get(&view_id) {
				for (table_id, flow_ids) in &dependency_graph.source_tables {
					if flow_ids.contains(producer_flow_id) {
						flow_sources.insert(PrimitiveId::Table(*table_id));
					}
				}
				for (rb_id, flow_ids) in &dependency_graph.source_ringbuffers {
					if flow_ids.contains(producer_flow_id) {
						flow_sources.insert(PrimitiveId::RingBuffer(*rb_id));
					}
				}
			}
		}

		// Filter changes to only those from this flow's sources
		let result: Vec<Change> = changes
			.iter()
			.filter(|change| {
				if let ChangeOrigin::Primitive(source) = change.origin {
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
		active_flows = tracing::field::Empty,
		batches = tracing::field::Empty,
		elapsed_us = tracing::field::Empty
	))]
	fn route_and_group_changes(
		&self,
		state: &CoordinatorState,
		changes: &[Change],
		to_version: CommitVersion,
		state_version: CommitVersion,
	) -> HashMap<usize, WorkerBatch> {
		let start = self.clock.instant();
		let mut worker_batches: HashMap<usize, WorkerBatch> = HashMap::new();

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
