// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Coordinator actor that handles CDC consumption and flow orchestration.
//!
//! This module provides an actor-based implementation of flow coordination:
//! - [`CoordinatorActor`]: Processes CDC events and coordinates flow workers
//! - [`CoordinatorMsg`]: Messages (Consume)
//! - [`CoordinatorResponse`]: Response with pending writes and checkpoints

use std::{cmp::min, collections::HashMap, ops::Bound, sync::Arc};

use crossbeam_channel::{Sender, bounded};
use reifydb_cdc::{consume::checkpoint::CdcCheckpoint, storage::CdcStore};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{flow::FlowId, primitive::PrimitiveId},
		cdc::{Cdc, CdcBatch, CdcChange},
	},
	key::{Key, kind::KeyKind},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_rql::flow::{analyzer::FlowGraphAnalyzer, flow::FlowDag};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		traits::{Actor, ActorConfig, Flow},
	},
	clock::Clock,
};
use reifydb_sdk::flow::{FlowChange, FlowChangeOrigin::External};
use tracing::{Span, debug, info, instrument};

use crate::{
	catalog::FlowCatalog,
	convert,
	instruction::{FlowInstruction, WorkerBatch},
	pool::{PoolMsg, PoolResponse},
	state::FlowStates,
	tracker::PrimitiveVersionTracker,
	transaction::pending::PendingWrites,
};

/// Result of consuming CDC events
pub struct ConsumeResult {
	/// Pending writes to apply to the transaction
	pub pending_writes: PendingWrites,
	/// Checkpoints to persist (flow_id -> version)
	pub checkpoints: Vec<(FlowId, CommitVersion)>,
}

/// Messages for the coordinator actor
pub enum CoordinatorMsg {
	/// Consume CDC events and process them through flows
	Consume {
		cdcs: Vec<Cdc>,
		state_version: CommitVersion,
		/// New flows pre-loaded by the wrapper (from CDC flow insertions)
		new_flows: Vec<FlowDag>,
		/// Current version for backfill processing
		current_version: CommitVersion,
		reply: Sender<CoordinatorResponse>,
	},
}

/// Response from the coordinator actor
pub enum CoordinatorResponse {
	/// Operation succeeded
	Success(ConsumeResult),
	/// Operation failed with error message
	Error(String),
}

/// Coordinator actor - processes CDC and coordinates flow workers.
pub struct CoordinatorActor {
	engine: StandardEngine,
	catalog: FlowCatalog,
	pool_ref: ActorRef<PoolMsg>,
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
			pool_ref,
			tracker,
			cdc_store,
			num_workers,
			clock,
		}
	}
}

/// Actor state - holds flow states and analyzer (previously RefCell)
pub struct CoordinatorState {
	states: FlowStates,
	analyzer: FlowGraphAnalyzer,
}

impl Actor for CoordinatorActor {
	type State = CoordinatorState;
	type Message = CoordinatorMsg;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		CoordinatorState {
			states: FlowStates::new(),
			analyzer: FlowGraphAnalyzer::new(),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Flow {
		match msg {
			CoordinatorMsg::Consume {
				cdcs,
				state_version,
				new_flows,
				current_version,
				reply,
			} => {
				let resp = self.handle_consume(state, cdcs, state_version, new_flows, current_version);
				let _ = reply.send(resp);
			}
		}
		Flow::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(0) // unbounded
	}
}

impl CoordinatorActor {
	/// Handle Consume message - main CDC processing logic.
	#[instrument(name = "flow::coordinator_actor::consume", level = "debug", skip(self, state, cdcs, new_flows), fields(
		cdc_count = cdcs.len(),
		version_start = tracing::field::Empty,
		version_end = tracing::field::Empty,
		batch_count = tracing::field::Empty,
		elapsed_us = tracing::field::Empty
	))]
	fn handle_consume(
		&self,
		state: &mut CoordinatorState,
		cdcs: Vec<Cdc>,
		state_version: CommitVersion,
		new_flows: Vec<FlowDag>,
		current_version: CommitVersion,
	) -> CoordinatorResponse {
		let consume_start = self.clock.instant();

		// Record version range
		if let Some(first) = cdcs.first() {
			Span::current().record("version_start", first.version.0);
		}
		if let Some(last) = cdcs.last() {
			Span::current().record("version_end", last.version.0);
		}

		let latest_version = cdcs.last().map(|c| c.version);

		// Register new flows
		for flow in new_flows {
			let flow_id = flow.id;
			if let Err(e) = self.register_flow_in_pool(flow.clone()) {
				return CoordinatorResponse::Error(e);
			}
			state.analyzer.add(flow);
			state.states.register_backfilling(flow_id);
			debug!(flow_id = flow_id.0, "registered new flow in backfilling status");
		}

		// Update tracker and convert CDC to flow changes
		let mut all_changes = Vec::new();
		for cdc in &cdcs {
			let version = cdc.version;

			// Update tracker for lag calculation
			for change in &cdc.changes {
				if let Some(Key::Row(row_key)) = Key::decode(change.key()) {
					self.tracker.update(row_key.primitive, version);
				}
			}

			// Convert CDC to flow changes
			match convert::to_flow_change(&self.engine, &self.catalog, cdc, version, &self.clock) {
				Ok(changes) => all_changes.extend(changes),
				Err(e) => return CoordinatorResponse::Error(e.to_string()),
			}
		}

		let mut combined_pending = PendingWrites::new();
		let mut checkpoints = Vec::new();

		// Route changes to active flows and group by worker
		if let Some(to_version) = latest_version {
			let worker_batches =
				self.route_and_group_changes(state, &all_changes, to_version, state_version);

			Span::current().record("batch_count", worker_batches.len());

			if !worker_batches.is_empty() {
				match self.submit_to_pool(worker_batches) {
					Ok(pending) => combined_pending = pending,
					Err(e) => return CoordinatorResponse::Error(e),
				}
			}

			// Collect checkpoints for active flows
			for flow_id in state.states.active_flow_ids() {
				checkpoints.push((flow_id, to_version));
			}

			// Advance backfilling flows
			match self.advance_backfilling_flows(state, current_version, state_version) {
				Ok((backfill_pending, backfill_checkpoints)) => {
					// Merge backfill pending writes
					for (key, value) in backfill_pending.iter_sorted() {
						match value {
							crate::transaction::pending::Pending::Set(v) => {
								combined_pending.insert(key.clone(), v.clone());
							}
							crate::transaction::pending::Pending::Remove => {
								combined_pending.remove(key.clone());
							}
						}
					}
					checkpoints.extend(backfill_checkpoints);
				}
				Err(e) => return CoordinatorResponse::Error(e),
			}
		} else {
			Span::current().record("batch_count", 0usize);
		}

		Span::current().record("elapsed_us", consume_start.elapsed().as_micros() as u64);

		CoordinatorResponse::Success(ConsumeResult {
			pending_writes: combined_pending,
			checkpoints,
		})
	}

	/// Register a flow in the pool actor.
	fn register_flow_in_pool(&self, flow: FlowDag) -> Result<(), String> {
		let (reply_tx, reply_rx) = bounded(1);

		self.pool_ref
			.send(PoolMsg::RegisterFlow {
				flow,
				reply: reply_tx,
			})
			.map_err(|_| "Pool actor stopped".to_string())?;

		match reply_rx.recv() {
			Ok(PoolResponse::RegisterSuccess) => Ok(()),
			Ok(PoolResponse::Success(_)) => Ok(()),
			Ok(PoolResponse::Error(e)) => Err(e),
			Err(_) => Err("Pool actor response error".to_string()),
		}
	}

	/// Submit batches to the pool actor.
	fn submit_to_pool(&self, batches: HashMap<usize, WorkerBatch>) -> Result<PendingWrites, String> {
		let (reply_tx, reply_rx) = bounded(1);

		self.pool_ref
			.send(PoolMsg::Submit {
				batches,
				reply: reply_tx,
			})
			.map_err(|_| "Pool actor stopped".to_string())?;

		match reply_rx.recv() {
			Ok(PoolResponse::Success(pending)) => Ok(pending),
			Ok(PoolResponse::RegisterSuccess) => Err("Unexpected response type".to_string()),
			Ok(PoolResponse::Error(e)) => Err(e),
			Err(_) => Err("Pool actor response error".to_string()),
		}
	}

	/// Submit a batch to a specific worker in the pool.
	fn submit_to_pool_worker(&self, worker_id: usize, batch: WorkerBatch) -> Result<PendingWrites, String> {
		let (reply_tx, reply_rx) = bounded(1);

		self.pool_ref
			.send(PoolMsg::SubmitToWorker {
				worker_id,
				batch,
				reply: reply_tx,
			})
			.map_err(|_| "Pool actor stopped".to_string())?;

		match reply_rx.recv() {
			Ok(PoolResponse::Success(pending)) => Ok(pending),
			Ok(PoolResponse::RegisterSuccess) => Err("Unexpected response type".to_string()),
			Ok(PoolResponse::Error(e)) => Err(e),
			Err(_) => Err("Pool actor response error".to_string()),
		}
	}

	/// Filter CDC changes to only those relevant to a specific flow.
	#[instrument(name = "flow::coordinator_actor::filter_cdc", level = "trace", skip(self, state, changes), fields(
		input = changes.len(),
		output = tracing::field::Empty
	))]
	fn filter_cdc_for_flow(
		&self,
		state: &CoordinatorState,
		flow_id: FlowId,
		changes: &[FlowChange],
	) -> Vec<FlowChange> {
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
		for (view_id, flow_ids) in &dependency_graph.source_views {
			if flow_ids.contains(&flow_id) {
				flow_sources.insert(PrimitiveId::View(*view_id));
			}
		}

		// Filter changes to only those from this flow's sources
		let result: Vec<FlowChange> = changes
			.iter()
			.filter(|change| {
				if let External(source) = change.origin {
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
	#[instrument(name = "flow::coordinator_actor::route_and_group", level = "debug", skip(self, state, changes), fields(
		changes = changes.len(),
		active_flows = tracing::field::Empty,
		batches = tracing::field::Empty,
		elapsed_us = tracing::field::Empty
	))]
	fn route_and_group_changes(
		&self,
		state: &CoordinatorState,
		changes: &[FlowChange],
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

	/// Advance backfilling flows by one chunk each.
	#[instrument(name = "flow::coordinator_actor::advance_backfill", level = "debug", skip(self, state), fields(
		backfilling = tracing::field::Empty,
		processed = tracing::field::Empty,
		elapsed_us = tracing::field::Empty
	))]
	fn advance_backfilling_flows(
		&self,
		state: &mut CoordinatorState,
		current_version: CommitVersion,
		state_version: CommitVersion,
	) -> Result<(PendingWrites, Vec<(FlowId, CommitVersion)>), String> {
		let start = self.clock.instant();
		const BACKFILL_CHUNK_SIZE: u64 = 1_000;

		let backfilling_flows: Vec<_> = state.states.backfilling_flow_ids();
		Span::current().record("backfilling", backfilling_flows.len());

		let mut combined_pending = PendingWrites::new();
		let mut checkpoints = Vec::new();
		let mut processed = 0u32;

		for flow_id in backfilling_flows {
			// Get current checkpoint for this flow
			let from_version = {
				let mut query = self.engine.begin_query().map_err(|e| e.to_string())?;
				CdcCheckpoint::fetch(&mut query, &flow_id).unwrap_or(CommitVersion(0))
			};

			// Check if already caught up
			if from_version >= current_version {
				if let Some(flow_state) = state.states.get_mut(&flow_id) {
					flow_state.activate();
					flow_state.update_checkpoint(current_version);
				}
				info!(flow_id = flow_id.0, "backfill complete, flow now active");
				continue;
			}

			// Calculate chunk range
			let to_version = CommitVersion(min(from_version.0 + BACKFILL_CHUNK_SIZE, current_version.0));

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
				checkpoints.push((flow_id, to_version));
				if let Some(flow_state) = state.states.get_mut(&flow_id) {
					flow_state.update_checkpoint(to_version);
				}
				continue;
			}

			// Convert CDC to flow changes
			let mut chunk_changes = Vec::new();
			for cdc in &batch.items {
				match convert::to_flow_change(&self.engine, &self.catalog, cdc, cdc.version, &self.clock) {
					Ok(changes) => chunk_changes.extend(changes),
					Err(e) => return Err(e.to_string()),
				}
			}

			// Filter to only changes relevant to this flow
			let flow_changes = self.filter_cdc_for_flow(state, flow_id, &chunk_changes);

			if flow_changes.is_empty() {
				// CDC exists but no relevant changes for this flow
				checkpoints.push((flow_id, to_version));
				if let Some(flow_state) = state.states.get_mut(&flow_id) {
					flow_state.update_checkpoint(to_version);
				}
				continue;
			}

			// Create instruction and send to worker
			let instruction = FlowInstruction::new(flow_id, to_version, flow_changes);
			let worker_id = (flow_id.0 as usize) % self.num_workers;

			let mut worker_batch = WorkerBatch::new(state_version);
			worker_batch.add_instruction(instruction);

			let pending_writes = self.submit_to_pool_worker(worker_id, worker_batch)?;

			// Merge pending writes
			for (key, value) in pending_writes.iter_sorted() {
				match value {
					crate::transaction::pending::Pending::Set(v) => {
						combined_pending.insert(key.clone(), v.clone());
					}
					crate::transaction::pending::Pending::Remove => {
						combined_pending.remove(key.clone());
					}
				}
			}

			// Record checkpoint
			checkpoints.push((flow_id, to_version));
			if let Some(flow_state) = state.states.get_mut(&flow_id) {
				flow_state.update_checkpoint(to_version);
			}

			processed += 1;
			debug!(
				flow_id = flow_id.0,
				from = from_version.0,
				to = to_version.0,
				"advanced backfilling flow by one chunk"
			);

			// Check if now caught up
			if to_version >= current_version {
				if let Some(flow_state) = state.states.get_mut(&flow_id) {
					flow_state.activate();
				}
				info!(flow_id = flow_id.0, "backfill complete, flow now active");
			}
		}

		Span::current().record("processed", processed);
		Span::current().record("elapsed_us", start.elapsed().as_micros() as u64);
		Ok((combined_pending, checkpoints))
	}
}

/// Extract new flow IDs from CDC events.
///
/// This is a helper for the wrapper to detect flow registrations
/// so it can load them from the catalog before sending to the actor.
pub fn extract_new_flow_ids(cdcs: &[Cdc]) -> Vec<FlowId> {
	let mut flow_ids = Vec::new();

	for cdc in cdcs {
		for change in &cdc.changes {
			if let Some(kind) = Key::kind(change.key()) {
				if kind == KeyKind::Flow {
					if let CdcChange::Insert {
						key,
						..
					} = &change.change
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
