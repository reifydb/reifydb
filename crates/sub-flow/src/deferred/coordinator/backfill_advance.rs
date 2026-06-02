// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_cdc::consume::checkpoint::CdcCheckpoint;
use reifydb_core::{
	actors::{
		flow::{FlowCoordinatorMessage, FlowInstruction, FlowPoolMessage, PoolResponse, WorkerBatch},
		pending::PendingWrite,
	},
	common::CommitVersion,
	interface::{catalog::flow::FlowId, cdc::CdcBatch, change::Change},
};
use reifydb_runtime::actor::context::Context;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, value::identity::IdentityId};
use tracing::{debug, info, warn};

use super::{
	ConsumeContext, CoordinatorActor, CoordinatorState, Phase, backfill_filter::collect_chunk_changes,
	coordinator_error,
};

impl CoordinatorActor {
	#[inline]
	pub(super) fn continue_advancing_backfill(
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
						PendingWrite::Drop => {
							consume_ctx.combined.drop_key(key.clone());
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

	pub(super) fn proceed_to_backfill(
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
		collect_downstream_flows(state, &backfilling_flows, &mut consume_ctx);

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
				consume_ctx.checkpoint_deletes.push(flow_id);
				consume_ctx.positions.push((flow_id, consume_ctx.current_version));
				continue;
			}

			let batch = self.read_backfill_chunk(
				from_version,
				consume_ctx.current_version,
				BACKFILL_CHUNK_SIZE,
			);

			if batch.items.is_empty() {
				let target = consume_ctx.current_version;
				self.record_chunk_checkpoint(
					state,
					&mut consume_ctx,
					flow_id,
					target,
					"backfill complete: no CDC up to current version (version gap skipped), flow now active",
				);
				continue;
			}

			let to_version = batch.items.iter().map(|cdc| cdc.version).max().unwrap_or(from_version);

			let chunk_changes = collect_chunk_changes(&batch);
			let flow_changes = self.filter_cdc_for_flow(state, flow_id, &chunk_changes);

			if flow_changes.is_empty() {
				self.record_chunk_checkpoint(
					state,
					&mut consume_ctx,
					flow_id,
					to_version,
					"backfill advanced past no-op chunk, flow now active",
				);
				continue;
			}

			if !self.submit_backfill_chunk(state, ctx, flow_id, to_version, flow_changes, &consume_ctx) {
				(consume_ctx.original_reply)(coordinator_error("Pool actor stopped"));
				return;
			}

			self.record_advanced_chunk(state, &mut consume_ctx, flow_id, from_version, to_version);

			state.set_phase(
				Phase::AdvancingBackfill {
					flows,
					ctx: consume_ctx,
				},
				self.clock.instant(),
			);
			return;
		}

		self.finish_consume(state, consume_ctx);
	}

	#[inline]
	fn record_advanced_chunk(
		&self,
		state: &mut CoordinatorState,
		consume_ctx: &mut ConsumeContext,
		flow_id: FlowId,
		from_version: CommitVersion,
		to_version: CommitVersion,
	) {
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
			state.flows_changed = true;
			consume_ctx.checkpoint_deletes.push(flow_id);
			info!(flow_id = flow_id.0, "backfill complete, flow now active");
		}
	}

	#[inline]
	fn fetch_flow_checkpoint(&self, flow_id: FlowId) -> Result<CommitVersion> {
		let mut query = self.engine.begin_query(IdentityId::system())?;
		Ok(CdcCheckpoint::fetch(&mut Transaction::Query(&mut query), &flow_id).unwrap_or(CommitVersion(0)))
	}

	#[inline]
	pub(super) fn fetch_coordinator_checkpoint(&self) -> Result<CommitVersion> {
		let mut query = self.engine.begin_query(IdentityId::system())?;
		Ok(CdcCheckpoint::fetch(&mut Transaction::Query(&mut query), &self.consumer_id)
			.unwrap_or(CommitVersion(0)))
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
		state.flows_changed = true;
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
		let activated = to_version >= consume_ctx.current_version;
		if let Some(flow_state) = state.states.get_mut(&flow_id) {
			flow_state.update_checkpoint(to_version);
			if activated {
				flow_state.activate();
			}
		}
		if activated {
			state.flows_changed = true;
			consume_ctx.checkpoint_deletes.push(flow_id);
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
}

#[inline]
fn collect_downstream_flows(state: &CoordinatorState, backfilling_flows: &[FlowId], consume_ctx: &mut ConsumeContext) {
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
}
