// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections, mem, sync::Arc};

use reifydb_core::{
	actors::{
		flow::{FlowCoordinatorMessage, FlowPoolMessage, PoolResponse},
		pending::Pending,
	},
	common::CommitVersion,
	interface::{
		cdc::Cdc,
		change::{Change, ChangeOrigin},
	},
};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_runtime::{actor::context::Context, context::clock::Instant};
use reifydb_transaction::multi::lease::VersionLeaseGuard;
use reifydb_value::{Result, reifydb_assertions};
use tracing::{Span, debug, field, instrument};

use super::{
	ConsumeContext, CoordinatorActor, CoordinatorState, PendingConsume, Phase,
	registration::extract_deleted_flow_ids,
};
use crate::error::FlowDispatchError;

#[inline]
fn record_version_range_span(cdcs: &[Cdc]) {
	if let Some(first) = cdcs.first() {
		Span::current().record("version_start", first.version.0);
	}
	if let Some(last) = cdcs.last() {
		Span::current().record("version_end", last.version.0);
	}
}

impl CoordinatorState {
	pub(super) fn set_phase(&mut self, phase: Phase, now: Instant) {
		self.phase_entered_at = if matches!(phase, Phase::Idle) {
			None
		} else {
			Some(now)
		};
		self.phase = phase;
	}

	pub(super) fn stash_consume(&mut self, pending: PendingConsume) {
		self.pending_consume = Some(pending);
	}

	pub(super) fn take_pending_consume(&mut self) -> Option<PendingConsume> {
		if matches!(self.phase, Phase::Idle) {
			self.pending_consume.take()
		} else {
			None
		}
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
	pub(super) fn handle_consume(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		cdcs: Vec<Cdc>,
		current_version: CommitVersion,
		reply: Box<dyn FnOnce(Result<()>) + Send>,
	) {
		if !matches!(state.phase, Phase::Idle) {
			reifydb_assertions! {
				assert!(
					state.pending_consume.is_none(),
					"poll actor must not have more than one consume in flight, yet a second arrived while one was already deferred"
				);
			}
			if state.pending_consume.is_some() {
				(reply)(Err(FlowDispatchError::CoordinatorBusy.into()));
				return;
			}
			state.stash_consume(PendingConsume {
				cdcs,
				current_version,
				reply,
			});
			return;
		}
		record_version_range_span(&cdcs);

		let (state_version, state_lease) = match self.engine.acquire_current_snapshot_lease() {
			Ok(pair) => pair,
			Err(e) => {
				(reply)(Err(e));
				return;
			}
		};

		let consume_ctx = self.build_consume_context(&cdcs, current_version, state_version, state_lease, reply);

		let deleted = extract_deleted_flow_ids(&cdcs);
		let new_flows = match self.discover_and_load_new_flows(state, &cdcs, &deleted) {
			Ok(flows) => flows,
			Err(e) => {
				(consume_ctx.original_reply)(Err(e));
				return;
			}
		};
		self.apply_flow_deletions(state, &deleted);

		self.start_registration_or_submit(state, ctx, consume_ctx, new_flows);
	}

	#[inline]
	fn build_consume_context(
		&self,
		cdcs: &[Cdc],
		current_version: CommitVersion,
		state_version: CommitVersion,
		state_lease: VersionLeaseGuard,
		reply: Box<dyn FnOnce(Result<()>) + Send>,
	) -> ConsumeContext {
		let consume_start = self.clock.instant();
		let latest_version = cdcs.last().map(|c| c.version);
		let all_changes = self.update_tracker_and_collect(cdcs);
		ConsumeContext {
			state_version,
			current_version,
			combined: Pending::new(),
			pending_shapes: Vec::new(),
			checkpoints: Vec::new(),
			positions: Vec::new(),
			checkpoint_deletes: Vec::new(),
			original_reply: reply,
			consume_start,
			all_changes,
			latest_version,
			downstream_flows: collections::HashSet::new(),
			view_changes: Vec::new(),
			state_lease,
		}
	}

	#[inline]
	pub(super) fn update_tracker_and_collect(&self, cdcs: &[Cdc]) -> Vec<Change> {
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

	pub(super) fn start_registration_or_submit(
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
		state.flows_changed = true;
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
			(consume_ctx.original_reply)(Err(FlowDispatchError::PoolActorStopped.into()));
			return;
		}
		state.set_phase(
			Phase::RegisteringFlows {
				flows: new_flows,
				ctx: consume_ctx,
			},
			self.clock.instant(),
		);
	}

	pub(super) fn drain_pending_consume(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
	) {
		if let Some(pending) = state.take_pending_consume() {
			self.handle_consume(state, ctx, pending.cdcs, pending.current_version, pending.reply);
		}
	}

	#[inline]
	pub(super) fn continue_submitting(
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
				(consume_ctx.original_reply)(Err(e));
				return;
			}
		}

		if let Some(to_version) = consume_ctx.latest_version {
			for flow_id in state.states.active_flow_ids() {
				consume_ctx.positions.push((flow_id, to_version));
			}
		}

		self.proceed_to_backfill(state, ctx, consume_ctx);
	}

	pub(super) fn proceed_to_submit(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		mut consume_ctx: ConsumeContext,
	) {
		if state.flows_changed {
			state.flows_changed = false;
			self.rebuild_routing_cache(state);
			let optimal = self.compute_flow_assignments(state);
			if optimal != state.flow_assignments {
				self.rebalance_flows(state, ctx, consume_ctx);
				return;
			}
		}

		if let Some(to_version) = consume_ctx.latest_version
			&& !state.cached_active.is_empty()
			&& !consume_ctx.all_changes.is_empty()
		{
			self.submit_broadcast(state, ctx, to_version, consume_ctx);
			return;
		}

		if let Some(to_version) = consume_ctx.latest_version {
			for flow_id in state.states.active_flow_ids() {
				if !consume_ctx.downstream_flows.contains(&flow_id) {
					consume_ctx.positions.push((flow_id, to_version));
				}
			}
		}

		self.proceed_to_backfill(state, ctx, consume_ctx);
	}

	#[inline]
	fn submit_broadcast(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		to_version: CommitVersion,
		mut consume_ctx: ConsumeContext,
	) {
		reifydb_assertions! {
			assert_eq!(
				state.states.active_flow_ids(),
				*state.cached_active,
				"routing cache is stale: the active flow set changed without flows_changed being set"
			);
		}

		let changes = Arc::new(mem::take(&mut consume_ctx.all_changes));
		let self_ref = ctx.self_ref().clone();
		let callback: Box<dyn FnOnce(PoolResponse) + Send> = Box::new(move |resp| {
			let _ = self_ref.send(FlowCoordinatorMessage::PoolReply(resp));
		});

		if self.pool
			.send(FlowPoolMessage::Broadcast {
				state_version: consume_ctx.state_version,
				to_version,
				changes,
				index: state.cached_routing_index.clone(),
				active: state.cached_active.clone(),
				reply: callback,
			})
			.is_err()
		{
			(consume_ctx.original_reply)(Err(FlowDispatchError::PoolActorStopped.into()));
			return;
		}

		state.set_phase(
			Phase::SubmittingBatches {
				ctx: consume_ctx,
			},
			self.clock.instant(),
		);
	}
}

#[cfg(test)]
mod tests {
	use std::{
		collections,
		collections::BTreeMap,
		sync::{
			Arc,
			atomic::{AtomicUsize, Ordering},
		},
	};

	use reifydb_rql::flow::analyzer::FlowGraphAnalyzer;
	use reifydb_runtime::context::clock::{Clock, MockClock};

	use super::*;
	use crate::deferred::state::FlowStates;

	fn idle_state() -> CoordinatorState {
		CoordinatorState {
			states: FlowStates::new(),
			analyzer: FlowGraphAnalyzer::new(),
			phase: Phase::Idle,
			phase_entered_at: None,
			pending_consume: None,
			tick_schedules: BTreeMap::new(),
			flow_assignments: BTreeMap::new(),
			flows_changed: false,
			cached_active: Arc::new(Vec::new()),
			cached_routing_index: Arc::new(collections::HashMap::new()),
		}
	}

	fn now() -> Instant {
		Clock::Mock(MockClock::from_millis(1000)).instant()
	}

	fn counting_reply(counter: &Arc<AtomicUsize>) -> Box<dyn FnOnce(Result<()>) + Send> {
		let counter = counter.clone();
		Box::new(move |_result| {
			counter.fetch_add(1, Ordering::SeqCst);
		})
	}

	// A consume that arrives while the coordinator is busy must be stashed, not
	// rejected, and its reply must stay un-fired until the consume is replayed.
	#[test]
	fn busy_consume_is_deferred_then_replayed_exactly_once() {
		let mut state = idle_state();
		state.set_phase(Phase::BootstrapRebalancing, now());

		let counter = Arc::new(AtomicUsize::new(0));
		state.stash_consume(PendingConsume {
			cdcs: Vec::new(),
			current_version: CommitVersion(0),
			reply: counting_reply(&counter),
		});

		// While still busy the reply has not fired and the slot is not drainable.
		assert_eq!(counter.load(Ordering::SeqCst), 0);
		assert!(state.take_pending_consume().is_none());

		// Returning to Idle makes the deferred consume drainable; replaying fires the reply once.
		state.set_phase(Phase::Idle, now());
		let pending = state.take_pending_consume().expect("deferred consume must be drainable once idle");
		(pending.reply)(Ok(()));
		assert_eq!(counter.load(Ordering::SeqCst), 1);

		// The slot is empty after draining.
		assert!(state.take_pending_consume().is_none());
	}

	// The watchdog reads phase_entered_at; it must be stamped for every non-Idle
	// phase (the bug was that only the Tick path stamped it) and cleared on Idle.
	#[test]
	fn set_phase_stamps_entered_at_for_non_idle_phases() {
		let mut state = idle_state();
		assert!(state.phase_entered_at.is_none());

		state.set_phase(Phase::BootstrapRebalancing, now());
		assert!(state.phase_entered_at.is_some());

		state.set_phase(Phase::Idle, now());
		assert!(state.phase_entered_at.is_none());
	}
}
