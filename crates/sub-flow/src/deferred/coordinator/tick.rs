// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{
	actors::flow::{FlowCoordinatorMessage, FlowPoolMessage, PoolResponse},
	interface::catalog::flow::FlowId,
};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_runtime::actor::context::Context;
use reifydb_value::value::datetime::DateTime;
use tracing::{debug, warn};

use super::{CoordinatorActor, CoordinatorState, Phase, TickSchedule};

impl CoordinatorActor {
	pub(super) fn maybe_register_tick_schedule(&self, state: &mut CoordinatorState, flow: &FlowDag) {
		if flow.ticks() {
			let tick = self.flow_tick();
			state.tick_schedules.insert(
				flow.id(),
				TickSchedule {
					tick,
					last_tick: self.clock.instant(),
				},
			);
			debug!(
				flow_id = flow.id().0,
				tick_nanos = tick.to_std().as_nanos(),
				"registered tick schedule for flow"
			);
		}
	}

	pub(super) fn handle_tick(&self, state: &mut CoordinatorState, ctx: &Context<FlowCoordinatorMessage>) {
		let now = self.clock.instant();
		let timestamp = DateTime::from_timestamp_millis(self.clock.now_millis()).unwrap();

		let mut due_flows: BTreeMap<usize, Vec<FlowId>> = BTreeMap::new();

		for (flow_id, schedule) in &mut state.tick_schedules {
			let tick_std = schedule.tick.to_std();
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

		let (state_version, state_lease) = match self.engine.acquire_current_snapshot_lease() {
			Ok(pair) => pair,
			Err(e) => {
				warn!(error = %e, "failed to acquire snapshot lease for tick");
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

		state.set_phase(
			Phase::Ticking {
				state_lease,
			},
			self.clock.instant(),
		);
	}

	#[inline]
	pub(super) fn continue_ticking(&self, response: PoolResponse) {
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
}
