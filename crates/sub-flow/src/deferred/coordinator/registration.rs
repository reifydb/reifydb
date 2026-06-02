// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	actors::flow::{FlowCoordinatorMessage, FlowPoolMessage, PoolResponse},
	interface::{
		catalog::flow::FlowId,
		cdc::{Cdc, SystemChange},
	},
	key::{Key, kind::KeyKind},
};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_runtime::actor::context::Context;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, value::identity::IdentityId};
use tracing::{debug, warn};

use super::{ConsumeContext, CoordinatorActor, CoordinatorState, Phase, coordinator_error};

impl CoordinatorActor {
	pub(super) fn discover_and_load_new_flows(
		&self,
		state: &mut CoordinatorState,
		cdcs: &[Cdc],
	) -> Result<Vec<FlowDag>> {
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
						state.flows_changed = true;

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

	#[inline]
	pub(super) fn continue_registering(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		response: PoolResponse,
		remaining_flows: Vec<FlowDag>,
		mut consume_ctx: ConsumeContext,
	) {
		if let Err(e) = absorb_register_reply(&mut consume_ctx, response) {
			(consume_ctx.original_reply)(Err(e));
			return;
		}

		if remaining_flows.is_empty() {
			self.rebalance_flows(state, ctx, consume_ctx);
			return;
		}

		self.register_next_flow(state, ctx, remaining_flows, consume_ctx);
	}

	#[inline]
	fn register_next_flow(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		mut remaining_flows: Vec<FlowDag>,
		consume_ctx: ConsumeContext,
	) {
		let flow = remaining_flows.remove(0);
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
			(consume_ctx.original_reply)(coordinator_error("Pool actor stopped"));
			return;
		}

		state.set_phase(
			Phase::RegisteringFlows {
				flows: remaining_flows,
				ctx: consume_ctx,
			},
			self.clock.instant(),
		);
	}
}

#[inline]
fn absorb_register_reply(consume_ctx: &mut ConsumeContext, response: PoolResponse) -> Result<()> {
	match response {
		PoolResponse::RegisterSuccess => Ok(()),
		PoolResponse::Success {
			pending_shapes,
			..
		} => {
			consume_ctx.pending_shapes.extend(pending_shapes);
			Ok(())
		}
		PoolResponse::Error(e) => coordinator_error(e),
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
