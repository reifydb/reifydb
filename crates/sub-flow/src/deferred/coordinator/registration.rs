// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	actors::flow::{FlowCoordinatorMessage, FlowPoolMessage, PoolResponse},
	common::CommitVersion,
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
		deleted: &[FlowId],
	) -> Result<Vec<FlowDag>> {
		let new_flows_at_version = extract_new_flows(cdcs);
		let mut new_flows = Vec::new();
		if new_flows_at_version.is_empty() {
			return Ok(new_flows);
		}

		for (flow_id, version) in new_flows_at_version {
			if deleted.contains(&flow_id) {
				self.catalog.remove(flow_id);
				debug!(flow_id = flow_id.0, "skipping flow created and dropped in the same batch");
				continue;
			}
			let lease = match self.engine.acquire_version_lease(version) {
				Ok(lease) => lease,

				Err(e) if e.0.code == "TXN_012" => match self.engine.acquire_current_snapshot_lease() {
					Ok((current, lease)) => {
						debug!(
							flow_id = flow_id.0,
							version = version.0,
							current = current.0,
							"creation version evicted, loading new flow at current snapshot"
						);
						lease
					}
					Err(e) => {
						warn!(
							flow_id = flow_id.0,
							version = version.0,
							error = %e,
							"failed to lease current snapshot for new flow, skipping"
						);
						continue;
					}
				},
				Err(e) => {
					warn!(
						flow_id = flow_id.0,
						version = version.0,
						error = %e,
						"failed to lease creation version for new flow, skipping"
					);
					continue;
				}
			};
			let mut query = self.engine.begin_query_at_version(&lease, IdentityId::system())?;
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

	pub(super) fn apply_flow_deletions(&self, state: &mut CoordinatorState, deleted: &[FlowId]) {
		for &flow_id in deleted {
			self.catalog.remove(flow_id);

			let was_tracked = state.states.remove(&flow_id);
			state.tick_schedules.remove(&flow_id);
			state.analyzer.remove(flow_id);

			if was_tracked {
				state.flows_changed = true;
				debug!(flow_id = flow_id.0, "deregistered dropped flow");
			}
		}
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

pub fn extract_new_flows(cdcs: &[Cdc]) -> Vec<(FlowId, CommitVersion)> {
	let mut flows = Vec::new();

	for cdc in cdcs {
		for change in &cdc.system_changes {
			if let Some(kind) = Key::kind(change.key())
				&& kind == KeyKind::Flow && let SystemChange::Insert {
				key,
				..
			} = change && let Some(Key::Flow(flow_key)) = Key::decode(key)
			{
				flows.push((flow_key.flow, cdc.version));
			}
		}
	}

	flows
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

pub fn extract_deleted_flow_ids(cdcs: &[Cdc]) -> Vec<FlowId> {
	let mut flow_ids = Vec::new();

	for cdc in cdcs {
		for change in &cdc.system_changes {
			if let Some(kind) = Key::kind(change.key())
				&& kind == KeyKind::Flow && let SystemChange::Delete {
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
