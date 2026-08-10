// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::{FlowId, OperatorId},
		change::{Change, ChangeOrigin},
	},
};
use reifydb_flow::transaction::FlowTransaction;
use reifydb_sub_flow::engine::FlowEngineInner;
use reifydb_transaction::{error::TransactionError, multi::transaction::read::MultiReadTransaction};
use reifydb_value::Result;
use tracing::warn;

use super::{SubscriptionFlowState, SubscriptionWorkerActor, SubscriptionWorkerState};

impl SubscriptionWorkerActor {
	pub(super) fn process_dispatch(
		&self,
		state: &mut SubscriptionWorkerState,
		to_version: CommitVersion,
		changes: &[Change],
	) -> Result<()> {
		if state.flows.is_empty() || !state.flow_engine.has_sources() {
			state.carry_lease = None;
			return Ok(());
		}

		let min_needed = min_version_the_flows_will_read(state, changes);
		let protect = match min_needed {
			Some(min_needed) => {
				Some(self.engine.multi().acquire_version_lease(min_needed).map_err(|e| {
					if TransactionError::is_snapshot_evicted(&e) {
						TransactionError::ConsumerOvertaken {
							version: min_needed,
							cutoff: self.engine.query_done_until(),
						}
						.into()
					} else {
						e
					}
				})?)
			}
			None => None,
		};
		match self.engine.multi().acquire_version_lease(to_version) {
			Ok(lease) if protect.is_some() => {
				let base_query = self.engine.multi().begin_query_at_version(&lease)?;
				state.carry_lease = Some(lease);
				self.evaluate_batch(state, &base_query, changes);
				drop(base_query);
				drop(protect);
				self.delivery.commit_batch();
				return Ok(());
			}
			Ok(lease) => {
				state.carry_lease = Some(lease);
			}
			Err(_) if protect.is_none() => {}
			Err(e) => return Err(e),
		}
		drop(protect);
		self.delivery.commit_batch();
		Ok(())
	}

	fn evaluate_batch(
		&self,
		state: &mut SubscriptionWorkerState,
		base_query: &MultiReadTransaction,
		changes: &[Change],
	) {
		let SubscriptionWorkerState {
			flow_engine,
			flows,
			..
		} = state;

		for change in changes {
			let source_shape = match &change.origin {
				ChangeOrigin::Object(s) => *s,
				ChangeOrigin::Flow(_) => continue,
			};
			let Some(flow_entries) = flow_engine.flows_for_source_object(source_shape) else {
				continue;
			};
			for (flow_id, operator_id) in flow_entries {
				let Some(flow_state) = flows.get_mut(&flow_id) else {
					continue;
				};
				self.evaluate_flow(flow_engine, flow_state, base_query, change, flow_id, operator_id);
			}
		}
	}

	#[inline]
	fn evaluate_flow(
		&self,
		flow_engine: &FlowEngineInner,
		flow_state: &mut SubscriptionFlowState,
		base_query: &MultiReadTransaction,
		change: &Change,
		flow_id: FlowId,
		operator_id: OperatorId,
	) {
		if let Some(gate) = flow_state.gate
			&& change.version <= gate
		{
			return;
		}

		let keyed = mem::take(&mut flow_state.keyed_state);
		let operators = mem::take(&mut flow_state.operator_states);

		let mut query = base_query.clone();
		query.read_as_of_version_inclusive(change.version);

		let mut txn = FlowTransaction::ephemeral(
			change.version,
			query,
			self.engine.single_owned(),
			self.catalog.clone(),
			keyed,
			flow_engine.clock().clone(),
		);
		txn.install_operator_states(operators);

		let flow_change =
			Change::from_flow(operator_id, change.version, change.diffs.clone(), change.changed_at);
		match flow_engine.process(&mut txn, flow_change, flow_id) {
			Ok(()) => txn.merge_state(),
			Err(e) => {
				warn!(flow_id = flow_id.0, error = %e, "subscription flow change processing failed; change dropped");
			}
		}
		flow_state.keyed_state = txn.take_state();
		flow_state.operator_states = txn.drain_operator_states();
	}
}

fn min_version_the_flows_will_read(state: &SubscriptionWorkerState, changes: &[Change]) -> Option<CommitVersion> {
	let mut min_needed: Option<CommitVersion> = None;
	for change in changes {
		let source_shape = match &change.origin {
			ChangeOrigin::Object(s) => *s,
			ChangeOrigin::Flow(_) => continue,
		};
		let Some(flow_entries) = state.flow_engine.flows_for_source_object(source_shape) else {
			continue;
		};
		let read_by_any_flow = flow_entries.iter().any(|(flow_id, _)| {
			state.flows.get(flow_id).is_some_and(|fs| fs.gate.is_none_or(|gate| change.version > gate))
		});
		if read_by_any_flow {
			min_needed = Some(min_needed.map_or(change.version, |m| m.min(change.version)));
		}
	}
	min_needed
}
