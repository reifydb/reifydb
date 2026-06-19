// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::{FlowId, FlowNodeId},
		change::{Change, ChangeOrigin},
	},
};
use reifydb_sub_flow::{engine::FlowEngineInner, transaction::FlowTransaction};
use reifydb_transaction::multi::transaction::read::MultiReadTransaction;
use reifydb_value::Result;

use super::{SubscriptionFlowState, SubscriptionWorkerActor, SubscriptionWorkerState};

impl SubscriptionWorkerActor {
	pub(super) fn process_dispatch(
		&self,
		state: &mut SubscriptionWorkerState,
		to_version: CommitVersion,
		changes: &[Change],
	) -> Result<()> {
		if state.flows.is_empty() || !state.flow_engine.has_sources() {
			return Ok(());
		}

		let lease = self.engine.multi().acquire_version_lease(to_version)?;
		let base_query = self.engine.multi().begin_query_at_version(&lease)?;

		let SubscriptionWorkerState {
			flow_engine,
			flows,
		} = state;

		for change in changes {
			let source_shape = match &change.origin {
				ChangeOrigin::Shape(s) => *s,
				ChangeOrigin::Flow(_) => continue,
			};
			let Some(flow_entries) = flow_engine.flows_for_source_shape(source_shape) else {
				continue;
			};
			for (flow_id, node_id) in flow_entries {
				let Some(flow_state) = flows.get_mut(&flow_id) else {
					continue;
				};
				self.evaluate_flow(flow_engine, flow_state, &base_query, change, flow_id, node_id);
			}
		}

		drop(base_query);
		drop(lease);
		self.delivery.commit_batch();
		Ok(())
	}

	#[inline]
	fn evaluate_flow(
		&self,
		flow_engine: &FlowEngineInner,
		flow_state: &mut SubscriptionFlowState,
		base_query: &MultiReadTransaction,
		change: &Change,
		flow_id: FlowId,
		node_id: FlowNodeId,
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

		let flow_change = Change::from_flow(node_id, change.version, change.diffs.clone(), change.changed_at);
		if flow_engine.process(&mut txn, flow_change, flow_id).is_ok() {
			txn.merge_state();
		}
		flow_state.keyed_state = txn.take_state();
		flow_state.operator_states = txn.drain_operator_states();
	}
}
