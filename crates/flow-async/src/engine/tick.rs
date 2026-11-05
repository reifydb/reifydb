// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	flow::dag::FlowDag,
	interface::{
		catalog::flow::{FlowId, OperatorId},
		change::Change,
	},
};
use reifydb_flow::txn::Intern;
use reifydb_value::Result;
use tracing::instrument;

use crate::{engine::FlowEngineInner, operator::host::TxnHostContext, transaction::FlowTransaction};

impl FlowEngineInner {
	#[instrument(name = "flow::engine::process_tick", level = "debug", skip(self, txn), fields(
		flow_id = ?flow_id,
		checkpoint = checkpoint.0
	))]
	pub fn process_tick<T: FlowTransaction + Intern>(
		&mut self,
		txn: &mut T,
		flow_id: FlowId,
		checkpoint: CommitVersion,
	) -> Result<()> {
		let flow = match self.flows.get(&flow_id) {
			Some(f) => f.clone(),
			None => return Ok(()),
		};

		let topo = flow.topological_order();

		self.dispatch_operator_ticks(txn, &flow, ChangeVersion::from(checkpoint), topo)?;
		self.dispatch_due_timers(txn, &flow, ChangeVersion::from(checkpoint), topo)?;
		Ok(())
	}

	fn dispatch_operator_ticks<T: FlowTransaction + Intern>(
		&mut self,
		txn: &mut T,
		flow: &FlowDag,
		version: ChangeVersion,
		topo: &[OperatorId],
	) -> Result<()> {
		let mut pending: HashMap<OperatorId, Vec<Change>> = HashMap::new();
		for operator_id in topo {
			let Some(operator) = self.operators.get_mut(&(flow.id, *operator_id)) else {
				continue;
			};
			let mut host = TxnHostContext::with_retention(txn, operator.id(), operator.retention());
			let Some(result) = operator.on_tick(&mut host)? else {
				continue;
			};
			if result.diffs.is_empty() {
				continue;
			}
			let combined = Change::from_flow(*operator_id, version, result.diffs, result.changed_at);
			let Some(node) = flow.get_operator(operator_id) else {
				continue;
			};
			for child_id in &node.outputs {
				pending.entry(*child_id).or_default().push(combined.clone());
			}
		}
		if !pending.is_empty() {
			self.run_topology(txn, flow, pending, topo)?;
		}
		Ok(())
	}
}
