// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowId};
use reifydb_value::{Result, value::datetime::DateTime};
use tracing::instrument;

use crate::{engine::FlowEngineInner, transaction::DepFlowTransaction};

impl FlowEngineInner {
	#[instrument(name = "flow::engine::process_tick", level = "debug", skip(self, txn), fields(
		flow_id = ?flow_id,
		timestamp = %timestamp,
		checkpoint = checkpoint.0
	))]
	pub fn process_tick(
		&self,
		txn: &mut DepFlowTransaction,
		flow_id: FlowId,
		timestamp: DateTime,
		checkpoint: CommitVersion,
	) -> Result<()> {
		let flow = match self.flows.get(&flow_id) {
			Some(f) => f.clone(),
			None => return Ok(()),
		};

		let topo = flow.topological_order()?;

		self.dispatch_due_timers(txn, &flow, checkpoint, &topo)?;
		Ok(())
	}
}
