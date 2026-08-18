// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::state::store::TimerKind;
use reifydb_core::interface::{
	catalog::flow::{FlowId, OperatorId},
	change::Change,
};
use reifydb_rql::flow::operator::FlowNode;
use reifydb_sdk::flow::operator::timer::Timer as Tick;
use reifydb_value::{Result, value::datetime::DateTime};
use tracing::instrument;

use crate::flow::{engine::FlowEngineInner, operator::Operators, transaction::FlowTransaction};

impl FlowEngineInner {
	#[instrument(name = "flow::engine::process_tick", level = "debug", skip(self, txn), fields(
		flow_id = ?flow_id,
		timestamp = %timestamp
	))]
	pub fn process_tick(&self, txn: &mut FlowTransaction, flow_id: FlowId, timestamp: DateTime) -> Result<()> {
		let flow = match self.flows.get(&flow_id) {
			Some(f) => f.clone(),
			None => return Ok(()),
		};

		let mut pending: HashMap<OperatorId, Vec<Change>> = HashMap::new();
		for node_id in flow.topological_order()? {
			let node = match flow.get_operator(&node_id) {
				Some(n) => n.clone(),
				None => continue,
			};

			self.dispatch_inbox(txn, &node, node_id, &mut pending)?;
			self.fire_operator_tick(txn, &node, node_id, timestamp, &mut pending)?;
		}

		Ok(())
	}

	#[inline]
	fn dispatch_inbox(
		&self,
		txn: &mut FlowTransaction,
		node: &FlowNode,
		node_id: OperatorId,
		pending: &mut HashMap<OperatorId, Vec<Change>>,
	) -> Result<()> {
		let Some(inbox) = pending.remove(&node_id).filter(|v| !v.is_empty()) else {
			return Ok(());
		};
		let combined_output = self.dispatch_node(txn, node, inbox)?;
		if !combined_output.diffs.is_empty() {
			for child_id in &node.outputs {
				pending.entry(*child_id).or_default().push(combined_output.clone());
			}
		}
		Ok(())
	}

	#[inline]
	fn fire_operator_tick(
		&self,
		txn: &mut FlowTransaction,
		node: &FlowNode,
		node_id: OperatorId,
		timestamp: DateTime,
		pending: &mut HashMap<OperatorId, Vec<Change>>,
	) -> Result<()> {
		let operator = match self.operators.get(&node_id) {
			Some(op) => op.clone(),
			None => return Ok(()),
		};
		let interval = match operator.ticks() {
			Some(interval) => interval,
			None => return Ok(()),
		};
		if matches!(&*operator, Operators::Custom(_) | Operators::Apply(_))
			&& !self.operator_due(node_id, timestamp.to_nanos(), interval)
		{
			return Ok(());
		}
		if let Some(tick_emission) = operator.tick(
			txn,
			Tick {
				due: timestamp,
				kind: TimerKind::Maintenance,
				key: &[],
			},
		)? && !tick_emission.diffs.is_empty()
		{
			for child_id in &node.outputs {
				pending.entry(*child_id).or_default().push(tick_emission.clone());
			}
		}
		Ok(())
	}
}
