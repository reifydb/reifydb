// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	actors::pending::PendingWrite,
	event::row::OperatorRowsExpiredEvent,
	interface::{
		catalog::flow::{FlowId, FlowNodeId},
		change::Change,
	},
	key::{EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey, flow_node_state::FlowNodeStateKey},
};
use reifydb_sdk::operator::Tick;
use reifydb_value::{Result, value::datetime::DateTime};
use tracing::instrument;

use crate::{engine::FlowEngineInner, operator::Operators, transaction::FlowTransaction};

impl FlowEngineInner {
	#[instrument(name = "flow::engine::process_tick", level = "debug", skip(self, txn), fields(
		flow_id = ?flow_id,
		timestamp = %timestamp
	))]
	pub fn process_tick(&self, txn: &mut FlowTransaction, flow_id: FlowId, timestamp: DateTime) -> Result<()> {
		let flow = match self.flows.get(&flow_id) {
			Some(f) => Arc::clone(f),
			None => return Ok(()),
		};

		let mut pending: HashMap<FlowNodeId, Vec<Change>> = HashMap::new();
		for node_id in flow.topological_order()? {
			let node = match flow.get_node(&node_id) {
				Some(n) => n.clone(),
				None => continue,
			};

			if let Some(inbox) = pending.remove(&node_id).filter(|v| !v.is_empty()) {
				let combined_output = self.dispatch_node(txn, &node, inbox)?;
				if !combined_output.diffs.is_empty() {
					for child_id in &node.outputs {
						pending.entry(*child_id).or_default().push(combined_output.clone());
					}
				}
			}

			let operator = match self.operators.get(&node_id) {
				Some(op) => op.clone(),
				None => continue,
			};
			let interval = match operator.ticks() {
				Some(interval) => interval,
				None => continue,
			};
			if matches!(&*operator, Operators::Custom(_) | Operators::Apply(_))
				&& !self.operator_due(node_id, timestamp.to_nanos(), interval)
			{
				continue;
			}
			if let Some(tick_emission) = operator.tick(
				txn,
				Tick {
					now: timestamp,
				},
			)? && !tick_emission.diffs.is_empty()
			{
				for child_id in &node.outputs {
					pending.entry(*child_id).or_default().push(tick_emission.clone());
				}
			}
		}

		self.emit_operator_drop_metrics(txn);
		Ok(())
	}

	fn emit_operator_drop_metrics(&self, txn: &FlowTransaction) {
		let mut per_node: HashMap<FlowNodeId, u64> = HashMap::new();
		for (key, write) in txn.pending().iter_sorted() {
			if !matches!(write, PendingWrite::Drop) {
				continue;
			}
			let node = FlowNodeStateKey::decode(key)
				.map(|k| k.node)
				.or_else(|| FlowNodeInternalStateKey::decode(key).map(|k| k.node));
			if let Some(node) = node {
				*per_node.entry(node).or_default() += 1;
			}
		}

		if per_node.is_empty() {
			return;
		}

		let rows: u64 = per_node.values().copied().sum();
		self.event_bus.emit(OperatorRowsExpiredEvent::new(
			per_node.len() as u64,
			0,
			rows,
			rows,
			per_node.clone(),
			per_node,
		));
	}
}
