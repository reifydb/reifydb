// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	actors::pending::PendingWrite,
	common::CommitVersion,
	event::row::OperatorRowsExpiredEvent,
	interface::{
		catalog::flow::{FlowId, FlowNodeId},
		change::Change,
	},
	key::{EncodableKey, operator_state::OperatorStateKey},
};
use reifydb_flow::transaction::FlowTransaction;
use reifydb_rql::flow::node::FlowNode;
use reifydb_value::{Result, value::datetime::DateTime};
use tracing::instrument;

use crate::{engine::FlowEngineInner, execution::reclaim::ReclaimBudget};

impl FlowEngineInner {
	#[instrument(name = "flow::engine::process_tick", level = "debug", skip(self, txn), fields(
		flow_id = ?flow_id,
		timestamp = %timestamp,
		checkpoint = checkpoint.0
	))]
	pub fn process_tick(
		&self,
		txn: &mut FlowTransaction,
		flow_id: FlowId,
		timestamp: DateTime,
		checkpoint: CommitVersion,
	) -> Result<()> {
		let flow = match self.flows.get(&flow_id) {
			Some(f) => f.clone(),
			None => return Ok(()),
		};

		let topo = flow.topological_order()?;
		let mut pending: HashMap<FlowNodeId, Vec<Change>> = HashMap::new();
		for node_id in topo.iter().copied() {
			let node = match flow.get_node(&node_id) {
				Some(n) => n.clone(),
				None => continue,
			};

			self.dispatch_inbox(txn, &node, node_id, &mut pending)?;
		}

		self.dispatch_due_timers(txn, &flow, checkpoint, &topo)?;

		self.emit_operator_expiry_metrics(txn);
		self.reclaim_flow(txn, flow_id, checkpoint, ReclaimBudget::from_config(&self.catalog))?;
		Ok(())
	}

	#[inline]
	fn dispatch_inbox(
		&self,
		txn: &mut FlowTransaction,
		node: &FlowNode,
		node_id: FlowNodeId,
		pending: &mut HashMap<FlowNodeId, Vec<Change>>,
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
	fn emit_operator_expiry_metrics(&self, txn: &FlowTransaction) {
		let mut per_node: HashMap<FlowNodeId, u64> = HashMap::new();
		for (key, write) in txn.pending().iter_sorted() {
			if !matches!(write, PendingWrite::Remove { .. }) {
				continue;
			}
			let node = OperatorStateKey::decode(key)
				.map(|k| k.node)
				.or_else(|| OperatorStateKey::decode(key).map(|k| k.node));
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
