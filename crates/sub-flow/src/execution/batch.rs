// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	sync::Arc,
};

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::{FlowId, FlowNodeId},
		change::Change,
	},
};
use reifydb_value::Result;
use tracing::{Span, field, instrument};

use crate::{engine::FlowEngineInner, transaction::FlowTransaction};

impl FlowEngineInner {
	#[instrument(name = "flow::engine::process", level = "debug", skip(self, txn, change), fields(
		flow_id = ?flow_id,
		origin = ?change.origin,
		version = change.version.0,
		diff_count = change.diffs.len(),
		row_count = change.row_count(),
		nodes_processed = field::Empty
	))]
	pub fn process(&self, txn: &mut FlowTransaction, change: Change, flow_id: FlowId) -> Result<()> {
		self.process_batch(txn, vec![change], flow_id)
	}

	#[instrument(name = "flow::engine::process_batch", level = "debug", skip(self, txn, changes), fields(
		flow_id = ?flow_id,
		batch_change_count = changes.len(),
		batch_row_count = changes.iter().map(Change::row_count).sum::<usize>(),
		version_count = field::Empty,
		nodes_processed = field::Empty
	))]
	pub fn process_batch(&self, txn: &mut FlowTransaction, changes: Vec<Change>, flow_id: FlowId) -> Result<()> {
		let flow = match self.flows.get(&flow_id) {
			Some(f) => Arc::clone(f),
			None => return Ok(()),
		};

		let mut by_version: BTreeMap<CommitVersion, Vec<Change>> = BTreeMap::new();
		for change in changes {
			by_version.entry(change.version).or_default().push(change);
		}
		Span::current().record("version_count", by_version.len());

		let topo = flow.topological_order()?;
		let mut nodes_processed = 0u32;

		for (_, version_changes) in by_version {
			let mut pending: HashMap<FlowNodeId, Vec<Change>> = HashMap::new();
			for change in version_changes {
				self.seed_entry_nodes(&flow, flow_id, change, &mut pending);
			}

			for node_id in &topo {
				let inbox = match pending.remove(node_id) {
					Some(v) if !v.is_empty() => v,
					_ => continue,
				};

				let node = match flow.get_node(node_id) {
					Some(n) => n.clone(),
					None => continue,
				};

				let combined_output = self.dispatch_node(txn, &node, inbox)?;
				nodes_processed += 1;
				if combined_output.diffs.is_empty() {
					continue;
				}

				let child_count = node.outputs.len();
				for (child_idx, child_id) in node.outputs.iter().enumerate() {
					if child_idx + 1 == child_count {
						pending.entry(*child_id).or_default().push(combined_output);
						break;
					}
					pending.entry(*child_id).or_default().push(combined_output.clone());
				}
			}
		}

		Span::current().record("nodes_processed", nodes_processed);
		Ok(())
	}
}
