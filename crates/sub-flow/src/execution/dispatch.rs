// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::change::Change;
use reifydb_rql::flow::node::FlowNode;
use reifydb_value::Result;
use tracing::{Span, field, instrument};

use crate::{engine::FlowEngineInner, transaction::FlowTransaction};

impl FlowEngineInner {
	pub(super) fn dispatch_node(
		&self,
		txn: &mut FlowTransaction,
		node: &FlowNode,
		inbox: Vec<Change>,
	) -> Result<Change> {
		let merged = Change::merge(inbox)?;
		let version = merged.version;
		let changed_at = merged.changed_at;
		let result = self.apply(txn, node, merged)?;
		let combined = Change::from_flow(node.id, version, result.diffs, changed_at.max(result.changed_at));
		Ok(combined)
	}

	#[instrument(name = "flow::engine::apply", level = "trace", skip(self, txn, change, node), fields(
		node_id = ?node.id,
		node_type = node.ty.label(),
		num_parents = node.inputs.len(),
		input_diffs = change.diffs.len(),
		input_rows = field::Empty,
		output_diffs_raw = field::Empty,
		output_diffs = field::Empty,
		output_rows = field::Empty,
		lock_wait_us = field::Empty,
		apply_time_us = field::Empty,
		coalesce_time_us = field::Empty
	))]
	fn apply(&self, txn: &mut FlowTransaction, node: &FlowNode, change: Change) -> Result<Change> {
		let lock_start = self.runtime_context.clock.instant();
		let operator = self.operators.get(&node.id).unwrap().clone();
		Span::current().record("lock_wait_us", lock_start.elapsed().as_micros() as u64);

		Span::current().record("input_rows", change.row_count());

		let apply_start = self.runtime_context.clock.instant();
		let result = operator.apply(txn, change)?;
		Span::current().record("apply_time_us", apply_start.elapsed().as_micros() as u64);
		Span::current().record("output_diffs_raw", result.diffs.len());

		let coalesce_start = self.runtime_context.clock.instant();
		Span::current().record("coalesce_time_us", coalesce_start.elapsed().as_micros() as u64);
		Span::current().record("output_diffs", result.diffs.len());
		Span::current().record("output_rows", result.row_count());
		Ok(result)
	}
}
