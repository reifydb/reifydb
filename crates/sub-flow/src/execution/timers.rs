// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::flow::OperatorId, change::Change},
};
use reifydb_flow::{
	timer::Timer,
	transaction::{ChangeCoordinate, FlowTransaction},
};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_value::Result;

use crate::engine::FlowEngineInner;

const MAX_TIMER_ROUNDS: u32 = 4_096;
const MAX_TIMERS_PER_DISPATCH: usize = 8_192;

impl FlowEngineInner {
	pub(super) fn dispatch_due_timers(
		&self,
		txn: &mut FlowTransaction,
		flow: &FlowDag,
		version: CommitVersion,
		topo: &[OperatorId],
	) -> Result<u32> {
		let wheel = txn.timer_wheel();
		let watermarks = txn.source_watermarks();
		let sources: Vec<OperatorId> = topo
			.iter()
			.copied()
			.filter(|id| flow.get_operator(id).is_some_and(|operator| operator.ty.is_source()))
			.collect();
		if sources.is_empty() {
			return Ok(0);
		}

		let mut fired_total = 0u32;
		let mut rounds = 0u32;
		let mut budget = MAX_TIMERS_PER_DISPATCH;
		loop {
			let watermark = watermarks.flow_watermark(&sources, txn)?;
			txn.set_flow_watermark(watermark);
			let mut due: Vec<(OperatorId, Timer)> = Vec::new();
			for operator_id in topo {
				if budget == 0 {
					break;
				}
				for timer in wheel.take_due(*operator_id, txn, watermark, budget)? {
					budget -= 1;
					due.push((*operator_id, timer));
				}
			}
			if due.is_empty() {
				return Ok(fired_total);
			}
			rounds += 1;
			assert!(
				rounds <= MAX_TIMER_ROUNDS,
				"timer dispatch did not reach quiescence within {MAX_TIMER_ROUNDS} rounds at \
				 version {}; an operator keeps arming timers that are already due",
				version.0
			);

			due.sort_by(|(left_node, left), (right_node, right)| {
				(left.at, left_node.0, left.kind as u8, left.key.as_ref()).cmp(&(
					right.at,
					right_node.0,
					right.kind as u8,
					right.key.as_ref(),
				))
			});

			let mut pending: HashMap<OperatorId, Vec<Change>> = HashMap::new();
			for (operator_id, timer) in due {
				fired_total += 1;
				let Some(graph_node) = flow.get_operator(&operator_id) else {
					continue;
				};
				let Some(operator) = self.operators.get(&operator_id).cloned() else {
					continue;
				};
				txn.set_change_coordinate(ChangeCoordinate {
					at: timer.at,
					version,
				});
				let Some(result) = operator.on_timer(txn, timer)? else {
					continue;
				};
				if result.diffs.is_empty() {
					continue;
				}
				let combined = Change::from_flow(operator_id, version, result.diffs, result.changed_at);
				let child_count = graph_node.outputs.len();
				for (child_idx, child_id) in graph_node.outputs.iter().enumerate() {
					if child_idx + 1 == child_count {
						pending.entry(*child_id).or_default().push(combined);
						break;
					}
					pending.entry(*child_id).or_default().push(combined.clone());
				}
			}
			self.run_topology(txn, flow, pending, topo)?;
		}
	}
}
