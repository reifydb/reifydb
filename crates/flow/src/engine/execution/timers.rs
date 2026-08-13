// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::flow::OperatorId, change::Change},
};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_value::Result;

use crate::{
	engine::{FlowEngineInner, execution::dispatch::Node},
	operator::host::TxnHostContext,
	timer::Timer,
	transaction::{ChangeCoordinate, FlowTransaction},
};

const MAX_TIMER_ROUNDS: u32 = 4_096;
const MAX_TIMERS_PER_DISPATCH: usize = 8_192;

impl FlowEngineInner {
	pub(super) fn dispatch_due_timers<T: FlowTransaction>(
		&mut self,
		txn: &mut T,
		flow: &FlowDag,
		version: CommitVersion,
		topo: &[OperatorId],
	) -> Result<u32> {
		let wheel = txn.timer_wheel();
		let watermarks = txn.source_watermarks();
		let sources: Vec<OperatorId> = topo
			.iter()
			.copied()
			.filter(|id| flow.get_operator(id).is_some_and(|operator| operator.ty.declares_time()))
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
			if rounds > MAX_TIMER_ROUNDS {
				let oldest = due.iter().map(|(_, timer)| timer.at).min().map(|at| at.to_millis());
				panic!(
					"timer dispatch did not reach quiescence within {MAX_TIMER_ROUNDS} rounds at \
					 version {}: watermark {} ms, {} timers still due, oldest armed at {:?} ms, \
					 {:?} ms behind the watermark; a lag near zero means an operator re-arms \
					 what it just fired, a large lag means the watermark jumped past a backlog",
					version.0,
					watermark.to_millis(),
					due.len(),
					oldest,
					oldest.map(|at| watermark.to_millis() as i64 - at as i64)
				);
			}

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
				let node = match self.operators.get_mut(&operator_id) {
					Some(operator) => Node::Operator(operator),
					None => match self.durable_sinks.get_mut(&operator_id) {
						Some(sink) => Node::DurableSink(sink),
						None => continue,
					},
				};
				txn.set_change_coordinate(ChangeCoordinate {
					at: Some(timer.at),
					version,
				});
				let fired = match node {
					Node::Operator(operator) => {
						let mut host = TxnHostContext::new(txn, operator.id());
						let fired = operator.on_timer(&mut host, timer)?;
						operator.flush(&mut host)?;
						fired
					}
					Node::DurableSink(sink) => txn.run_durable_sink_timer(&mut **sink, timer)?,
				};
				let Some(result) = fired else {
					continue;
				};
				if result.diffs.is_empty() {
					continue;
				}
				let combined = Change::from_flow(operator_id, version, result.diffs, result.changed_at);
				for child_id in &graph_node.outputs {
					pending.entry(*child_id).or_default().push(combined.clone());
				}
			}
			self.run_topology(txn, flow, pending, topo)?;
		}
	}
}
