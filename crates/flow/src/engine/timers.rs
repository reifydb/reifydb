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
	engine::{FlowEngineInner, dispatch::Node},
	operator::host::TxnHostContext,
	timer::{Timer, TimerDue, registry::TimerStage, wheel::TimerWheel},
	transaction::{ChangeCoordinate, FlowTransaction, watermark::SourceWatermarks},
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
		let sources: Vec<OperatorId> = topo
			.iter()
			.copied()
			.filter(|id| flow.get_operator(id).is_some_and(|operator| operator.ty.declares_time()))
			.collect();
		if sources.is_empty() {
			return Ok(0);
		}

		let mut stage = self.timers.stage(flow.id);
		let fired = self.dispatch_staged_timers(txn, flow, version, topo, &sources, &mut stage)?;
		self.timers.apply(stage);
		Ok(fired)
	}

	fn dispatch_staged_timers<T: FlowTransaction>(
		&mut self,
		txn: &mut T,
		flow: &FlowDag,
		version: CommitVersion,
		topo: &[OperatorId],
		sources: &[OperatorId],
		stage: &mut TimerStage,
	) -> Result<u32> {
		let mut fired_total = 0u32;
		let mut rounds = 0u32;
		let mut budget = MAX_TIMERS_PER_DISPATCH;
		loop {
			let watermark = SourceWatermarks::flow_watermark(sources, txn)?;
			txn.set_flow_watermark(watermark);
			let armed = txn.take_armed();
			let mut due: Vec<(OperatorId, Timer)> = Vec::new();
			for candidate in stage.due_before(armed, watermark) {
				if budget == 0 {
					break;
				}
				let operator_id = candidate.operator_id;
				let (timers, next) = TimerWheel::take_due(operator_id, txn, watermark, budget)?;
				for timer in timers {
					budget -= 1;
					due.push((operator_id, timer));
				}
				stage.refresh(
					operator_id,
					next.map(|due| TimerDue {
						operator_id,
						due,
					}),
				);
			}
			if due.is_empty() {
				return Ok(fired_total);
			}
			rounds += 1;
			if rounds > MAX_TIMER_ROUNDS {
				let oldest = due.iter().map(|(_, timer)| timer.due).min().map(|at| at.to_millis());
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
				(left.due, left_node.0, left.kind as u8, left.key.as_ref()).cmp(&(
					right.due,
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
				let node = match self.operators.get_mut(&(flow.id, operator_id)) {
					Some(operator) => Node::Operator(operator),
					None => match self.durable_sinks.get_mut(&(flow.id, operator_id)) {
						Some(sink) => Node::DurableSink(sink),
						None => continue,
					},
				};
				txn.set_change_coordinate(ChangeCoordinate {
					at: Some(timer.due),
					version,
				});
				let fired = match node {
					Node::Operator(operator) => {
						let mut host = TxnHostContext::new(txn, operator.id());
						operator.on_timer(&mut host, timer)?
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

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::{
		common::CommitVersion,
		interface::{
			catalog::{
				flow::{FlowId, OperatorId},
				id::ViewId,
			},
			change::Change,
			flow::OperatorCapability,
		},
		state::timer::TimerKind,
	};
	use reifydb_rql::flow::{
		flow::FlowDag,
		operator::{FlowNode, OperatorDef},
	};
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{
		Result,
		error::{Diagnostic, Error},
		factory::time::at_millis,
	};

	use crate::{
		engine::FlowEngineInner,
		operator::{
			HostOperator, host::HostContext, metrics::OperatorSampleRegistry,
			provider::EmptyOperatorProvider,
		},
		timer::{Timer, TimerDue, wheel::TimerWheel},
		transaction::{
			FlowTransaction,
			mock::FlowTxn,
			substrate::{FlowSubstrate, apply_operator_state},
			watermark::SourceWatermarks,
		},
	};

	const FLOW: FlowId = FlowId(1);
	const OPERATOR: OperatorId = OperatorId(1);
	const DUE_MS: u64 = 5_000;
	const WATERMARK_MS: u64 = 10_000;

	struct TimerProbe {
		fails: bool,
	}

	impl HostOperator for TimerProbe {
		fn id(&self) -> OperatorId {
			OPERATOR
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> Result<Change> {
			Ok(change)
		}

		fn on_timer(&mut self, _host: &mut dyn HostContext, _timer: Timer) -> Result<Option<Change>> {
			if self.fails {
				return Err(Error(Box::new(Diagnostic {
					code: "TEST_TIMER_HANDLER_FAILED".to_string(),
					message: "the timer handler failed".to_string(),
					..Default::default()
				})));
			}
			Ok(None)
		}
	}

	fn engine_inner(engine: &TestEngine) -> FlowEngineInner {
		FlowEngineInner::new(
			engine.catalog(),
			engine.executor().routines.clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			Arc::new(EmptyOperatorProvider),
			FlowSubstrate::with_dictionary(
				engine.inner().dictionary_allocators(),
				engine.inner().operator_state(),
			),
			OperatorSampleRegistry::new(),
		)
	}

	fn dag() -> FlowDag {
		let mut builder = FlowDag::builder(FLOW);
		builder.add_node(FlowNode::new(
			OPERATOR,
			OperatorDef::SourceView {
				view: ViewId(1),
			},
		));
		builder.build()
	}

	fn due_timer() -> Timer {
		Timer {
			due: at_millis(DUE_MS),
			kind: TimerKind::Seal,
			key: EncodedKey::new(b"bucket"),
		}
	}

	fn probe(inner: &mut FlowEngineInner, fails: bool) {
		inner.insert_operator(
			FLOW,
			OPERATOR,
			Box::new(TimerProbe {
				fails,
			}),
		);
	}

	#[test]
	fn a_dispatch_that_fails_after_taking_a_timer_leaves_it_discoverable() {
		// take_due only stages its removes, so an index advanced before the failure strands a timer that is
		// still armed
		let engine = TestEngine::new();
		let mut inner = engine_inner(&engine);
		let flow = dag();
		inner.register_flow_dag(flow.clone());

		let mut seed = engine.flow_txn().deferred();
		SourceWatermarks::advance(OPERATOR, &mut seed, at_millis(WATERMARK_MS)).unwrap();
		TimerWheel::arm(OPERATOR, &mut seed, &due_timer()).unwrap();
		let seeded = seed.take_pending();
		let store = engine.inner().operator_state();
		apply_operator_state(&store, &seeded);

		let armed: Vec<TimerDue> =
			flow.get_operator_ids().filter_map(|id| TimerWheel::next_due_stored(id, &store)).collect();
		assert_eq!(
			armed,
			vec![TimerDue {
				operator_id: OPERATOR,
				due: at_millis(DUE_MS),
			}],
			"the index must start where registration would rebuild it from the stored wheel"
		);
		inner.timers.rebuild(FLOW, armed);

		probe(&mut inner, true);
		let mut failing = engine.flow_txn().deferred();
		assert!(
			inner.dispatch_due_timers(&mut failing, &flow, CommitVersion(1), flow.topological_order())
				.is_err(),
			"the probe must fail the dispatch after take_due has staged its removes"
		);
		drop(failing);

		assert_eq!(
			inner.timers.due_before(Vec::new(), FLOW, at_millis(WATERMARK_MS)),
			vec![TimerDue {
				operator_id: OPERATOR,
				due: at_millis(DUE_MS),
			}],
			"a failed dispatch must leave the index exactly where it found it"
		);

		probe(&mut inner, false);
		let mut retry = engine.flow_txn().deferred();
		let fired = inner
			.dispatch_due_timers(&mut retry, &flow, CommitVersion(1), flow.topological_order())
			.unwrap();

		assert_eq!(fired, 1, "the timer the failed dispatch never committed must still be due");
	}
}
