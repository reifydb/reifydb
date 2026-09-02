// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	sync::OnceLock,
	time::{Duration, Instant},
};

use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::flow::OperatorId, change::Change},
	key::operator::keyspace::timer::TimerWheelKey,
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

static TIMER_PROFILE: OnceLock<bool> = OnceLock::new();

fn timer_profile() -> bool {
	*TIMER_PROFILE.get_or_init(|| std::env::var_os("REIFYDB_TIMER_PROFILE").is_some())
}

#[derive(Default)]
struct DispatchProfile {
	loops: u32,
	watermark: Duration,
	take_due: Duration,
	on_timer: Duration,
	topology: Duration,
}

impl DispatchProfile {
	fn report(&self, flow: u64, version: u64, rounds: u32, fired: u32) {
		let total = self.watermark + self.take_due + self.on_timer + self.topology;
		println!(
			"TIMERPROF flow={} version={} loops={} rounds={} fired={} total_us={} watermark_us={} \
			 take_due_us={} on_timer_us={} topology_us={}",
			flow,
			version,
			self.loops,
			rounds,
			fired,
			total.as_micros(),
			self.watermark.as_micros(),
			self.take_due.as_micros(),
			self.on_timer.as_micros(),
			self.topology.as_micros()
		);
	}
}

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
		let profile_on = timer_profile();
		let mut profile = DispatchProfile::default();
		let mut fired_total = 0u32;
		let mut rounds = 0u32;
		let mut budget = MAX_TIMERS_PER_DISPATCH;
		let mut cursors: HashMap<OperatorId, TimerWheelKey> = HashMap::new();
		loop {
			profile.loops += 1;
			let started = Instant::now();
			let watermark = SourceWatermarks::flow_watermark(sources, txn)?;
			profile.watermark += started.elapsed();
			txn.set_flow_watermark(watermark);
			let armed = txn.take_armed();
			for entry in &armed {
				if cursors.get(&entry.operator_id).is_some_and(|cursor| entry.due <= cursor.due.0) {
					cursors.remove(&entry.operator_id);
				}
			}
			let started = Instant::now();
			let mut due: Vec<(OperatorId, Timer)> = Vec::new();
			for candidate in stage.due_before(armed, watermark) {
				if budget == 0 {
					break;
				}
				let operator_id = candidate.operator_id;
				let taken =
					TimerWheel::take_due(operator_id, txn, watermark, budget, cursors.get(&operator_id))?;
				for timer in taken.timers {
					budget -= 1;
					due.push((operator_id, timer));
				}
				if let Some(resume) = taken.resume {
					cursors.insert(operator_id, resume);
				}
				stage.refresh(
					operator_id,
					taken.next.map(|due| TimerDue {
						operator_id,
						due,
					}),
				);
			}
			profile.take_due += started.elapsed();
			if due.is_empty() {
				if profile_on && fired_total > 0 {
					profile.report(flow.id.0, version.0, rounds, fired_total);
				}
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

			let started = Instant::now();
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
			profile.on_timer += started.elapsed();
			let started = Instant::now();
			self.run_topology(txn, flow, pending, topo)?;
			profile.topology += started.elapsed();
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
		engine::{FlowEngineInner, timers::MAX_TIMERS_PER_DISPATCH},
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

	fn burst_timer(index: usize, kind: TimerKind) -> Timer {
		Timer {
			due: at_millis(DUE_MS),
			kind,
			key: EncodedKey::new(format!("burst{index:08}").as_bytes()),
		}
	}

	fn dispatch_burst(count: usize, kind: TimerKind) {
		let engine = TestEngine::new();
		let mut inner = engine_inner(&engine);
		let flow = dag();
		inner.register_flow_dag(flow.clone());

		let mut seed = engine.flow_txn().deferred();
		SourceWatermarks::advance(OPERATOR, &mut seed, at_millis(WATERMARK_MS)).unwrap();
		for index in 0..count {
			TimerWheel::arm(OPERATOR, &mut seed, &burst_timer(index, kind)).unwrap();
		}
		let seeded = seed.take_pending();
		let store = engine.inner().operator_state();
		apply_operator_state(&store, &seeded);

		let armed: Vec<TimerDue> =
			flow.get_operator_ids().filter_map(|id| TimerWheel::next_due_stored(id, &store)).collect();
		inner.timers.rebuild(FLOW, armed);
		probe(&mut inner, false);

		let mut txn = engine.flow_txn().deferred();
		let started = std::time::Instant::now();
		let fired = inner
			.dispatch_due_timers(&mut txn, &flow, CommitVersion(1), flow.topological_order())
			.unwrap();
		let elapsed = started.elapsed();
		println!(
			"TIMERBURST kind={:?} armed={} fired={} wall_us={}",
			kind,
			count,
			fired,
			elapsed.as_micros()
		);

		assert_eq!(
			fired as usize,
			count.min(MAX_TIMERS_PER_DISPATCH),
			"every armed timer under the dispatch budget must fire in one dispatch"
		);
	}

	#[test]
	fn a_burst_of_seal_timers_all_fire_in_one_dispatch() {
		// a burst larger than one scan must still drain inside the dispatch budget, otherwise timers stay
		for count in [64usize, 256, 1_024, 4_096, 8_192] {
			dispatch_burst(count, TimerKind::Seal);
		}
	}

	#[test]
	fn a_burst_of_maintenance_timers_all_fire_in_one_dispatch() {
		// a maintenance key carries a second index row that take_due must remove with it, or the next arm
		for count in [64usize, 256, 1_024, 4_096, 8_192] {
			dispatch_burst(count, TimerKind::Maintenance);
		}
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
