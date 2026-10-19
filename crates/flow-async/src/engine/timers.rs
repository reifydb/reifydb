// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	common::ChangeVersion,
	flow::dag::FlowDag,
	interface::{catalog::flow::OperatorId, change::Change},
	internal_err,
	key::operator::keyspace::timer::TimerWheelKey,
	state::timer::TimerKind,
};
use reifydb_flow::txn::Intern;
use reifydb_value::{Result, reifydb_assertions};

use crate::{
	engine::{FlowEngineInner, dispatch::Node},
	operator::host::TxnHostContext,
	timer::{Timer, TimerDue, registry::TimerStage, timer_runs, wheel::TimerWheel},
	transaction::{ChangeCoordinate, FlowTransaction, watermark::SourceWatermarks},
};

const MAX_TIMER_ROUNDS: u32 = 4_096;
const MAX_TIMERS_PER_DISPATCH: usize = 8_192;

impl FlowEngineInner {
	pub(super) fn dispatch_due_timers<T: FlowTransaction + Intern>(
		&mut self,
		txn: &mut T,
		flow: &FlowDag,
		version: ChangeVersion,
		topo: &[OperatorId],
	) -> Result<u32> {
		let sources = self.watermark_sources(flow);
		if sources.is_empty() {
			return Ok(0);
		}

		let mut stage = self.timers.stage(flow.id);
		let fired = self.dispatch_staged_timers(txn, flow, version, topo, &sources, &mut stage)?;
		self.timers.apply(stage);
		Ok(fired)
	}

	fn dispatch_staged_timers<T: FlowTransaction + Intern>(
		&mut self,
		txn: &mut T,
		flow: &FlowDag,
		version: ChangeVersion,
		topo: &[OperatorId],
		sources: &[OperatorId],
		stage: &mut TimerStage,
	) -> Result<u32> {
		let mut fired_total = 0u32;
		let mut rounds = 0u32;
		let mut budget = MAX_TIMERS_PER_DISPATCH;
		let mut cursors: HashMap<OperatorId, TimerWheelKey> = HashMap::new();
		loop {
			let watermark = SourceWatermarks::flow_watermark(sources, txn)?;
			txn.set_flow_watermark(watermark);
			let armed = txn.take_armed();
			for entry in &armed {
				if cursors.get(&entry.operator_id).is_some_and(|cursor| entry.due <= cursor.due.0) {
					cursors.remove(&entry.operator_id);
				}
			}
			let mut due: Vec<(OperatorId, Timer)> = Vec::new();
			for candidate in stage.due_before(armed, watermark) {
				if budget == 0 {
					break;
				}
				let operator_id = candidate.operator_id;
				let taken = TimerWheel::take_due(
					operator_id,
					txn,
					watermark,
					budget,
					cursors.get(&operator_id),
				)?;
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
					version.commit.0,
					watermark.to_millis(),
					due.len(),
					oldest,
					oldest.map(|at| watermark.to_millis() - at)
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
			let mut queue = due.into_iter().peekable();
			while let Some((operator_id, first)) = queue.next() {
				let mut chunk = vec![first];
				while let Some((_, timer)) = queue.next_if(|(next, _)| *next == operator_id) {
					chunk.push(timer);
				}
				fired_total += chunk.len() as u32;
				let Some(graph_node) = flow.get_operator(&operator_id) else {
					continue;
				};
				for run in timer_runs(chunk) {
					let node = match self.operators.get_mut(&(flow.id, operator_id)) {
						Some(operator) => Node::Operator(operator),
						None => match self.durable_sinks.get_mut(&(flow.id, operator_id)) {
							Some(sink) => Node::DurableSink(sink),
							None => continue,
						},
					};
					let outputs = match node {
						Node::Operator(operator)
							if operator.batches_timers()
								&& run[0].kind == TimerKind::Seal =>
						{
							let count = run.len();
							txn.set_change_coordinate(ChangeCoordinate {
								at: run.iter().map(|timer| timer.due).max(),
							});
							let mut host = TxnHostContext::with_retention(
								txn,
								operator.id(),
								operator.retention(),
							);
							let outputs = operator.on_timers(&mut host, run)?;
							if outputs.len() != count {
								return internal_err!(
									"operator {} answered {} timer outputs for {} timers",
									operator_id.0,
									outputs.len(),
									count
								);
							}
							outputs
						}
						Node::Operator(operator) => {
							let mut outputs = Vec::with_capacity(run.len());
							for timer in run {
								txn.set_change_coordinate(ChangeCoordinate {
									at: Some(timer.due),
								});
								let mut host = TxnHostContext::with_retention(
									txn,
									operator.id(),
									operator.retention(),
								);
								outputs.push(operator.on_timer(&mut host, timer)?);
							}
							outputs
						}
						Node::DurableSink(sink) => {
							let mut outputs = Vec::with_capacity(run.len());
							for timer in run {
								txn.set_change_coordinate(ChangeCoordinate {
									at: Some(timer.due),
								});
								outputs.push(
									txn.run_durable_sink_timer(&mut **sink, timer)?
								);
							}
							outputs
						}
					};
					for result in outputs.into_iter().flatten() {
						reifydb_assertions! {
							for diff in &result.diffs {
								for batch in diff.pre().into_iter().chain(diff.post()) {
									reifydb_value::value::canonical::assert_canonical_floats(batch, "flow timer");
								}
							}
						}
						if result.diffs.is_empty() {
							continue;
						}
						let combined = Change::from_flow(
							operator_id,
							version,
							result.diffs,
							result.changed_at,
						);
						for child_id in &graph_node.outputs {
							pending.entry(*child_id).or_default().push(combined.clone());
						}
					}
				}
			}
			self.run_topology(txn, flow, pending, topo)?;
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::{Arc, Mutex};

	#[cfg(reifydb_assertions)]
	use arrow_array::{ArrayRef, Float64Array, RecordBatch};
	use reifydb_codec::key::encoded::EncodedKey;
	#[cfg(reifydb_assertions)]
	use reifydb_core::interface::change::Diff;
	use reifydb_core::{
		common::{ChangeVersion, CommitVersion},
		flow::{
			dag::FlowDag,
			operator::{FlowNode, OperatorDef},
		},
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
	const OTHER: OperatorId = OperatorId(2);
	const DUE_MS: i64 = 5_000;
	const WATERMARK_MS: i64 = 10_000;

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

	#[cfg(reifydb_assertions)]
	struct NegativeZeroTimer(fn(RecordBatch) -> Diff);

	#[cfg(reifydb_assertions)]
	impl HostOperator for NegativeZeroTimer {
		fn id(&self) -> OperatorId {
			OPERATOR
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> Result<Change> {
			Ok(change)
		}

		fn on_timer(&mut self, _host: &mut dyn HostContext, timer: Timer) -> Result<Option<Change>> {
			let column: ArrayRef = Arc::new(Float64Array::from(vec![-0.0f64]));
			let batch = RecordBatch::try_from_iter([("c", column)]).unwrap();
			let version = ChangeVersion::from(CommitVersion(1));
			Ok(Some(Change::from_flow(OPERATOR, version, vec![(self.0)(batch)], timer.due)))
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
		let fired = inner
			.dispatch_due_timers(
				&mut txn,
				&flow,
				ChangeVersion::from(CommitVersion(1)),
				flow.topological_order(),
			)
			.unwrap();

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

	type Fired = Arc<Mutex<Vec<(OperatorId, i64, TimerKind, Vec<u8>)>>>;

	struct RecordingProbe {
		id: OperatorId,
		fired: Fired,
	}

	impl HostOperator for RecordingProbe {
		fn id(&self) -> OperatorId {
			self.id
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> Result<Change> {
			Ok(change)
		}

		fn on_timer(&mut self, _host: &mut dyn HostContext, timer: Timer) -> Result<Option<Change>> {
			self.fired.lock().unwrap().push((
				self.id,
				timer.due.to_millis(),
				timer.kind,
				timer.key.to_vec(),
			));
			Ok(None)
		}
	}

	fn dispatch_recorded(timers: &[(OperatorId, Timer)]) -> Vec<(OperatorId, i64, TimerKind, Vec<u8>)> {
		let fired: Fired = Arc::default();
		dispatch_with(timers, |id| {
			Box::new(RecordingProbe {
				id,
				fired: fired.clone(),
			})
		});
		let recorded = fired.lock().unwrap().clone();
		recorded
	}

	fn dispatch_with(timers: &[(OperatorId, Timer)], probe: impl Fn(OperatorId) -> Box<dyn HostOperator>) {
		let engine = TestEngine::new();
		let mut inner = engine_inner(&engine);
		let mut builder = FlowDag::builder(FLOW);
		for (id, view) in [(OPERATOR, 1), (OTHER, 2)] {
			builder.add_node(FlowNode::new(
				id,
				OperatorDef::SourceView {
					view: ViewId(view),
				},
			));
		}
		let flow = builder.build();
		inner.register_flow_dag(flow.clone());

		let mut seed = engine.flow_txn().deferred();
		for id in [OPERATOR, OTHER] {
			SourceWatermarks::advance(id, &mut seed, at_millis(WATERMARK_MS)).unwrap();
		}
		for (id, timer) in timers {
			TimerWheel::arm(*id, &mut seed, timer).unwrap();
		}
		let seeded = seed.take_pending();
		let store = engine.inner().operator_state();
		apply_operator_state(&store, &seeded);
		let armed: Vec<TimerDue> =
			flow.get_operator_ids().filter_map(|id| TimerWheel::next_due_stored(id, &store)).collect();
		inner.timers.rebuild(FLOW, armed);

		for id in [OPERATOR, OTHER] {
			inner.insert_operator(FLOW, id, probe(id));
		}
		let mut txn = engine.flow_txn().deferred();
		inner.dispatch_due_timers(
			&mut txn,
			&flow,
			ChangeVersion::from(CommitVersion(1)),
			flow.topological_order(),
		)
		.unwrap();
	}

	type Runs = Arc<Mutex<Vec<(bool, Vec<(OperatorId, i64, TimerKind)>)>>>;

	struct BatchingProbe {
		id: OperatorId,
		runs: Runs,
	}

	impl HostOperator for BatchingProbe {
		fn id(&self) -> OperatorId {
			self.id
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> Result<Change> {
			Ok(change)
		}

		fn on_timer(&mut self, _host: &mut dyn HostContext, timer: Timer) -> Result<Option<Change>> {
			self.runs.lock().unwrap().push((false, vec![(self.id, timer.due.to_millis(), timer.kind)]));
			Ok(None)
		}

		fn batches_timers(&self) -> bool {
			true
		}

		fn on_timers(
			&mut self,
			_host: &mut dyn HostContext,
			timers: Vec<Timer>,
		) -> Result<Vec<Option<Change>>> {
			let run = timers.iter().map(|timer| (self.id, timer.due.to_millis(), timer.kind)).collect();
			self.runs.lock().unwrap().push((true, run));
			Ok(timers.iter().map(|_| None).collect())
		}
	}

	fn timer_at(millis: i64, kind: TimerKind, key: &str) -> Timer {
		Timer {
			due: at_millis(millis),
			kind,
			key: EncodedKey::new(key.as_bytes()),
		}
	}

	#[test]
	fn interleaved_operators_keep_their_timer_order_within_a_dispatch() {
		// Grouping one operator's timers across another's reorders their outputs into shared children.
		let fired = dispatch_recorded(&[
			(OPERATOR, timer_at(5_000, TimerKind::Seal, "a")),
			(OTHER, timer_at(5_000, TimerKind::Seal, "b")),
			(OPERATOR, timer_at(6_000, TimerKind::Seal, "c")),
		]);

		assert_eq!(
			fired.iter().map(|(id, due, _, key)| (*id, *due, key.clone())).collect::<Vec<_>>(),
			vec![
				(OPERATOR, 5_000, b"a".to_vec()),
				(OTHER, 5_000, b"b".to_vec()),
				(OPERATOR, 6_000, b"c".to_vec())
			],
			"timers must fire in (due, operator) order across operators"
		);
	}

	#[test]
	fn a_maintenance_timer_between_seals_keeps_its_place_in_the_dispatch() {
		// A seal batched past a maintenance timer reaps groups before their grace runs out.
		let fired = dispatch_recorded(&[
			(OPERATOR, timer_at(5_000, TimerKind::Seal, "a")),
			(OPERATOR, timer_at(5_500, TimerKind::Maintenance, "")),
			(OPERATOR, timer_at(6_000, TimerKind::Seal, "b")),
		]);

		assert_eq!(
			fired.iter().map(|(_, due, kind, _)| (*due, *kind)).collect::<Vec<_>>(),
			vec![(5_000, TimerKind::Seal), (5_500, TimerKind::Maintenance), (6_000, TimerKind::Seal)],
			"the maintenance timer must fire between the two seals"
		);
	}

	#[test]
	fn a_batching_operator_gets_each_run_of_its_own_seals_and_nothing_else_in_one_call() {
		// A run spanning another operator or a maintenance timer reorders outputs or reaps before grace.
		let runs: Runs = Arc::default();
		dispatch_with(
			&[
				(OPERATOR, timer_at(5_000, TimerKind::Seal, "a")),
				(OPERATOR, timer_at(5_100, TimerKind::Seal, "b")),
				(OPERATOR, timer_at(5_500, TimerKind::Maintenance, "")),
				(OPERATOR, timer_at(6_000, TimerKind::Seal, "c")),
				(OTHER, timer_at(6_000, TimerKind::Seal, "d")),
				(OPERATOR, timer_at(7_000, TimerKind::Seal, "e")),
			],
			|id| {
				Box::new(BatchingProbe {
					id,
					runs: runs.clone(),
				})
			},
		);

		assert_eq!(
			runs.lock().unwrap().clone(),
			vec![
				(true, vec![(OPERATOR, 5_000, TimerKind::Seal), (OPERATOR, 5_100, TimerKind::Seal)]),
				(false, vec![(OPERATOR, 5_500, TimerKind::Maintenance)]),
				(true, vec![(OPERATOR, 6_000, TimerKind::Seal)]),
				(true, vec![(OTHER, 6_000, TimerKind::Seal)]),
				(true, vec![(OPERATOR, 7_000, TimerKind::Seal)]),
			],
			"only consecutive seals of one operator may share a batch, and maintenance must fire alone"
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
			inner.dispatch_due_timers(
				&mut failing,
				&flow,
				ChangeVersion::from(CommitVersion(1)),
				flow.topological_order()
			)
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
			.dispatch_due_timers(
				&mut retry,
				&flow,
				ChangeVersion::from(CommitVersion(1)),
				flow.topological_order(),
			)
			.unwrap();

		assert_eq!(fired, 1, "the timer the failed dispatch never committed must still be due");
	}

	#[test]
	#[cfg(reifydb_assertions)]
	#[should_panic(expected = "is not canonical")]
	fn a_timer_output_holding_negative_zero_panics() {
		// A timer's output skips apply, so a raw -0.0 must stop here or it reaches the children unchecked.
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
		inner.timers.rebuild(FLOW, armed);
		inner.insert_operator(FLOW, OPERATOR, Box::new(NegativeZeroTimer(Diff::insert)));

		let mut txn = engine.flow_txn().deferred();
		let _ = inner.dispatch_due_timers(
			&mut txn,
			&flow,
			ChangeVersion::from(CommitVersion(1)),
			flow.topological_order(),
		);
	}

	#[test]
	#[cfg(reifydb_assertions)]
	#[should_panic(expected = "is not canonical")]
	fn a_timer_removal_holding_negative_zero_panics() {
		// A timer removal carries its batch in pre only, so a check on post alone must never let -0.0 pass.
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
		inner.timers.rebuild(FLOW, armed);
		inner.insert_operator(FLOW, OPERATOR, Box::new(NegativeZeroTimer(Diff::remove)));

		let mut txn = engine.flow_txn().deferred();
		let _ = inner.dispatch_due_timers(
			&mut txn,
			&flow,
			ChangeVersion::from(CommitVersion(1)),
			flow.topological_order(),
		);
	}
}
