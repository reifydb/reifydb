// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::change::{Change, Diff},
	state::timer::TimerKind,
};
use reifydb_value::{Result, util::hash::Hash128, value::duration::Duration};
use tracing::instrument;

use super::{hooks::BuiltinHooks, operator::WindowOperator, rows::BuiltinRows};
use crate::{
	operator::{
		aggregation::accumulator::RowAccumulator,
		host::HostContext,
		state::seal::{gate::rearm_seal, ledger::FiredAt, rule::SealRule},
	},
	timer::Timer,
	window::{
		coord::DateTimeCoord,
		driver::{seal::reap, session::route_session},
		engine::tumbling::TumblingEngine,
	},
};

#[instrument(name = "flow::operator::window::tumbling", level = "trace", skip_all)]
pub fn apply_tumbling_engine(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	change: Change,
) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let mut runner = operator.runner.take().expect("a tumbling or sliding window owns a window runner");
	let mut rows = BuiltinRows::new(operator, &change);
	let routed = runner.route(&mut rows);
	let group_values = rows.into_group_values();
	let engine_config = operator.engine_config();
	let immutable = operator.immutable();
	let mut engine = operator.core.tumbling_engine_slot().take().unwrap_or_else(|| {
		Box::new(TumblingEngine::<Hash128, DateTimeCoord, RowAccumulator>::new(engine_config))
	});
	let mut hooks = BuiltinHooks::new(&mut operator.core, &kinds, immutable, group_values, change.changed_at);
	let diffs = routed.and_then(|routed| runner.apply(&mut engine, host, routed, &mut hooks));
	*operator.core.tumbling_engine_slot() = Some(engine);
	operator.runner = Some(runner);
	Ok(Change::from_flow(operator.core.operator, change.version, diffs?, change.changed_at))
}

#[instrument(name = "flow::operator::window::session", level = "trace", skip_all)]
pub fn apply_session_engine(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	change: Change,
) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let mut runner = operator.runner.take().expect("a session window owns a window runner");
	let mut rows = BuiltinRows::sessions(operator, &change);
	let routed = route_session(&mut rows);
	let group_values = rows.into_group_values();
	let engine_config = operator.engine_config();
	let immutable = operator.immutable();
	let mut engine = operator.core.tumbling_engine_slot().take().unwrap_or_else(|| {
		Box::new(TumblingEngine::<Hash128, DateTimeCoord, RowAccumulator>::new(engine_config))
	});
	let mut hooks = BuiltinHooks::new(&mut operator.core, &kinds, immutable, group_values, change.changed_at);
	let diffs = routed.and_then(|rows| runner.apply_session(&mut engine, host, rows, &mut hooks));
	*operator.core.tumbling_engine_slot() = Some(engine);
	operator.runner = Some(runner);
	Ok(Change::from_flow(operator.core.operator, change.version, diffs?, change.changed_at))
}

fn with_engine<T>(
	operator: &mut WindowOperator,
	run: impl FnOnce(&mut TumblingEngine<Hash128, DateTimeCoord, RowAccumulator>) -> Result<T>,
) -> Result<T> {
	let config = operator.engine_config();
	let mut engine =
		operator.core.tumbling_engine_slot().take().unwrap_or_else(|| {
			Box::new(TumblingEngine::<Hash128, DateTimeCoord, RowAccumulator>::new(config))
		});
	let out = run(&mut engine);
	*operator.core.tumbling_engine_slot() = Some(engine);
	out
}

fn engine_earliest_expiry(operator: &mut WindowOperator, host: &mut dyn HostContext) -> Result<Option<u64>> {
	with_engine(operator, |engine| engine.earliest_expiry(host))
}

fn rearm_engine_seal(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	rule: SealRule,
	before: Option<u64>,
) -> Result<()> {
	let after = engine_earliest_expiry(operator, host)?;
	rearm_seal(host, rule, &EncodedKey::new(Vec::new()), before, after)
}

pub fn forget_sealed_groups(operator: &mut WindowOperator, host: &mut dyn HostContext, fired: FiredAt) -> Result<()> {
	if let Some(mut runner) = operator.runner.take() {
		let reaped =
			with_engine(operator, |engine| runner.on_timer(engine, host, TimerKind::Maintenance, fired));
		operator.runner = Some(runner);
		return reaped;
	}
	let Some(size) = operator.size_duration() else {
		return Ok(());
	};
	let rule = SealRule::tumbling(size, operator.lateness().unwrap_or_else(Duration::zero));
	let budget = operator.engine_config().expire_batch();
	with_engine(operator, |engine| reap(engine, host, fired, rule.seal_span().duration(), budget).map(|_| ()))
}

pub fn seal_session_engines(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	timers: &[Timer],
) -> Result<Vec<Diff>> {
	let Some(latest) = timers.iter().max_by_key(|timer| timer.due) else {
		return Ok(Vec::new());
	};
	let Some(mut runner) = operator.runner.take() else {
		return Ok(Vec::new());
	};
	let keys: Vec<&[u8]> = timers.iter().map(|timer| timer.key.as_slice()).collect();
	let sealed = with_engine(operator, |engine| runner.on_session_timers(engine, host, &keys, FiredAt::of(latest)));
	operator.runner = Some(runner);
	sealed?;
	Ok(Vec::new())
}

pub fn seal_engine_windows(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	fired: FiredAt,
) -> Result<Vec<Diff>> {
	let Some(mut runner) = operator.runner.take() else {
		return Ok(Vec::new());
	};
	let sealed = with_engine(operator, |engine| runner.on_timer(engine, host, TimerKind::Seal, fired));
	operator.runner = Some(runner);
	sealed?;
	Ok(Vec::new())
}

#[cfg(test)]
mod tests {
	use reifydb_value::{factory::time::at_millis, value::duration::Duration};

	use super::SealRule;
	use crate::{
		operator::state::seal::rule::{is_sealed, output_watermark},
		window::coord::DateTimeCoord,
	};

	#[test]
	fn the_armed_seal_instant_reproduces_the_pre_timer_boundary() {
		// The armed instant and the sealing gate must be the same expression, or a bucket is
		// dropped before its timer fires or rebuilt from a late row after it did. The wheel fires
		// at watermark >= at, so reproducing the strict gate needs the +1 seal_instant carries.
		let cutoff_ms = 19u64;
		let cutoff = Duration::from_milliseconds(cutoff_ms as i64).expect("representable span");
		let rule = SealRule::tumbling(cutoff, Duration::from_milliseconds_const(0));
		let last = 10u64;
		let order = |millis: u64| {
			at_millis(i64::try_from(millis).expect("test order offset fits in i64 milliseconds")).to_order()
		};
		let sealed = |wm: u64| rule.seal_instant_from_order(order(last)).at().to_order() <= order(wm);
		let pre_timer_gate = |wm: u64| wm.saturating_sub(last) > cutoff_ms;

		for wm in 0..100u64 {
			assert_eq!(
				sealed(wm),
				pre_timer_gate(wm),
				"timer seal diverges from the gate at watermark {wm}"
			);
		}
		assert!(!sealed(last + cutoff_ms), "watermark exactly cutoff past the last event is still mutable");
		assert!(sealed(last + cutoff_ms + 1), "one past the cutoff is sealed");
	}

	#[test]
	fn output_watermark_goes_pre_epoch_for_young_watermarks() {
		// Wrapping past the epoch instead of going negative would seal every window early
		let lateness = Duration::from_milliseconds_const(10);

		assert_eq!(
			output_watermark(DateTimeCoord(at_millis(3)), lateness),
			DateTimeCoord(at_millis(-7)),
			"young watermark minus lateness is a real pre-epoch instant"
		);
		assert!(
			!is_sealed(
				DateTimeCoord(at_millis(0)),
				output_watermark(DateTimeCoord(at_millis(3)), lateness)
			),
			"the epoch is still after a pre-epoch horizon"
		);
		assert!(
			is_sealed(
				DateTimeCoord(at_millis(4)),
				output_watermark(DateTimeCoord(at_millis(20)), lateness)
			),
			"anchor below watermark - lateness is sealed"
		);
	}
}

#[cfg(test)]
mod reap_tests {
	use std::sync::Arc;

	use reifydb_core::{
		common::{CommitVersion, WindowKind, WindowSize},
		interface::catalog::flow::OperatorId,
		key::operator::state::{GroupId, KeyspaceId},
	};
	use reifydb_flow::context::FlowContext;
	use reifydb_routine_abi::registry::Routines;
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{
		factory::time::at_millis,
		value::{datetime::DateTime, duration::Duration},
	};

	use super::*;
	use crate::{
		operator::{
			host::TxnHostContext,
			state::{reaper::enqueue, test_utils::test::whole_keyspace},
			window::operator::{WindowConfig, WindowOperator},
		},
		timer::Timer,
		transaction::{
			ChangeCoordinate, FlowTransaction,
			deferred::DeferredTransaction,
			mock::FlowTxn,
			state::{StateExtension, StateRange},
		},
	};

	fn window(operator: u64) -> WindowOperator {
		WindowOperator::new(WindowConfig {
			parent_schema: None,
			operator: OperatorId(operator),
			kind: WindowKind::Tumbling {
				size: WindowSize::Duration(Duration::from_seconds(10).unwrap()),
			},
			group_by: Vec::new(),
			aggregations: Vec::new(),
			runtime_context: RuntimeContext::testing(0, 1),
			routines: Routines::empty(),
			lateness: None,
			immutable: None,
			ctx: Arc::new(FlowContext::default()),
		})
		.expect("the window operator must build")
	}

	fn txn_at(engine: &TestEngine, coordinate: u64) -> DeferredTransaction {
		let mut txn = engine.flow_txn().at(CommitVersion(coordinate)).deferred();
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_nanos(
				i64::try_from(coordinate).expect("test coordinate fits in i64 nanoseconds"),
			)),
		});
		txn
	}

	fn armed_timers(txn: &mut DeferredTransaction, operator: OperatorId) -> usize {
		txn.state_range(
			operator,
			StateRange::forward(whole_keyspace(GroupId::ROOT, KeyspaceId::TIMER_WHEEL), "test"),
		)
		.unwrap()
		.items
		.len()
	}

	fn fired() -> FiredAt {
		FiredAt::of(&Timer {
			due: at_millis(1_000),
			kind: TimerKind::Maintenance,
			key: EncodedKey::new(Vec::new()),
		})
	}

	fn reap_with_queued(groups: u128) -> usize {
		let engine = TestEngine::new();
		let mut operator = window(1);
		let id = operator.core.operator;
		let mut txn = txn_at(&engine, 100);
		{
			let mut host = TxnHostContext::new(&mut txn, id);
			for n in 0..groups {
				enqueue(&mut host, &[GroupId::hashed(Hash128(n + 1))]).unwrap();
			}
			forget_sealed_groups(&mut operator, &mut host, fired()).unwrap();
		}
		armed_timers(&mut txn, id)
	}

	#[test]
	fn a_reap_backlog_larger_than_one_budget_arms_another_maintenance_pass() {
		// one maintenance pass drains at most expire_batch groups; if the pass does not re-arm
		// itself the remainder is never revisited and the reap queue grows without bound
		let budget = window(1).engine_config().expire_batch() as u128;

		assert!(
			reap_with_queued(budget + 5) > 0,
			"a queue deeper than one budget must leave a maintenance timer armed to finish it"
		);
	}

	#[test]
	fn a_reap_queue_that_empties_in_one_pass_arms_nothing_further() {
		// re-arming unconditionally would spin the maintenance timer forever on an empty queue
		assert_eq!(reap_with_queued(2), 0, "a queue the pass drained to empty must not schedule another pass");
	}
}

#[cfg(test)]
mod seal_arm_tests {
	use std::{collections::HashMap, sync::Arc};

	use reifydb_codec::row::pod::EncodedPodRow;
	use reifydb_core::{
		common::{CommitVersion, WindowKind, WindowSize},
		interface::catalog::flow::OperatorId,
		key::{
			any::TaggedKey,
			operator::{
				keyspace::{expiry::RETIRED_AT_FLUSH, timer::TimerWheelKey},
				state::{GroupId, KeyspaceId},
			},
		},
		state::typed::SuffixBytes,
	};
	use reifydb_flow::context::FlowContext;
	use reifydb_routine_abi::registry::Routines;
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{
		factory::time::at_millis,
		value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
	};

	use super::*;
	use crate::{
		operator::{
			aggregation::{
				accumulator::SlotContributions,
				core::slot_rows::slot_row,
				engine::{EngineBuckets, slot_coord},
			},
			host::TxnHostContext,
			state::{seal::coord::Coord, test_utils::test::whole_keyspace},
			window::operator::{WindowConfig, WindowOperator},
		},
		timer::{Timer, wheel::TimerWheel},
		transaction::{
			ChangeCoordinate, FlowTransaction,
			deferred::DeferredTransaction,
			mock::FlowTxn,
			state::{StateExtension, StateRange},
		},
		window::{
			accumulator::{AccumulatorEvents, AccumulatorOp},
			driver::{route::Routed as DriverRouted, session::tracker_key, tumbling::finish_tumbling},
			engine::{ExpiryAnchor, WindowReindex},
			kind::session::open::{Member, OpenSession, OpenSessions, SessionIds},
			span::WindowSpan,
		},
	};

	const SIZE_MS: u64 = 10_000;

	fn window() -> WindowOperator {
		WindowOperator::new(WindowConfig {
			parent_schema: None,
			operator: OperatorId(1),
			kind: WindowKind::Tumbling {
				size: WindowSize::Duration(Duration::from_milliseconds(SIZE_MS as i64).unwrap()),
			},
			group_by: Vec::new(),
			aggregations: Vec::new(),
			runtime_context: RuntimeContext::testing(0, 1),
			routines: Routines::empty(),
			lateness: None,
			immutable: None,
			ctx: Arc::new(FlowContext::default()),
		})
		.expect("the window operator must build")
	}

	fn rule() -> SealRule {
		SealRule::tumbling(Duration::from_milliseconds(SIZE_MS as i64).unwrap(), Duration::zero())
	}

	fn txn_at(engine: &TestEngine, coordinate: u64) -> DeferredTransaction {
		let mut txn = engine.flow_txn().at(CommitVersion(coordinate)).deferred();
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_nanos(
				i64::try_from(coordinate).expect("test coordinate fits in i64 nanoseconds"),
			)),
		});
		txn
	}

	fn seal_timers(txn: &mut DeferredTransaction, operator: OperatorId) -> Vec<DateTime> {
		txn.state_range(
			operator,
			StateRange::forward(whole_keyspace(GroupId::ROOT, KeyspaceId::TIMER_WHEEL), "test"),
		)
		.unwrap()
		.items
		.iter()
		.filter_map(|item| {
			let TaggedKey::OperatorState(decoded) = &item.key else {
				panic!("a wheel row must decode");
			};
			let suffix =
				TimerWheelKey::from_suffix_bytes(&decoded.suffix).expect("a wheel row must decode");
			(suffix.kind.0 == TimerKind::Seal).then_some(suffix.due.0)
		})
		.collect()
	}

	fn index_windows(operator: &mut WindowOperator, host: &mut dyn HostContext, starts: &[u64]) {
		let config = operator.engine_config();
		let mut engine = operator.core.tumbling_engine_slot().take().unwrap_or_else(|| {
			Box::new(TumblingEngine::<Hash128, DateTimeCoord, RowAccumulator>::new(config))
		});
		let slot_key = EncodedKey::new(Vec::new());
		let windows: Vec<(Hash128, DateTimeCoord)> = starts
			.iter()
			.enumerate()
			.map(|(n, start_ms)| {
				let start = DateTimeCoord(at_millis(
					i64::try_from(*start_ms).expect("test window start fits in i64 milliseconds"),
				));
				(Hash128::from(n as u128), start)
			})
			.collect();
		let entries: Vec<WindowReindex<'_, Hash128, DateTimeCoord>> = windows
			.iter()
			.map(|(group, start)| WindowReindex {
				group,
				window_start: *start,
				id: GroupId::window(*group, start.to_order()),
				slot_key: &slot_key,
				prior: None,
				new: Some(start.to_order()),
			})
			.collect();
		engine.reindex_windows(host, &entries).unwrap();
		*operator.core.tumbling_engine_slot() = Some(engine);
	}

	fn take_one_due(txn: &mut DeferredTransaction, operator: OperatorId, watermark: u64) -> Timer {
		let mut timers = TimerWheel::take_due(
			operator,
			txn,
			at_millis(i64::try_from(watermark).expect("test watermark fits in i64 milliseconds")),
			16,
			None,
		)
		.unwrap()
		.timers;
		assert_eq!(timers.len(), 1, "exactly one timer may stand for the whole operator");
		timers.remove(0)
	}

	fn session_window() -> WindowOperator {
		WindowOperator::new(WindowConfig {
			parent_schema: None,
			operator: OperatorId(1),
			kind: WindowKind::Session {
				gap: Duration::from_milliseconds(1_000).unwrap(),
			},
			group_by: Vec::new(),
			aggregations: Vec::new(),
			runtime_context: RuntimeContext::testing(0, 1),
			routines: Routines::empty(),
			lateness: None,
			immutable: None,
			ctx: Arc::new(FlowContext::default()),
		})
		.expect("the session operator must build")
	}

	fn tracker_timer(hash: Hash128, due_ms: i64) -> Timer {
		Timer {
			due: at_millis(due_ms),
			kind: TimerKind::Seal,
			key: tracker_key(hash),
		}
	}

	fn queued_bodies(txn: &mut DeferredTransaction, operator: OperatorId) -> Vec<Vec<u8>> {
		txn.state_range(
			operator,
			StateRange::forward(whole_keyspace(GroupId::ROOT, KeyspaceId::REAP_QUEUE), "test"),
		)
		.unwrap()
		.items
		.iter()
		.map(|item| EncodedPodRow::from(item.bytes.clone()).body().to_vec())
		.collect()
	}

	fn maintenance_timers(txn: &mut DeferredTransaction, operator: OperatorId) -> usize {
		txn.state_range(
			operator,
			StateRange::forward(whole_keyspace(GroupId::ROOT, KeyspaceId::TIMER_WHEEL), "test"),
		)
		.unwrap()
		.items
		.iter()
		.filter(|item| {
			let TaggedKey::OperatorState(decoded) = &item.key else {
				panic!("a wheel row must decode");
			};
			TimerWheelKey::from_suffix_bytes(&decoded.suffix).expect("a wheel row must decode").kind.0
				== TimerKind::Maintenance
		})
		.count()
	}

	#[test]
	fn a_closed_tumbling_window_is_left_for_the_flush_and_arms_no_maintenance() {
		// a maintenance timer would drain the group inline, putting the cost back on the flow's hot path
		let engine = TestEngine::new();
		let mut operator = window();
		let id = operator.core.operator;
		let mut txn = txn_at(&engine, 100);
		let mut host = TxnHostContext::new(&mut txn, id);
		index_windows(&mut operator, &mut host, &[0, SIZE_MS]);

		seal_engine_windows(
			&mut operator,
			&mut host,
			FiredAt::of(&Timer {
				due: at_millis(1_000_000),
				kind: TimerKind::Seal,
				key: EncodedKey::new(Vec::new()),
			}),
		)
		.unwrap();
		drop(host);

		assert_eq!(
			queued_bodies(&mut txn, id),
			vec![RETIRED_AT_FLUSH.to_vec(), RETIRED_AT_FLUSH.to_vec()],
			"every closed window must carry the marker, or the flush never deletes it"
		);
		assert_eq!(maintenance_timers(&mut txn, id), 0);
	}

	#[test]
	fn a_closed_session_still_reaps_inline_because_the_flush_cannot_reach_its_root_rows() {
		// a session member has a sibling in the root group; deleting by group alone would leak that row for
		// good
		let engine = TestEngine::new();
		let mut operator = session_window();
		let id = operator.core.operator;
		let mut txn = txn_at(&engine, 100);
		let mut host = TxnHostContext::new(&mut txn, id);
		index_windows(&mut operator, &mut host, &[0, 1]);

		seal_session_engines(&mut operator, &mut host, &[tracker_timer(Hash128::from(1u128), 1_000_000)])
			.unwrap();
		drop(host);

		let bodies = queued_bodies(&mut txn, id);
		assert_eq!(bodies.len(), 2, "precondition: both sessions must have closed");
		assert!(bodies.iter().all(|body| body.is_empty()), "a session must never carry the flush marker");
		assert_eq!(maintenance_timers(&mut txn, id), 1, "the inline reap needs its timer");
	}

	#[test]
	fn a_seal_run_keeps_a_group_with_a_session_ahead_of_the_output_watermark_and_forgets_one_behind_it() {
		// Retiring a group whose session is still ahead of the horizon drops a live session and its later rows.
		let engine = TestEngine::new();
		let mut operator = session_window();
		let id = operator.core.operator;
		let mut txn = txn_at(&engine, 100);
		let (alive, closing) = (Hash128::from(1u128), Hash128::from(2u128));
		let gap = Duration::from_milliseconds_const(1_000);
		let one_session = |millis: i64| {
			let at = DateTimeCoord(at_millis(millis));
			OpenSessions::resumed(
				gap,
				SessionIds::StartTime,
				vec![OpenSession {
					id: at.to_order(),
					start: at,
					last: at,
					members: vec![Member {
						at,
						row: RowNumber(1),
					}],
				}],
				None,
			)
		};
		let mut host = TxnHostContext::new(&mut txn, id);
		let (alive_sessions, closing_sessions) = (one_session(2_500), one_session(1_100));
		operator.save_open_sessions(&mut host, &[(alive, &alive_sessions), (closing, &closing_sessions)])
			.unwrap();

		seal_session_engines(
			&mut operator,
			&mut host,
			&[tracker_timer(alive, 3_001), tracker_timer(closing, 3_101)],
		)
		.unwrap();

		let alive_left = operator.load_open_sessions(&mut host, alive).unwrap();
		let closing_left = operator.load_open_sessions(&mut host, closing).unwrap();
		assert_eq!(alive_left.sessions().len(), 1, "a session ahead of the horizon must survive the seal run");
		assert!(
			closing_left.sessions().is_empty(),
			"a session behind the horizon must close and its group retire"
		);
		assert_eq!(closing_left.sealed_last(), None, "a retired group keeps no state behind");
	}

	#[test]
	fn a_seal_run_closes_one_expire_budget_of_windows_per_timer_it_carries() {
		// A run capped at one budget closes fewer windows than its timers would alone, leaving late rows
		// admitted.
		let engine = TestEngine::new();
		let mut operator = session_window();
		let id = operator.core.operator;
		let mut txn = txn_at(&engine, 100);
		let due = operator.engine_config().expire_batch() + 44;
		let starts: Vec<u64> = (0..due as u64).collect();
		let mut host = TxnHostContext::new(&mut txn, id);
		index_windows(&mut operator, &mut host, &starts);

		seal_session_engines(
			&mut operator,
			&mut host,
			&[
				tracker_timer(Hash128::from(1u128), 1_000_000),
				tracker_timer(Hash128::from(2u128), 1_000_000),
			],
		)
		.unwrap();

		drop(host);
		assert!(
			seal_timers(&mut txn, id).is_empty(),
			"a run of two timers must close two budgets of due windows in its one pass"
		);
	}

	#[test]
	fn a_batch_of_many_windows_leaves_exactly_one_seal_timer() {
		// The sweep discards the timer key entirely, so any row past the earliest buys nothing.
		let engine = TestEngine::new();
		let mut operator = window();
		let id = operator.core.operator;
		let mut txn = txn_at(&engine, 100);
		let starts: Vec<u64> = (0..64).map(|n| n * SIZE_MS).collect();
		{
			let mut host = TxnHostContext::new(&mut txn, id);
			index_windows(&mut operator, &mut host, &starts);
			rearm_engine_seal(&mut operator, &mut host, rule(), None).unwrap();
		}

		assert_eq!(
			seal_timers(&mut txn, id),
			vec![at_millis(
				i64::try_from(SIZE_MS + 1).expect("test window bound fits in i64 milliseconds")
			)],
			"64 indexed windows must leave one seal timer, armed at the earliest window's seal instant"
		);
	}

	#[test]
	fn a_sweep_rearms_for_the_windows_it_left_behind() {
		// The wheel consumes the fired row, so without a re-arm every window left behind stays open.
		let engine = TestEngine::new();
		let mut operator = window();
		let id = operator.core.operator;
		let mut txn = txn_at(&engine, 100);
		{
			let mut host = TxnHostContext::new(&mut txn, id);
			index_windows(&mut operator, &mut host, &[0, SIZE_MS, 2 * SIZE_MS]);
			rearm_engine_seal(&mut operator, &mut host, rule(), None).unwrap();
		}
		let timer = take_one_due(&mut txn, id, SIZE_MS + 1);
		{
			let mut host = TxnHostContext::new(&mut txn, id);
			seal_engine_windows(&mut operator, &mut host, FiredAt::of(&timer)).unwrap();
		}

		assert_eq!(
			seal_timers(&mut txn, id),
			vec![at_millis(
				i64::try_from(2 * SIZE_MS + 1).expect("test window bound fits in i64 milliseconds"),
			)],
			"the sweep took the window at 0 and must leave the window at one span armed, not nothing"
		);
	}

	#[test]
	fn an_index_drained_to_empty_leaves_no_seal_timer_behind() {
		// A timer on an empty index fires forever, advancing the seal ledger past windows nothing holds.
		let engine = TestEngine::new();
		let mut operator = window();
		let id = operator.core.operator;
		let mut txn = txn_at(&engine, 100);
		{
			let mut host = TxnHostContext::new(&mut txn, id);
			index_windows(&mut operator, &mut host, &[0]);
			rearm_engine_seal(&mut operator, &mut host, rule(), None).unwrap();
		}
		let timer = take_one_due(&mut txn, id, SIZE_MS + 1);
		{
			let mut host = TxnHostContext::new(&mut txn, id);
			seal_engine_windows(&mut operator, &mut host, FiredAt::of(&timer)).unwrap();
		}

		assert!(seal_timers(&mut txn, id).is_empty(), "an empty expiry index must hold no seal timer");
	}

	fn accumulators(txn: &mut DeferredTransaction, operator: OperatorId, group: GroupId) -> usize {
		txn.state_range(operator, StateRange::forward(whole_keyspace(group, KeyspaceId::ACCUMULATOR), "test"))
			.unwrap()
			.items
			.len()
	}

	#[test]
	fn a_window_that_nets_to_empty_in_one_batch_is_still_forgotten_once_it_seals() {
		// a window that publishes nothing must still be indexed, otherwise its accumulator is never reaped
		let engine = TestEngine::new();
		let mut operator = window();
		let id = operator.core.operator;
		let hash = Hash128(7);
		let start = DateTimeCoord(at_millis(0));
		let span = WindowSpan {
			start,
			end: DateTimeCoord(at_millis(SIZE_MS as i64)),
		};
		let group = GroupId::window(hash, start.to_order());
		let mut txn = txn_at(&engine, 100);
		{
			let mut host = TxnHostContext::new(&mut txn, id);
			let mut events = AccumulatorEvents::<SlotContributions>::new();
			events.push(AccumulatorOp::Add, (slot_coord(start, 1), slot_row(Vec::new())));
			events.push(AccumulatorOp::Remove, (slot_coord(start, 1), slot_row(Vec::new())));
			let mut buckets = EngineBuckets::new();
			buckets.insert((hash, span), events);
			let routed = DriverRouted {
				buckets,
				arrival: vec![(hash, span)],
				window_max_coord: HashMap::from([((hash, span), start)]),
			};
			let config = operator.engine_config();
			let mut tumbling = operator.core.tumbling_engine_slot().take().unwrap_or_else(|| {
				Box::new(TumblingEngine::<Hash128, DateTimeCoord, RowAccumulator>::new(config))
			});
			let mut hooks = BuiltinHooks::new(
				&mut operator.core,
				&[],
				None,
				HashMap::from([(hash, Vec::new())]),
				start.0,
			);
			let diffs = finish_tumbling(
				&mut tumbling,
				&mut host,
				routed,
				&mut hooks,
				ExpiryAnchor::WindowStart,
			)
			.unwrap();
			*operator.core.tumbling_engine_slot() = Some(tumbling);
			assert!(diffs.is_empty(), "precondition: a window that nets to empty publishes nothing");
		}
		assert_eq!(accumulators(&mut txn, id, group), 1, "precondition: the empty accumulator is stored");

		let seal = FiredAt::of(&Timer {
			due: at_millis(SIZE_MS as i64 + 1),
			kind: TimerKind::Seal,
			key: EncodedKey::new(Vec::new()),
		});
		{
			let mut host = TxnHostContext::new(&mut txn, id);
			seal_engine_windows(&mut operator, &mut host, seal).unwrap();
			forget_sealed_groups(&mut operator, &mut host, seal).unwrap();
		}

		assert_eq!(accumulators(&mut txn, id, group), 0, "a sealed window must not keep its accumulator");
	}
}
