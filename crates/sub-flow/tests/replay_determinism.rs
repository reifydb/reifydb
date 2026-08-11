// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The flow replay-determinism gate: a flow fed the same recorded input must end in the same
//! place no matter what the wall clock reads and no matter how the rows were sliced into
//! batches. Every scenario runs the same input under (a) wildly different mock clocks with the
//! batching held fixed, and (b) wildly different batch boundaries with the clock held fixed,
//! then compares the emitted output (post-consolidation) and the raw state keyspaces.
//!
//! Comparison contract:
//! - Clock axis: byte-identical everything - keys, value bodies, AND row header stamps - across every operator
//!   keyspace, data and control. No allowlist. This is what tasks "no clock read is ever encoded into operator state"
//!   buys.
//! - Batch axis: keys and value BODIES must match everywhere; the named allowlists carve out exactly the state that is
//!   arrival-derived by design, and nothing else.
//!
//! Both comparators live in `reifydb_testing_flow::state` so the catch-up suites hold flows to
//! the same contract this one does.

use std::sync::Arc;

use reifydb_core::{
	common::{CommitVersion, JoinType, WindowKind, WindowSize},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
		consolidate::consolidate_diffs,
	},
	key::operator_state::Keyspace,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_flow::{
	context::FlowContext,
	operator::{
		Operator, OperatorCell,
		distinct::operator::DistinctOperator,
		join::operator::{JoinOperator, JoinSideConfig},
		scan::series::SourceSeriesOperator,
		window::operator::{WindowConfig, WindowOperator},
	},
};
use reifydb_routine::{
	function::default_in_process_functions, monoid::default_in_process_monoids,
	procedure::default_in_process_procedures,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::expression::parse_expression;
use reifydb_testing_flow::{
	generator,
	harness::Harness,
	state::{State, assert_batch_equivalent, assert_identical_bytes, keyspace_of},
};
use reifydb_value::{
	factory::time::at_millis,
	fragment::Fragment,
	value::{
		Value, datetime::DateTime, duration::Duration, row_number::RowNumber, system_columns::SystemColumns,
		value_type::ValueType,
	},
};

const SOURCE: OperatorId = OperatorId(0);
const SUBJECT: OperatorId = OperatorId(1);

// A plausible live wall clock and a replay wall clock over a century later; the row times sit far
// from both so neither run can pass by the clock happening to agree with the data.
const CLOCK_LIVE_MS: u64 = 1_000_000_000;
const CLOCK_REPLAY_MS: u64 = 4_102_444_800_000;

fn routines() -> Routines {
	let b = Routines::builder();
	let b = default_in_process_functions(b);
	let b = default_in_process_procedures(b);
	default_in_process_monoids(b).configure()
}

fn count_keyspace(state: &State, keyspace: Keyspace) -> usize {
	state.iter().filter(|(key, _)| keyspace_of(key) == Some(keyspace)).count()
}

fn render(diffs: Vec<Diff>) -> Vec<String> {
	consolidate_diffs(diffs).expect("emitted diffs consolidate").iter().map(|diff| format!("{diff:?}")).collect()
}

struct Run {
	emitted: Vec<String>,
	state: State,
	pre_settle: Option<State>,
}

fn chunks<'a, T>(events: &'a [T], slices: &[usize]) -> Vec<&'a [T]> {
	assert_eq!(slices.iter().sum::<usize>(), events.len(), "a slicing must cover the input exactly");
	let mut out = Vec::with_capacity(slices.len());
	let mut start = 0;
	for &len in slices {
		out.push(&events[start..start + len]);
		start += len;
	}
	out
}

#[derive(Clone)]
enum Event {
	Insert(reifydb_core::row::Row),
	Remove(reifydb_core::row::Row),
	Update(reifydb_core::row::Row, reifydb_core::row::Row),
}

fn change_of(events: &[Event]) -> Change {
	let mut diffs = Vec::with_capacity(events.len());
	for event in events {
		diffs.push(match event {
			Event::Insert(row) => Diff::insert(Columns::from_row(row)),
			Event::Remove(row) => Diff::remove(Columns::from_row(row)),
			Event::Update(pre, post) => Diff::update(Columns::from_row(pre), Columns::from_row(post)),
		});
	}
	Change::from_flow(SOURCE, CommitVersion(1), diffs, DateTime::default())
}

fn feed<O: Operator>(h: &mut Harness<O>, events: &[Event], slices: &[usize]) -> Vec<Diff> {
	let mut emitted = Vec::new();
	for chunk in chunks(events, slices) {
		let out = h.apply(change_of(chunk)).expect("the change applies");
		emitted.extend(out.diffs);
	}
	emitted
}

mod distinct {
	use super::*;

	fn events() -> Vec<Event> {
		// Nine distinct values interleaved so that first-appearance order is very unlikely to
		// coincide with hash order, plus removes that first shrink and then empty one entry,
		// plus an update that moves a row to a brand-new value.
		let values: [i64; 16] = [30, 10, 40, 10, 50, 90, 20, 60, 50, 30, 50, 80, 90, 70, 90, 30];
		let mut events: Vec<Event> = values
			.iter()
			.enumerate()
			.map(|(i, &v)| {
				Event::Insert(generator::row(
					RowNumber(i as u64 + 1),
					1,
					v,
					at_millis(1_000 * (i as u64 + 1)),
				))
			})
			.collect();
		events.push(Event::Remove(generator::row(RowNumber(4), 1, 10, at_millis(4_000))));
		events.push(Event::Remove(generator::row(RowNumber(2), 1, 10, at_millis(2_000))));
		events.push(Event::Update(
			generator::row(RowNumber(9), 1, 50, at_millis(9_000)),
			generator::row(RowNumber(9), 1, 55, at_millis(19_000)),
		));
		events
	}

	fn drive(clock_ms: u64, slices: &[usize]) -> Run {
		let mut h = Harness::with_engine(move |engine, runtime| {
			engine.mock_clock().set_millis(clock_ms);
			DistinctOperator::new(
				OperatorCell::new(SourceSeriesOperator::new(SOURCE)),
				SUBJECT,
				parse_expression("v").expect("the distinct key parses"),
				routines(),
				runtime,
				Arc::new(FlowContext::default()),
				None,
			)
		});
		let events = events();
		let emitted = feed(&mut h, &events, slices);
		Run {
			emitted: render(emitted),
			state: h.state_items().expect("the state range reads"),
			pre_settle: None,
		}
	}

	const ALL: &[usize] = &[19];
	const ONE_BY_ONE: &[usize] = &[1; 19];
	const UNEVEN: &[usize] = &[3, 1, 5, 2, 4, 1, 3];

	#[test]
	fn distinct_state_is_identical_across_wall_clocks() {
		// The clock half of the distinct contract: with batching fixed, a century of wall-clock
		// difference must not reach the emitted diffs or a single persisted byte. Before the
		// flush-stamp fix the DistinctEntry/DistinctLayout headers carried clock().now() read at
		// flush time, which this comparison catches byte-for-byte.
		let live = drive(CLOCK_LIVE_MS, ALL);
		let replay = drive(CLOCK_REPLAY_MS, ALL);

		assert!(
			count_keyspace(&live.state, Keyspace::DISTINCT_ENTRY) >= 8,
			"too few distinct entries survived; the comparison would prove nothing about ordering"
		);
		assert_eq!(live.emitted, replay.emitted, "emitted diffs must not depend on the wall clock");
		assert_identical_bytes("distinct/clock", &live.state, &replay.state);
	}

	#[test]
	fn distinct_state_is_identical_across_batch_boundaries() {
		// The batching half: the same 19 events as one batch, as 19 batches, and as uneven
		// slices must allocate the same GroupIds (first-appearance order, which slicing
		// preserves), mint the same row numbers, and leave byte-identical distinct entries
		// including their header stamps (carried row-derived mutation times). Before the
		// allocation-order fix the one-big-batch run interned in HashSet iteration order, so
		// its id assignment disagreed with the row-at-a-time run's.
		let canonical = drive(CLOCK_LIVE_MS, ALL);
		for (label, slices) in [("one-by-one", ONE_BY_ONE), ("uneven", UNEVEN)] {
			let sliced = drive(CLOCK_LIVE_MS, slices);
			assert_eq!(
				canonical.emitted, sliced.emitted,
				"distinct/batch/{label}: consolidated output must not depend on batch boundaries"
			);
			assert_batch_equivalent(&format!("distinct/batch/{label}"), &canonical.state, &sliced.state);
		}

		let both = drive(CLOCK_REPLAY_MS, UNEVEN);
		assert_eq!(canonical.emitted, both.emitted, "distinct/both-axes: output diverged");
		assert_batch_equivalent("distinct/both-axes", &canonical.state, &both.state);
	}

	#[test]
	fn distinct_snapshot_covers_the_keyspaces_the_gate_promises_to_watch() {
		// A vacuous gate is worse than none: if the flow stopped writing one of the keyspaces
		// the comparisons above claim to cover, they would pass forever on emptiness.
		let run = drive(CLOCK_LIVE_MS, ALL);
		for keyspace in [
			Keyspace::DISTINCT_ENTRY,
			Keyspace::DISTINCT_LAYOUT,
			Keyspace::GROUP_DICTIONARY,
			Keyspace::GROUP_RECORD,
			Keyspace::ROW_NUMBER_MAPPING,
			Keyspace::NODE_COUNTER,
		] {
			assert!(
				count_keyspace(&run.state, keyspace) > 0,
				"keyspace {keyspace:?} is empty; the determinism gate no longer covers it"
			);
		}
	}
}

mod count_window {
	use super::*;

	fn events() -> Vec<Event> {
		// Two groups filling count-2 tumbling buckets at staggered rates, with a leftover row
		// per group so unsealed buffer state is compared too.
		let rows: [(u64, i32, i64, u64); 9] = [
			(1, 1, 5, 1_000),
			(2, 2, 7, 1_500),
			(3, 1, 9, 2_000),
			(4, 1, 11, 3_000),
			(5, 2, 13, 3_500),
			(6, 2, 17, 4_000),
			(7, 1, 19, 5_000),
			(8, 2, 23, 6_000),
			(9, 1, 29, 7_000),
		];
		rows.iter()
			.map(|&(rn, g, v, ms)| Event::Insert(generator::row(RowNumber(rn), g, v, at_millis(ms))))
			.collect()
	}

	fn drive(clock_ms: u64, slices: &[usize]) -> Run {
		let mut h = window_harness(
			WindowKind::Tumbling {
				size: WindowSize::Count(2),
			},
			Duration::default(),
			clock_ms,
		);
		let events = events();
		let mut emitted = feed(&mut h, &events, slices);
		for change in h.settle_timers(600_000).expect("the wheel settles") {
			emitted.extend(change.diffs);
		}
		Run {
			emitted: render(emitted),
			state: h.state_items().expect("the state range reads"),
			pre_settle: None,
		}
	}

	const ALL: &[usize] = &[9];
	const ONE_BY_ONE: &[usize] = &[1; 9];
	const UNEVEN: &[usize] = &[2, 1, 4, 2];

	#[test]
	fn count_window_state_is_identical_across_wall_clocks() {
		let live = drive(CLOCK_LIVE_MS, ALL);
		let replay = drive(CLOCK_REPLAY_MS, ALL);

		assert!(!live.state.is_empty(), "no state survived, the comparison is vacuous");
		assert_eq!(live.emitted, replay.emitted, "emitted diffs must not depend on the wall clock");
		assert_identical_bytes("count-window/clock", &live.state, &replay.state);
	}

	#[test]
	fn count_window_state_is_identical_across_batch_boundaries() {
		let canonical = drive(CLOCK_LIVE_MS, ALL);
		for (label, slices) in [("one-by-one", ONE_BY_ONE), ("uneven", UNEVEN)] {
			let sliced = drive(CLOCK_LIVE_MS, slices);
			assert_eq!(
				canonical.emitted, sliced.emitted,
				"count-window/batch/{label}: consolidated output must not depend on batch boundaries"
			);
			assert_batch_equivalent(
				&format!("count-window/batch/{label}"),
				&canonical.state,
				&sliced.state,
			);
		}
	}
}

mod time_window {
	use super::*;

	fn events() -> Vec<Event> {
		// Two groups spread over five 2s buckets, with one late-but-in-grace row (rn 8 lands at
		// 2.5s after rows reached 6s) so the grace path participates in the comparison.
		let rows: [(u64, i32, i64, u64); 10] = [
			(1, 1, 5, 500),
			(2, 2, 7, 1_000),
			(3, 1, 9, 2_500),
			(4, 2, 11, 3_000),
			(5, 1, 13, 4_500),
			(6, 2, 17, 5_000),
			(7, 1, 19, 6_000),
			(8, 1, 23, 2_600),
			(9, 2, 29, 7_500),
			(10, 1, 31, 9_500),
		];
		rows.iter()
			.map(|&(rn, g, v, ms)| Event::Insert(generator::row(RowNumber(rn), g, v, at_millis(ms))))
			.collect()
	}

	fn drive(clock_ms: u64, slices: &[usize]) -> Run {
		let mut h = window_harness(
			WindowKind::Tumbling {
				size: WindowSize::Duration(Duration::from_seconds(2).expect("representable")),
			},
			Duration::from_seconds(1).expect("representable"),
			clock_ms,
		);
		let events = events();
		let mut emitted = feed(&mut h, &events, slices);
		let pre_settle = h.state_items().expect("the state range reads");
		for change in h.settle_timers(600_000).expect("the wheel settles") {
			emitted.extend(change.diffs);
		}
		Run {
			emitted: render(emitted),
			state: h.state_items().expect("the state range reads"),
			pre_settle: Some(pre_settle),
		}
	}

	const ALL: &[usize] = &[10];
	const ONE_BY_ONE: &[usize] = &[1; 10];
	const UNEVEN: &[usize] = &[3, 1, 4, 2];

	#[test]
	fn time_window_state_is_identical_across_wall_clocks() {
		// The pre-settle snapshot is compared too: armed seal and grace timers are state (the
		// wheel keyspace), and their rows are written while timers are still outstanding. A
		// clock read stamped at arm time only exists BEFORE the wheel drains, so comparing
		// only the settled state would let it escape.
		let live = drive(CLOCK_LIVE_MS, ALL);
		let replay = drive(CLOCK_REPLAY_MS, ALL);

		let live_pre = live.pre_settle.as_ref().expect("the drive snapshots before settling");
		let replay_pre = replay.pre_settle.as_ref().expect("the drive snapshots before settling");
		assert!(
			count_keyspace(live_pre, Keyspace::TIMER_WHEEL) > 0,
			"no timer was armed before settling, so the wheel comparison is vacuous"
		);
		assert_identical_bytes("time-window/clock/pre-settle", live_pre, replay_pre);

		assert_eq!(live.emitted, replay.emitted, "emitted diffs must not depend on the wall clock");
		assert_identical_bytes("time-window/clock", &live.state, &replay.state);
		assert_eq!(
			count_keyspace(&live.state, Keyspace::TIMER_WHEEL),
			0,
			"settling must drain the wheel to quiescence"
		);
	}

	#[test]
	fn time_window_state_is_identical_across_batch_boundaries() {
		let canonical = drive(CLOCK_LIVE_MS, ALL);
		for (label, slices) in [("one-by-one", ONE_BY_ONE), ("uneven", UNEVEN)] {
			let sliced = drive(CLOCK_LIVE_MS, slices);
			assert_eq!(
				canonical.emitted, sliced.emitted,
				"time-window/batch/{label}: consolidated output must not depend on batch boundaries"
			);
			assert_batch_equivalent(&format!("time-window/batch/{label}"), &canonical.state, &sliced.state);
		}
	}
}

mod join {
	use super::*;

	const LEFT: OperatorId = OperatorId(10);
	const RIGHT: OperatorId = OperatorId(11);
	const JOIN: OperatorId = OperatorId(12);

	const LEFT_COLUMNS: [(&str, ValueType); 3] =
		[("lid", ValueType::Int8), ("k", ValueType::Int4), ("lv", ValueType::Int8)];
	const RIGHT_COLUMNS: [(&str, ValueType); 3] =
		[("rid", ValueType::Int8), ("k", ValueType::Int4), ("rv", ValueType::Int8)];

	#[derive(Clone, Copy)]
	enum Side {
		Left,
		Right,
	}

	impl Side {
		fn operator(self) -> OperatorId {
			match self {
				Side::Left => LEFT,
				Side::Right => RIGHT,
			}
		}

		fn spec(self) -> &'static [(&'static str, ValueType); 3] {
			match self {
				Side::Left => &LEFT_COLUMNS,
				Side::Right => &RIGHT_COLUMNS,
			}
		}
	}

	#[derive(Clone, Copy)]
	struct Spec {
		side: Side,
		rn: u64,
		k: i32,
		v: i64,
		ms: u64,
	}

	#[derive(Clone, Copy)]
	enum JoinEvent {
		Insert(Spec),
		Remove(Spec),
	}

	fn schema(spec: &[(&str, ValueType)]) -> Columns {
		Columns::new(
			spec.iter()
				.map(|(name, ty)| {
					ColumnWithName::new(
						Fragment::internal(*name),
						ColumnBuffer::with_capacity(ty.clone(), 0),
					)
				})
				.collect(),
		)
	}

	fn columns_of(spec: Spec) -> Columns {
		let shape = spec.side.spec();
		let values = [Value::Int8(spec.rn as i64), Value::Int4(spec.k), Value::Int8(spec.v)];
		let columns = shape
			.iter()
			.zip(values)
			.map(|((name, ty), value)| {
				let mut buffer = ColumnBuffer::with_capacity(ty.clone(), 1);
				buffer.push_value(value);
				ColumnWithName::new(Fragment::internal(*name), buffer)
			})
			.collect();
		let time = at_millis(spec.ms);
		Columns::with_system(
			columns,
			SystemColumns::new(vec![RowNumber(spec.rn)], Vec::new(), vec![time], vec![time], vec![time]),
		)
	}

	fn change_of(events: &[JoinEvent]) -> Change {
		let diffs: Vec<Diff> = events
			.iter()
			.map(|event| {
				let (mut diff, side) = match event {
					JoinEvent::Insert(spec) => (Diff::insert(columns_of(*spec)), spec.side),
					JoinEvent::Remove(spec) => (Diff::remove(columns_of(*spec)), spec.side),
				};
				diff.set_origin(Some(ChangeOrigin::Flow(side.operator())));
				diff
			})
			.collect();
		Change::from_flow(LEFT, CommitVersion(1), diffs, DateTime::default())
	}

	fn events() -> Vec<JoinEvent> {
		// Three keys, both sides multi-row on key 1 (the cartesian path), one right-side
		// retraction and one left-side retraction so unpublish paths are compared too.
		let l = |rn, k, v, ms| Spec {
			side: Side::Left,
			rn,
			k,
			v,
			ms,
		};
		let r = |rn, k, v, ms| Spec {
			side: Side::Right,
			rn,
			k,
			v,
			ms,
		};
		vec![
			JoinEvent::Insert(l(1, 1, 100, 1_000)),
			JoinEvent::Insert(r(101, 1, 900, 1_500)),
			JoinEvent::Insert(l(2, 2, 200, 2_000)),
			JoinEvent::Insert(r(102, 3, 800, 2_500)),
			JoinEvent::Insert(l(3, 1, 300, 3_000)),
			JoinEvent::Insert(r(103, 1, 700, 3_500)),
			JoinEvent::Insert(l(4, 3, 400, 4_000)),
			JoinEvent::Insert(r(104, 2, 600, 4_500)),
			JoinEvent::Insert(l(5, 2, 500, 5_000)),
			JoinEvent::Remove(r(103, 1, 700, 3_500)),
			JoinEvent::Insert(r(105, 1, 650, 6_000)),
			JoinEvent::Remove(l(2, 2, 200, 2_000)),
		]
	}

	fn drive(clock_ms: u64, slices: &[usize]) -> Run {
		let mut h = Harness::with_engine(move |engine, _runtime| {
			engine.mock_clock().set_millis(clock_ms);
			JoinOperator::new(
				JoinSideConfig {
					operator: LEFT,
					exprs: parse_expression("k").expect("left key parses"),
					schema: schema(&LEFT_COLUMNS),
				},
				JoinSideConfig {
					operator: RIGHT,
					exprs: parse_expression("k").expect("right key parses"),
					schema: schema(&RIGHT_COLUMNS),
				},
				JOIN,
				JoinType::Inner,
				None,
				engine.executor().routines.clone(),
				engine.executor().runtime_context.clone(),
				false,
				false,
				false,
				None,
				None,
				Arc::new(FlowContext::default()),
			)
		});
		let events = events();
		let mut emitted = Vec::new();
		for chunk in chunks(&events, slices) {
			let out = h.apply(change_of(chunk)).expect("the change applies");
			emitted.extend(out.diffs);
		}
		Run {
			emitted: render(emitted),
			state: h.state_items().expect("the state range reads"),
			pre_settle: None,
		}
	}

	const ALL: &[usize] = &[12];
	const ONE_BY_ONE: &[usize] = &[1; 12];
	const UNEVEN: &[usize] = &[2, 1, 4, 3, 2];

	#[test]
	fn join_state_is_identical_across_wall_clocks() {
		let live = drive(CLOCK_LIVE_MS, ALL);
		let replay = drive(CLOCK_REPLAY_MS, ALL);

		assert!(
			count_keyspace(&live.state, Keyspace::JOIN_LEFT) > 0
				&& count_keyspace(&live.state, Keyspace::JOIN_RIGHT) > 0,
			"one join side holds no state, so the comparison would not cover the join keyspaces"
		);
		assert_eq!(live.emitted, replay.emitted, "emitted diffs must not depend on the wall clock");
		assert_identical_bytes("join/clock", &live.state, &replay.state);
	}

	#[test]
	fn join_state_is_identical_across_batch_boundaries() {
		let canonical = drive(CLOCK_LIVE_MS, ALL);
		for (label, slices) in [("one-by-one", ONE_BY_ONE), ("uneven", UNEVEN)] {
			let sliced = drive(CLOCK_LIVE_MS, slices);
			assert_eq!(
				canonical.emitted, sliced.emitted,
				"join/batch/{label}: consolidated output must not depend on batch boundaries"
			);
			assert_batch_equivalent(&format!("join/batch/{label}"), &canonical.state, &sliced.state);
		}
	}
}

fn window_harness(kind: WindowKind, grace: Duration, clock_ms: u64) -> Harness<WindowOperator> {
	Harness::with_engine(move |engine, runtime| {
		engine.mock_clock().set_millis(clock_ms);
		WindowOperator::new(WindowConfig {
			parent: OperatorCell::new(SourceSeriesOperator::new(SOURCE)),
			operator: SUBJECT,
			kind,
			group_by: parse_expression("g").expect("group_by parses"),
			aggregations: parse_expression("total: math::sum(v)").expect("aggregation parses"),
			runtime_context: runtime,
			routines: routines(),
			grace,
			ctx: Arc::new(FlowContext::default()),
		})
	})
}
