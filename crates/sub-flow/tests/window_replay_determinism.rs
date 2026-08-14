// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Replay determinism for the window operator: the same input rows must produce the same emitted
//! diffs, the same armed seal timers, and the same persisted state no matter what the wall clock
//! reads. Each pair of runs below differs only in its mock clock, so any divergence is a clock
//! read leaking into sealing decisions or state bytes.

use std::sync::Arc;

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::operator::{EncodedOperatorRow, decode},
};
use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::catalog::flow::OperatorId,
	key::{
		EncodableKey,
		operator_state::{Keyspace, OperatorStateKey},
	},
	state::store::TimerKind,
	value::column::columns::Columns,
};
use reifydb_flow::{
	context::FlowContext,
	operator::window::operator::{WindowConfig, WindowOperator},
	state::seal::coord::Coord,
	timer::Timer,
	window::meta::EngineMeta,
};
use reifydb_routine::{
	function::default_in_process_functions, monoid::default_in_process_monoids,
	procedure::default_in_process_procedures,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::expression::parse_expression;
use reifydb_testing_flow::{generator, harness::Harness};
use reifydb_value::{
	factory::time::at_millis,
	value::{duration::Duration, row_number::RowNumber},
};

const SUBJECT: OperatorId = OperatorId(1);
const GROUP: i32 = 1;

// A plausible live wall clock and a replay wall clock over a century later. The row times below
// sit far from both, so neither run can pass by the clock happening to agree with the data.
const CLOCK_LIVE_MS: u64 = 1_000_000_000;
const CLOCK_REPLAY_MS: u64 = 4_102_444_800_000;

fn routines() -> Routines {
	let b = Routines::builder();
	let b = default_in_process_functions(b);
	let b = default_in_process_procedures(b);
	default_in_process_monoids(b).configure()
}

fn harness(kind: WindowKind, clock_ms: u64) -> Harness<WindowOperator> {
	Harness::with_engine(move |engine, runtime| {
		engine.mock_clock().set_millis(clock_ms);
		WindowOperator::new(WindowConfig {
			parent_schema: Some(Columns::empty()),
			operator: SUBJECT,
			kind,
			group_by: parse_expression("g").expect("group_by parses"),
			aggregations: parse_expression("total: math::sum(v)").expect("aggregation parses"),
			runtime_context: runtime,
			routines: routines(),
			seal: Duration::default(),
			ctx: Arc::new(FlowContext::default()),
		})
	})
}

/// What one run leaves behind: every state key, the value bytes of every data-keyspace row (the
/// rows the operator itself writes; identity rows carry substrate bookkeeping stamps and are
/// compared by key only), the decoded EngineMeta last-event times, and the armed timer count.
struct Snapshot {
	keys: Vec<Vec<u8>>,
	data_values: Vec<(Vec<u8>, Vec<u8>)>,
	metas: Vec<u64>,
	armed_timers: usize,
}

fn snapshot(h: &mut Harness<WindowOperator>) -> Snapshot {
	let mut keys = Vec::new();
	let mut data_values = Vec::new();
	let mut metas = Vec::new();
	let mut armed_timers = 0usize;
	for (key, row) in h.state_items().expect("the state range reads") {
		keys.push(key.to_vec());
		let Some(keyspace) = OperatorStateKey::decode(&key).map(|state| state.keyspace) else {
			continue;
		};
		if keyspace == Keyspace::TIMER_WHEEL {
			armed_timers += 1;
		}
		if keyspace == Keyspace::ENGINE_META {
			let meta: EngineMeta =
				decode(&EncodedOperatorRow::try_from(row.clone()).expect("engine meta state bytes"))
					.expect("engine meta decodes");
			metas.push(meta.last_event_time);
		}
		if keyspace.is_data() {
			data_values.push((key.to_vec(), row.to_vec()));
		}
	}
	Snapshot {
		keys,
		data_values,
		metas,
		armed_timers,
	}
}

fn drive_sealing(clock_ms: u64) -> (Vec<String>, Snapshot) {
	let kind = WindowKind::Tumbling {
		size: WindowSize::Duration(Duration::from_seconds(2).expect("representable")),
	};
	let mut h = harness(kind, clock_ms);
	let mut emitted = Vec::new();
	emitted.push(format!(
		"{:?}",
		h.apply(generator::insert(vec![
			generator::row(RowNumber(1), GROUP, 5, at_millis(1_000)),
			generator::row(RowNumber(2), GROUP, 7, at_millis(1_500)),
		]))
		.expect("the first bucket applies")
	));
	emitted.push(format!(
		"{:?}",
		h.apply(generator::insert(vec![generator::row(RowNumber(3), GROUP, 9, at_millis(10_000))]))
			.expect("the second bucket applies")
	));
	emitted.push(format!(
		"{:?}",
		h.on_timer(Timer {
			due: at_millis(10_000),
			kind: TimerKind::Seal,
			key: EncodedKey::new(Vec::new()),
		})
		.expect("the seal fires")
	));
	let snap = snapshot(&mut h);
	(emitted, snap)
}

#[test]
fn sealing_is_identical_under_wildly_different_wall_clocks() {
	// The replay-determinism contract for a time-based window: two runs over the same rows, one
	// with a live clock and one replaying a century later, must arm the same seal timers at the
	// same data-derived instants, emit the same diffs, and leave the same state. The armed timer
	// instant is part of its TIMER_WHEEL key, so a clock-derived seal instant shows up as a key
	// mismatch; a clock stamped into accumulators, meta, or the seal ledger shows up in the
	// data-keyspace bytes.
	let (live_emitted, live) = drive_sealing(CLOCK_LIVE_MS);
	let (replay_emitted, replay) = drive_sealing(CLOCK_REPLAY_MS);

	assert!(live.armed_timers > 0, "no seal timer was armed, so this run proves nothing about seal timing");
	assert!(!live.data_values.is_empty(), "no data-keyspace state survived, so the byte comparison is vacuous");
	assert_eq!(live_emitted, replay_emitted, "the emitted diffs must not depend on the wall clock");
	assert_eq!(
		live.keys, replay.keys,
		"the state keys (including armed timer instants) must derive from row time, not the clock"
	);
	assert_eq!(
		live.data_values, replay.data_values,
		"the persisted window state must be byte-identical across replays"
	);
}

fn drive_count(clock_ms: u64) -> (String, Snapshot) {
	let kind = WindowKind::Tumbling {
		size: WindowSize::Count(2),
	};
	let mut h = harness(kind, clock_ms);
	let emitted = format!(
		"{:?}",
		h.apply(generator::insert(vec![
			generator::row(RowNumber(1), GROUP, 5, at_millis(40_000)),
			generator::row(RowNumber(2), GROUP, 7, at_millis(41_000)),
			generator::row(RowNumber(3), GROUP, 9, at_millis(39_000)),
		]))
		.expect("the count rows apply")
	);
	let snap = snapshot(&mut h);
	(emitted, snap)
}

#[test]
fn count_window_meta_takes_the_max_row_time_never_the_clock() {
	// A count window has no time column driving its spans, which historically made its
	// EngineMeta.last_event_time a wall-clock stamp - the last clock read that reached persisted
	// window state. It must instead derive from row time, and per BUCKET rather than per batch:
	// a batch-max stamp is clock-free but changes with batch boundaries, which the replay
	// determinism gate forbids. Rows 1 and 2 (40_000, 41_000) fill the first count-2 bucket, so
	// its meta reads 41_000; row 3 (39_000) opens the second, so its meta reads 39_000. Both
	// mock clocks sit far away so a clock read cannot masquerade as the right answer.
	let (_, live) = drive_count(CLOCK_LIVE_MS);

	assert!(!live.metas.is_empty(), "no EngineMeta row was persisted, so last_event_time was never exercised");
	let mut metas = live.metas.clone();
	metas.sort_unstable();
	assert_eq!(
		metas,
		vec![at_millis(39_000).to_order(), at_millis(41_000).to_order()],
		"last_event_time must be each bucket's max row time, never the clock ({CLOCK_LIVE_MS}) and \
		 never the batch max (41_000 everywhere)"
	);
}

#[test]
fn count_window_state_is_byte_identical_across_clocks() {
	// The state-byte half of the count-window contract: identical inputs under a live clock and
	// a century-later replay clock must leave byte-identical persisted window state and identical
	// emitted diffs, or replaying a recorded stream would diverge.
	let (live_emitted, live) = drive_count(CLOCK_LIVE_MS);
	let (replay_emitted, replay) = drive_count(CLOCK_REPLAY_MS);

	assert!(!live.data_values.is_empty(), "no data-keyspace state survived, so the byte comparison is vacuous");
	assert_eq!(live_emitted, replay_emitted, "the emitted diffs must not depend on the wall clock");
	assert_eq!(live.keys, replay.keys, "the state keys must derive from row data, not the clock");
	assert_eq!(
		live.data_values, replay.data_values,
		"the persisted count-window state must be byte-identical across replays"
	);
}
