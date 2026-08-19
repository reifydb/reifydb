// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "chaos")]

#[path = "chaos/framework/mod.rs"]
mod framework;
#[path = "chaos/operators/mod.rs"]
mod operators;

use std::collections::BTreeSet;

use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::change::Diff,
	value::column::columns::Columns,
};
use reifydb_testing_chaos::{
	fuzz::run_reported,
	operator::{
		session::Session,
		subject::Subject,
		view::{MaterializedView, RowKey},
		workload::Workload,
	},
};
use reifydb_testing_macro::chaos_test;
use reifydb_value::value::{Value, datetime::DateTime, duration::Duration, row_number::RowNumber};

use crate::{
	framework::{generator, harness::Harness},
	operators::{
		aggregate::{
			Agg,
			workload::{AggregateRow, AggregateWorkload},
		},
		append::workload::{AppendRow, AppendWorkload},
		distinct::workload::{DistinctRow, DistinctWorkload},
		gate::workload::{GateRow, GateWorkload},
		join::{
			Variant,
			workload::{JoinRow, JoinWorkload, Side},
		},
		pipeline::Chain,
		rowwise::{
			Shape,
			workload::{RowwiseRow, RowwiseWorkload},
		},
		sink::Layout,
		take::workload::{TakeRow, TakeWorkload},
		window::{WindowSpec, build, grid::Fold},
	},
};

// Not free to pick: an operator's own lateness span overrides whatever the harness declares.
const WINDOW_SECS: i64 = 60;

fn tumbling_sum() -> WindowSpec {
	WindowSpec {
		kind: WindowKind::Tumbling {
			size: WindowSize::Duration(Duration::from_seconds(WINDOW_SECS).unwrap()),
		},
		group_by: "g",
		aggregations: "total: math::sum(v)",
		lateness: Some(Duration::default()),
	}
}

#[test]
fn a_window_operator_can_be_built_and_driven() {
	let spec = tumbling_sum();
	let mut harness = Harness::new(|runtime| build(&spec, runtime));

	let at = DateTime::from_epoch_millis(60_000).unwrap();
	let change = generator::insert(vec![
		generator::row(RowNumber(1), 1, 10, at),
		generator::row(RowNumber(2), 1, 5, at),
	]);

	let out = harness.apply(change).expect("apply must succeed");

	assert!(
		!out.diffs.is_empty(),
		"a window fed two rows in one group must emit at least one diff; got none, which means the \
		 operator was built but never routed the batch"
	);
}

chaos_test!(window_tumbling_sum_chaos, |seed| {
	operators::window::tumbling::drive(
		seed,
		operators::window::tumbling::Params {
			size_secs: 60,
			lateness_secs: 0,
			groups: 4,
			steps: 40,
			max_batch: 5,
			coord_span_ms: 600_000,
			remove_pct: 30,
			update_pct: 20,
			seal_pct: 20,
		},
	);
});

chaos_test!(window_tumbling_seal_chaos, |seed| {
	operators::window::tumbling::drive(
		seed,
		operators::window::tumbling::Params {
			size_secs: 30,
			lateness_secs: 45,
			groups: 3,
			steps: 60,
			max_batch: 4,
			coord_span_ms: 400_000,
			remove_pct: 25,
			update_pct: 15,
			seal_pct: 30,
		},
	);
});

chaos_test!(window_sliding_sum_chaos, |seed| {
	operators::window::sliding::drive(
		seed,
		operators::window::sliding::Params {
			size_secs: 60,
			slide_secs: 15,
			lateness_secs: 0,
			groups: 4,
			steps: 40,
			max_batch: 5,
			coord_span_ms: 600_000,
			remove_pct: 30,
			update_pct: 20,
			seal_pct: 20,
		},
	);
});

chaos_test!(window_rolling_sum_chaos, |seed| {
	operators::window::rolling::drive(
		seed,
		operators::window::rolling::Params {
			size_secs: 60,
			lateness_secs: 0,
			groups: 4,
			steps: 40,
			max_batch: 5,
			coord_span_ms: 600_000,
			remove_pct: 30,
			update_pct: 20,
			seal_pct: 20,
		},
	);
});

chaos_test!(window_rolling_seal_chaos, |seed| {
	operators::window::rolling::drive(
		seed,
		operators::window::rolling::Params {
			size_secs: 30,
			lateness_secs: 45,
			groups: 3,
			steps: 60,
			max_batch: 4,
			coord_span_ms: 400_000,
			remove_pct: 25,
			update_pct: 15,
			seal_pct: 30,
		},
	);
});

chaos_test!(window_sliding_seal_chaos, |seed| {
	operators::window::sliding::drive(
		seed,
		operators::window::sliding::Params {
			size_secs: 30,
			slide_secs: 10,
			lateness_secs: 45,
			groups: 3,
			steps: 60,
			max_batch: 4,
			coord_span_ms: 400_000,
			remove_pct: 25,
			update_pct: 15,
			seal_pct: 30,
		},
	);
});

chaos_test!(window_tumbling_flow_shaped_chaos, |seed| {
	// A duplicate update must net to no change in the aggregate, and an update split into
	// remove-then-insert must land the same total the update would have.
	operators::window::tumbling::drive_flow_shaped(
		seed,
		operators::window::tumbling::Params {
			size_secs: 30,
			lateness_secs: 15,
			groups: 3,
			steps: 60,
			max_batch: 5,
			coord_span_ms: 400_000,
			remove_pct: 20,
			update_pct: 35,
			seal_pct: 20,
		},
	);
});

fn window_fold_params(lateness_secs: u64) -> operators::window::tumbling::Params {
	operators::window::tumbling::Params {
		size_secs: 30,
		lateness_secs,
		groups: 3,
		steps: 60,
		max_batch: 4,
		coord_span_ms: 400_000,
		remove_pct: 30,
		update_pct: 25,
		seal_pct: 20,
	}
}

chaos_test!(window_tumbling_min_zero_lateness_chaos, |seed| {
	// Zero lateness keeps min invertible, so it runs on the multiset and a retraction of the current
	// minimum is what forces the full recompute.
	operators::window::tumbling::drive_folded(seed, window_fold_params(0), Fold::Min);
});

chaos_test!(window_tumbling_max_zero_lateness_chaos, |seed| {
	operators::window::tumbling::drive_folded(seed, window_fold_params(0), Fold::Max);
});

chaos_test!(window_tumbling_min_sealed_chaos, |seed| {
	// Seal makes min non-invertible, so the operator switches to the sealing accumulator. No sweep
	// reached that path while sum was the only fold: sum is invertible at every lateness setting.
	operators::window::tumbling::drive_folded(seed, window_fold_params(45), Fold::Min);
});

chaos_test!(window_tumbling_max_sealed_chaos, |seed| {
	operators::window::tumbling::drive_folded(seed, window_fold_params(45), Fold::Max);
});

#[test]
fn the_window_folds_are_stated_independently_of_the_slots_they_check() {
	// Same role as the aggregate's width pin: a mismatch between what the slot emits and what the
	// oracle renders shows up as a divergence whose two sides print identically. Sum promotes an int8
	// input to int16; min and max hand the input width back.
	assert_eq!(Fold::Sum.apply(&[1, 2, 3]), Value::Int16(6));
	assert_eq!(Fold::Min.apply(&[3, 1, 2]), Value::Int8(1));
	assert_eq!(Fold::Max.apply(&[3, 1, 2]), Value::Int8(3));
}

fn rolling_fold_params(lateness_secs: u64) -> operators::window::rolling::Params {
	operators::window::rolling::Params {
		size_secs: 30,
		lateness_secs,
		groups: 3,
		steps: 60,
		max_batch: 4,
		coord_span_ms: 400_000,
		remove_pct: 30,
		update_pct: 25,
		seal_pct: 30,
	}
}

chaos_test!(window_rolling_min_sealed_chaos, |seed| {
	// The only path that reaches the sealing accumulator's SEALED half. Min is non-invertible under
	// lateness, so the slot is a SealingMin; rolling is the kind whose seal driver ages entries out of
	// the seal tail, which is what fills `sealed`. Tumbling reaches the container and its tail only.
	operators::window::rolling::drive_folded(seed, rolling_fold_params(45), Fold::Min);
});

chaos_test!(window_rolling_max_sealed_chaos, |seed| {
	operators::window::rolling::drive_folded(seed, rolling_fold_params(45), Fold::Max);
});

chaos_test!(window_rolling_min_zero_lateness_chaos, |seed| {
	// Zero lateness keeps min invertible, so this drives the multiset instead and is the control that
	// says the sealed sweeps above are testing something different.
	operators::window::rolling::drive_folded(seed, rolling_fold_params(0), Fold::Min);
});

chaos_test!(window_tumbling_random_chaos, |seed| {
	operators::window::tumbling::drive_random(seed);
});

chaos_test!(window_sliding_random_chaos, |seed| {
	operators::window::sliding::drive_random(seed);
});

chaos_test!(window_rolling_random_chaos, |seed| {
	operators::window::rolling::drive_random(seed);
});

#[test]
fn the_random_sweeps_reach_the_configurations_that_found_defects() {
	// A parameter generator can degenerate silently: narrow a range and every sweep still passes
	// while no longer covering the region that mattered. Pinning the regions by name makes
	// shrinking one fail here rather than go quiet.
	const SEEDS: u64 = 512;

	let mut zero_lateness = 0;
	let mut lateness_over_size = 0;
	let mut tumbling_sizes = std::collections::BTreeSet::new();
	for seed in 0..SEEDS {
		let (_, params) = operators::window::tumbling::random_params(seed);
		tumbling_sizes.insert(params.size_secs);
		if params.lateness_secs == 0 {
			zero_lateness += 1;
		}
		if params.lateness_secs > params.size_secs {
			lateness_over_size += 1;
		}
		assert!(
			params.seal_pct + params.remove_pct + params.update_pct <= 85,
			"inserts must keep at least a 15% share or the corpus never grows: {params:?}"
		);
		assert!(params.steps > 0 && params.max_batch > 0 && params.groups > 0, "degenerate draw: {params:?}");
	}
	assert!(
		zero_lateness > 0,
		"no zero-lateness draw in {SEEDS} seeds; the closes-immediately boundary is uncovered"
	);
	assert!(
		lateness_over_size > 0,
		"no lateness-wider-than-size draw in {SEEDS} seeds; that band is where the rolling operator was \
		 withdrawing live groups"
	);
	assert!(tumbling_sizes.len() >= 5, "sizes collapsed to {tumbling_sizes:?}; the sweep stopped spanning scales");

	let mut divides = 0;
	let mut leaves_remainder = 0;
	for seed in 0..SEEDS {
		let (_, params) = operators::window::sliding::random_params(seed);
		assert!(
			params.slide_secs < params.size_secs && params.slide_secs > 0,
			"a sliding sweep must stay in the overlapping region the planner allows: {params:?}"
		);
		if params.size_secs % params.slide_secs == 0 {
			divides += 1;
		} else {
			leaves_remainder += 1;
		}
	}
	assert!(divides > 0, "no slide dividing its size in {SEEDS} seeds");
	assert!(leaves_remainder > 0, "no slide leaving a remainder in {SEEDS} seeds; coverage is uniform-only");

	// Count sweeps bucket on a per-group ordinal rather than a coordinate, so the regions worth
	// pinning are different ones.
	let mut tumbling_counts = std::collections::BTreeSet::new();
	let mut rolling_capacities = std::collections::BTreeSet::new();
	for seed in 0..SEEDS {
		let (_, tumbling) = operators::window::tumbling::random_count_params(seed);
		let (_, rolling) = operators::window::rolling::random_count_params(seed);
		tumbling_counts.insert(tumbling.size_count);
		rolling_capacities.insert(rolling.size_count);
		for (kind, size, steps, max_batch, groups, remove, update) in [
			(
				"tumbling",
				tumbling.size_count,
				tumbling.steps,
				tumbling.max_batch,
				tumbling.groups,
				tumbling.remove_pct,
				tumbling.update_pct,
			),
			(
				"rolling",
				rolling.size_count,
				rolling.steps,
				rolling.max_batch,
				rolling.groups,
				rolling.remove_pct,
				rolling.update_pct,
			),
		] {
			assert!(size > 0, "a {kind} count window of size zero has no defined bucketing");
			assert!(
				steps > 0 && max_batch > 0 && groups > 0,
				"degenerate {kind} count draw at seed {seed}"
			);
			assert!(
				remove + update <= 85,
				"inserts must keep a share or a {kind} count sweep never fills a window"
			);
		}
	}
	assert!(
		tumbling_counts.contains(&1),
		"no size-of-one draw in {SEEDS} seeds; the one-row-per-window boundary is uncovered"
	);
	assert!(tumbling_counts.len() >= 4, "count sizes collapsed to {tumbling_counts:?}");
	assert!(
		rolling_capacities.contains(&1),
		"no capacity-of-one draw in {SEEDS} seeds; retracting an already-evicted row goes untested"
	);

	let mut count_divides = 0;
	let mut count_leaves_remainder = 0;
	let mut maximal_overlap = 0;
	for seed in 0..SEEDS {
		let (_, params) = operators::window::sliding::random_count_params(seed);
		assert!(
			params.slide_count < params.size_count && params.slide_count > 0,
			"a sliding count sweep must stay in the overlapping region the planner allows: {params:?}"
		);
		if params.slide_count == 1 {
			maximal_overlap += 1;
		}
		if params.size_count % params.slide_count == 0 {
			count_divides += 1;
		} else {
			count_leaves_remainder += 1;
		}
	}
	assert!(
		maximal_overlap > 0,
		"no slide-of-one draw in {SEEDS} seeds; a row landing in `size` windows at once is uncovered"
	);
	assert!(count_divides > 0, "no count slide dividing its size in {SEEDS} seeds");
	assert!(count_leaves_remainder > 0, "no count slide leaving a remainder in {SEEDS} seeds");
}

#[test]
fn a_fuzzed_sweep_failure_is_re_raised_after_reporting() {
	// If run_reported swallowed the panic instead of re-raising it, all three random sweeps would
	// report green forever and nothing would say otherwise.
	let params = operators::window::tumbling::random_params(0).1;

	// nextest gives each test its own process, so swapping the hook cannot disturb anything else.
	let previous = std::panic::take_hook();
	std::panic::set_hook(Box::new(|_| {}));
	let outcome = std::panic::catch_unwind(|| {
		run_reported("format_check", 1234, &params, || panic!("deliberate"));
	});
	std::panic::set_hook(previous);

	assert!(outcome.is_err(), "run_reported must re-raise, otherwise a failing sweep reports success");
}

chaos_test!(window_tumbling_count_chaos, |seed| {
	operators::window::tumbling::drive_count(
		seed,
		operators::window::tumbling::CountParams {
			size_count: 4,
			groups: 3,
			steps: 60,
			max_batch: 4,
			coord_span_ms: 400_000,
			remove_pct: 25,
			update_pct: 15,
		},
	);
});

chaos_test!(window_sliding_count_chaos, |seed| {
	operators::window::sliding::drive_count(
		seed,
		operators::window::sliding::CountParams {
			size_count: 4,
			slide_count: 2,
			groups: 3,
			steps: 60,
			max_batch: 4,
			coord_span_ms: 400_000,
			remove_pct: 25,
			update_pct: 15,
		},
	);
});

chaos_test!(window_rolling_count_chaos, |seed| {
	operators::window::rolling::drive_count(
		seed,
		operators::window::rolling::CountParams {
			size_count: 4,
			groups: 3,
			steps: 60,
			max_batch: 4,
			coord_span_ms: 400_000,
			remove_pct: 25,
			update_pct: 15,
		},
	);
});

chaos_test!(window_tumbling_count_random_chaos, |seed| {
	operators::window::tumbling::drive_count_random(seed);
});

chaos_test!(window_sliding_count_random_chaos, |seed| {
	operators::window::sliding::drive_count_random(seed);
});

chaos_test!(window_rolling_count_random_chaos, |seed| {
	operators::window::rolling::drive_count_random(seed);
});

#[test]
fn a_join_operator_can_be_built_and_driven() {
	// If the corpus stopped tagging diff origins, or tagged both sides the same, every join sweep
	// below would still run and would simply never join anything.
	let workload = JoinWorkload {
		keys: 1,
		right_pct: 0,
		none_pct: 0,
		rekey_pct: 0,
		coord_span_ms: 1,
		flip_definedness: false,
	};
	let mut harness =
		Harness::with_engine(|engine, _| operators::join::build(engine, Variant::inner(), None, None));

	let left = JoinRow {
		side: Side::Left,
		number: RowNumber(1),
		key: Some(1),
		value: 10,
		coord_ms: 1_000,
	};
	let right = JoinRow {
		side: Side::Right,
		number: RowNumber(2),
		key: Some(1),
		value: 20,
		coord_ms: 2_000,
	};

	assert!(
		harness.apply(workload.insert(&[left])).expect("left apply must succeed").diffs.is_empty(),
		"an inner join has nothing to emit for a left row with no right row to match"
	);
	let out = harness.apply(workload.insert(&[right])).expect("right apply must succeed");
	assert!(!out.diffs.is_empty(), "the right row completes the pair and the join must publish it");
}

#[test]
fn an_append_operator_can_be_built_and_driven() {
	// Append refuses a diff whose origin it cannot resolve to one of its inputs, so a mistagged
	// corpus fails here rather than quietly driving nothing.
	let workload = AppendWorkload {
		inputs: 2,
		row_space: 4,
	};
	let mut harness = Harness::new(|_| operators::append::build(2));

	let first = AppendRow {
		input: 0,
		source: RowNumber(1),
		value: 10,
	};
	let second = AppendRow {
		input: 1,
		source: RowNumber(1),
		value: 20,
	};

	let out = harness.apply(workload.insert(&[first, second])).expect("apply must succeed");
	assert_eq!(out.diffs.len(), 2, "two inputs cannot share a diff, so one row from each is two diffs");
	let numbers: Vec<RowNumber> =
		out.diffs.iter().flat_map(|diff| diff.post().unwrap().row_numbers().to_vec()).collect();
	assert_ne!(
		numbers[0], numbers[1],
		"row 1 on two different inputs is two unrelated rows and must not collapse onto one output row"
	);
}

chaos_test!(join_inner_chaos, |seed| {
	operators::join::drive(seed, operators::join::matched_params(Variant::inner()));
});

chaos_test!(join_left_chaos, |seed| {
	operators::join::drive(seed, operators::join::matched_params(Variant::left()));
});

chaos_test!(join_latest_inner_chaos, |seed| {
	operators::join::drive(seed, operators::join::matched_params(Variant::inner().with_latest()));
});

chaos_test!(join_latest_left_chaos, |seed| {
	operators::join::drive(seed, operators::join::matched_params(Variant::left().with_latest()));
});

chaos_test!(join_left_keys_1_chaos, |seed| {
	// A single key is the widest cartesian product a hash join can be asked for and the slot a
	// latest join rewrites on every right arrival; the sweeps above rarely land there.
	operators::join::drive(
		seed,
		operators::join::Params {
			variant: Variant::left(),
			keys: 1,
			right_pct: 45,
			none_pct: 0,
			rekey_pct: 0,
			steps: 50,
			max_batch: 6,
			max_live: 24,
			remove_pct: 30,
			update_pct: 30,
			coord_span_ms: 400_000,
			left_ttl: None,
			right_ttl: None,
			static_right: 0,
		},
	);
});

chaos_test!(join_random_chaos, |seed| {
	operators::join::drive_random(seed);
});

chaos_test!(append_inputs_3_row_space_8_chaos, |seed| {
	operators::append::drive(
		seed,
		operators::append::Params {
			inputs: 3,
			row_space: 8,
			steps: 60,
			max_batch: 5,
			max_live: 40,
			remove_pct: 25,
			update_pct: 30,
		},
	);
});

chaos_test!(append_random_chaos, |seed| {
	operators::append::drive_random(seed);
});

fn aggregate_params(agg: Agg, value_ceiling: i64) -> operators::aggregate::Params {
	operators::aggregate::Params {
		agg,
		groups: 4,
		value_ceiling,
		steps: 60,
		max_batch: 5,
		max_live: 40,
		remove_pct: 25,
		update_pct: 30,
	}
}

// Wide enough that a group rarely holds two equal values, so the accumulator stays on its incremental
// path and the sweep is about the fold itself rather than about recompute.
const AGGREGATE_SPREAD: i64 = 20;

chaos_test!(aggregate_sum_chaos, |seed| {
	operators::aggregate::drive(seed, aggregate_params(Agg::Sum, AGGREGATE_SPREAD));
});

chaos_test!(aggregate_count_chaos, |seed| {
	operators::aggregate::drive(seed, aggregate_params(Agg::Count, AGGREGATE_SPREAD));
});

chaos_test!(aggregate_min_chaos, |seed| {
	operators::aggregate::drive(seed, aggregate_params(Agg::Min, AGGREGATE_SPREAD));
});

chaos_test!(aggregate_max_chaos, |seed| {
	operators::aggregate::drive(seed, aggregate_params(Agg::Max, AGGREGATE_SPREAD));
});

chaos_test!(aggregate_min_ties_chaos, |seed| {
	// Every row in a group carries the same value, so every retraction takes out the value the group
	// currently reports. `Min::invert` refuses that case and the accumulator must fall back to a full
	// recompute - the path a spread-out corpus almost never reaches.
	operators::aggregate::drive(seed, aggregate_params(Agg::Min, 1));
});

chaos_test!(aggregate_max_ties_chaos, |seed| {
	operators::aggregate::drive(seed, aggregate_params(Agg::Max, 1));
});

chaos_test!(aggregate_single_group_chaos, |seed| {
	// One group means every row contends for the same accumulator slot, which is where a lost
	// retraction shows up as a wrong total rather than as a stranded group.
	operators::aggregate::drive(
		seed,
		operators::aggregate::Params {
			groups: 1,
			..aggregate_params(Agg::Sum, 4)
		},
	);
});

chaos_test!(aggregate_random_chaos, |seed| {
	operators::aggregate::drive_random(seed);
});

#[test]
fn an_aggregate_operator_can_be_built_and_driven() {
	// A build failure inside a chaos_test reports as a divergence on step zero, which reads as an
	// oracle defect. This separates "the operator cannot be constructed" from "the operator is wrong".
	let mut harness = Harness::new(|runtime| operators::aggregate::build(Agg::Sum, runtime));
	let workload = AggregateWorkload {
		groups: 2,
		value_ceiling: 10,
	};

	let out = harness
		.apply(workload.insert(&[
			AggregateRow {
				number: RowNumber(1),
				group: 1,
				value: 7,
			},
			AggregateRow {
				number: RowNumber(2),
				group: 1,
				value: 5,
			},
		]))
		.expect("apply must succeed");

	assert_eq!(out.diffs.len(), 1, "two rows of one group are one aggregate row, got {:?}", out.diffs);
}

#[test]
fn the_aggregate_fold_is_stated_independently_of_the_monoid_it_checks() {
	// The oracle's whole value rests on not being the operator's own arithmetic, so what it computes
	// is worth stating outright: sum promotes an int8 input to int16, count reports int8 whatever it
	// counted, and min and max hand the input width back.
	assert_eq!(Agg::Sum.fold(&[1, 2, 3]), Value::Int16(6));
	assert_eq!(Agg::Count.fold(&[1, 2, 3]), Value::Int8(3));
	assert_eq!(Agg::Min.fold(&[3, 1, 2]), Value::Int8(1));
	assert_eq!(Agg::Max.fold(&[3, 1, 2]), Value::Int8(3));

	// Sum must not overflow the way an int8 accumulator would, which is the reason for the promotion.
	assert_eq!(Agg::Sum.fold(&[i64::MAX, i64::MAX]), Value::Int16(i64::MAX as i128 * 2));
}

#[test]
fn the_operator_emits_the_value_widths_the_oracle_renders() {
	// `values_match` falls through to `a == b`, so an Int8 and a Uint8 holding the same number are a
	// divergence whose two sides print identically - "oracle 1, operator 1, diff +0", which reads as
	// a framework defect rather than a width mismatch. This is the bridge between the oracle's fold
	// and the operator, asserted on the type rather than the number so it fails legibly.
	//
	// Notably these are NOT the widths `Monoid::state_type` declares: it says Uint8 for count. The
	// aggregate engine routes representable shapes through `SlotKind` and never consults the monoid,
	// so `state_type` has no callers anywhere in the workspace and its answer is stale.
	for agg in operators::aggregate::MATRIX {
		let mut harness = Harness::new(|runtime| operators::aggregate::build(agg, runtime));
		let workload = AggregateWorkload {
			groups: 1,
			value_ceiling: 10,
		};

		let out = harness
			.apply(workload.insert(&[AggregateRow {
				number: RowNumber(1),
				group: 1,
				value: 7,
			}]))
			.expect("apply must succeed");

		let post = out.diffs.iter().next().and_then(|diff| diff.post()).expect("one row is one aggregate row");
		let names: Vec<String> = post.names.iter().map(|name| name.text().to_string()).collect();
		let idx = names.iter().position(|name| name == agg.column()).unwrap_or_else(|| {
			panic!("{} must publish a {} column, got {names:?}", agg.label(), agg.column())
		});

		let emitted = post.columns[idx].get_value(0);
		let rendered = agg.fold(&[7]);
		assert_eq!(
			std::mem::discriminant(&emitted),
			std::mem::discriminant(&rendered),
			"{} emits {emitted:?} but the oracle renders {rendered:?}; the widths must agree or every \
			 comparison in the sweep fails with two identical-looking numbers",
			agg.label()
		);
		assert_eq!(emitted, rendered, "{} must agree on the value too", agg.label());
	}
}

fn pipeline_params(chain: operators::pipeline::Chain) -> operators::pipeline::Params {
	operators::pipeline::Params {
		chain,
		groups: 4,
		value_ceiling: 100,
		steps: 60,
		max_batch: 5,
		max_live: 40,
		remove_pct: 25,
		update_pct: 35,
	}
}

chaos_test!(pipeline_filter_aggregate_chaos, |seed| {
	// Membership changes upstream become inserts and removes the corpus never issued, and the
	// aggregate has to fold them into the right total. A filter that mislabels a crossing gives the
	// aggregate a contribution to add twice or none to subtract.
	operators::pipeline::drive(
		seed,
		pipeline_params(Chain::Filter {
			threshold: 50,
		}),
	);
});

chaos_test!(pipeline_map_aggregate_chaos, |seed| {
	// The chain that exists because of a mutation that survived: an aggregate reads the `pre` of an
	// update to retract the old contribution, so a stage publishing `post` as its own `pre` leaves the
	// old value in the total forever. Invisible to a single-operator view comparison, visible here.
	operators::pipeline::drive(seed, pipeline_params(Chain::Map));
});

chaos_test!(pipeline_gate_aggregate_chaos, |seed| {
	// A latch feeding a fold. A row that falls back below the threshold must keep contributing, and a
	// filter-shaped gate would quietly subtract it.
	operators::pipeline::drive(
		seed,
		pipeline_params(Chain::Gate {
			threshold: 50,
		}),
	);
});

chaos_test!(pipeline_random_chaos, |seed| {
	operators::pipeline::drive_random(seed);
});

#[test]
fn the_pipeline_sweep_reaches_every_chain() {
	let mut seen: BTreeSet<&'static str> = BTreeSet::new();
	for seed in 0..400u64 {
		let (_, params) = operators::pipeline::random_params(seed);
		seen.insert(params.chain.label());
	}
	assert_eq!(seen.len(), operators::pipeline::MATRIX.len(), "the sweep must reach every chain, saw {seen:?}");
}

fn rowwise_params(shape: operators::rowwise::Shape) -> operators::rowwise::Params {
	operators::rowwise::Params {
		shape,
		value_ceiling: 100,
		steps: 60,
		max_batch: 5,
		max_live: 40,
		remove_pct: 25,
		update_pct: 35,
	}
}

chaos_test!(filter_midpoint_chaos, |seed| {
	// Half the corpus passes, so updates cross the predicate in both directions often. Crossing is the
	// whole risk surface: an update that enters the result must be published as an insert and one that
	// leaves it as a remove, and emitting an update in either case gives the sink a row it never had.
	operators::rowwise::drive(
		seed,
		rowwise_params(Shape::Filter {
			threshold: 50,
		}),
	);
});

chaos_test!(filter_strict_chaos, |seed| {
	// Almost nothing passes, so the result set is nearly always empty and the rare admission is the
	// only thing the sweep has to get right.
	operators::rowwise::drive(
		seed,
		rowwise_params(Shape::Filter {
			threshold: 95,
		}),
	);
});

chaos_test!(filter_permissive_chaos, |seed| {
	// Everything passes, so filter degenerates to a pass-through and the sweep checks it neither drops
	// nor duplicates while doing nothing.
	operators::rowwise::drive(
		seed,
		rowwise_params(Shape::Filter {
			threshold: 0,
		}),
	);
});

chaos_test!(map_chaos, |seed| {
	operators::rowwise::drive(seed, rowwise_params(Shape::Map));
});

chaos_test!(extend_chaos, |seed| {
	operators::rowwise::drive(seed, rowwise_params(Shape::Extend));
});

chaos_test!(rowwise_random_chaos, |seed| {
	operators::rowwise::drive_random(seed);
});

#[test]
fn a_filter_publishes_a_crossing_as_an_insert_or_a_remove_not_an_update() {
	// The one thing a filter can get wrong that a view-level comparison would not always catch: a row
	// that enters or leaves the result must change membership, not merely change value. A sink handed
	// an update for a row it is not holding cannot fold it.
	let mut harness = Harness::new(|runtime| {
		operators::rowwise::build(
			Shape::Filter {
				threshold: 50,
			},
			runtime,
		)
	});
	let workload = RowwiseWorkload {
		value_ceiling: 100,
		shape: Shape::Filter {
			threshold: 50,
		},
	};
	let row = |number: u64, value: i64| RowwiseRow {
		number: RowNumber(number),
		value,
	};

	let refused = harness.apply(workload.insert(&[row(1, 10)])).expect("apply must succeed");
	assert!(refused.diffs.is_empty(), "a row below the predicate must not be published, got {:?}", refused.diffs);

	let entering = harness.apply(workload.update(&row(1, 10), &row(1, 60))).expect("apply must succeed");
	assert!(
		matches!(entering.diffs.as_slice(), [Diff::Insert { .. }]),
		"a row crossing into the result must arrive as an insert, got {:?}",
		entering.diffs
	);

	let staying = harness.apply(workload.update(&row(1, 60), &row(1, 70))).expect("apply must succeed");
	assert!(
		matches!(staying.diffs.as_slice(), [Diff::Update { .. }]),
		"a row that was in the result and stays in it is an update, got {:?}",
		staying.diffs
	);

	let leaving = harness.apply(workload.update(&row(1, 70), &row(1, 10))).expect("apply must succeed");
	assert!(
		matches!(leaving.diffs.as_slice(), [Diff::Remove { .. }]),
		"a row crossing out of the result must leave as a remove, got {:?}",
		leaving.diffs
	);
}

#[test]
fn the_rowwise_operators_emit_what_their_oracles_render() {
	// The bridge between each shape's RQL and its hand-written Rust answer, asserted on one row so a
	// disagreement reports as a type or arithmetic mismatch here rather than as a divergent view
	// inside a sweep. Widths matter: `values_match` compares Values, so an Int8 and an Int16 holding
	// the same number are a divergence whose two sides print identically.
	for shape in operators::rowwise::MATRIX {
		let mut harness = Harness::new(|runtime| operators::rowwise::build(shape, runtime));
		let workload = RowwiseWorkload {
			value_ceiling: 100,
			shape,
		};
		let row = RowwiseRow {
			number: RowNumber(1),
			value: 60,
		};

		let out = harness.apply(workload.insert(std::slice::from_ref(&row))).expect("apply must succeed");
		let post = out.diffs.iter().next().and_then(|diff| diff.post()).unwrap_or_else(|| {
			panic!("{} must publish a row that passes, got {:?}", shape.label(), out.diffs)
		});

		let emitted: Vec<Value> = post.columns.iter().map(|column| column.get_value(0)).collect();
		assert_eq!(
			emitted,
			shape.render(&row),
			"{} emits {emitted:?} but its oracle renders {:?}",
			shape.label(),
			shape.render(&row)
		);
	}
}

#[test]
fn a_rowwise_update_carries_the_previous_row_as_its_pre() {
	// Found by mutation: making map publish `post` as its own `pre` passed all 64 sweep iterations.
	// A view-folding oracle cannot see this. The session folds an update by row number and keeps the
	// post, so a wrong pre changes nothing it can compare - the same blind spot the snapshot join's
	// retraction has, and it is guarded the same way, by reading the returned diffs directly.
	//
	// It matters for the same reason it does there: a consumer that builds its retraction from
	// pre_data verbatim, as chaindex block_trade does, subtracts whatever pre says. A pre equal to
	// post subtracts the row that was just added and leaves the one that should have gone.
	for shape in operators::rowwise::MATRIX {
		let mut harness = Harness::new(|runtime| operators::rowwise::build(shape, runtime));
		let workload = RowwiseWorkload {
			value_ceiling: 100,
			shape,
		};
		let before = RowwiseRow {
			number: RowNumber(1),
			value: 60,
		};
		let after = RowwiseRow {
			number: RowNumber(1),
			value: 80,
		};

		harness.apply(workload.insert(std::slice::from_ref(&before))).expect("apply must succeed");
		let out = harness.apply(workload.update(&before, &after)).expect("apply must succeed");

		// Both values sit above the filter's threshold, so every shape stays in its result and the
		// change is an update rather than a crossing. Without that this would be asserting the
		// membership transition instead of the content of `pre`.
		let [
			Diff::Update {
				pre,
				post,
				..
			},
		] = out.diffs.as_slice()
		else {
			panic!("{} must publish one update, got {:?}", shape.label(), out.diffs);
		};

		let emitted_pre: Vec<Value> = pre.columns.iter().map(|column| column.get_value(0)).collect();
		let emitted_post: Vec<Value> = post.columns.iter().map(|column| column.get_value(0)).collect();
		assert_eq!(
			emitted_pre,
			shape.render(&before),
			"{}'s update must retract the row as it was published before, not as it is now",
			shape.label()
		);
		assert_eq!(emitted_post, shape.render(&after), "{}'s update must publish the new row", shape.label());
		assert_ne!(emitted_pre, emitted_post, "the two halves must differ, or this asserts nothing");
	}
}

#[test]
fn the_rowwise_sweep_reaches_every_shape_and_both_ends_of_the_predicate() {
	let mut seen: BTreeSet<&'static str> = BTreeSet::new();
	let mut permissive = 0;
	let mut strict = 0;
	for seed in 0..400u64 {
		let (_, params) = operators::rowwise::random_params(seed);
		seen.insert(params.shape.label());
		if let Shape::Filter {
			threshold,
		} = params.shape
		{
			if threshold * 4 <= params.value_ceiling {
				permissive += 1;
			}
			if threshold * 4 >= params.value_ceiling * 3 {
				strict += 1;
			}
		}
	}
	assert_eq!(seen.len(), 3, "the sweep must reach filter, map and extend, saw {seen:?}");
	assert!(permissive > 0, "the sweep must sometimes pass nearly everything");
	assert!(strict > 0, "the sweep must sometimes pass nearly nothing");
}

fn gate_params(threshold: i64) -> operators::gate::Params {
	operators::gate::Params {
		threshold,
		value_ceiling: 100,
		steps: 60,
		max_batch: 5,
		max_live: 40,
		remove_pct: 25,
		update_pct: 35,
	}
}

chaos_test!(gate_midpoint_chaos, |seed| {
	// Half the corpus passes on arrival and half does not, so the sweep spends its time on both the
	// admit-on-insert and the admit-later-by-update paths.
	operators::gate::drive(seed, gate_params(50));
});

chaos_test!(gate_strict_chaos, |seed| {
	// Almost nothing passes on arrival, so nearly every admission happens later through an update
	// that crosses the threshold. That is the path a filter-shaped implementation gets wrong.
	operators::gate::drive(seed, gate_params(95));
});

chaos_test!(gate_permissive_chaos, |seed| {
	// Everything passes immediately, so the gate degenerates to a pass-through and the sweep is
	// checking that it does not drop or duplicate anything while doing nothing.
	operators::gate::drive(seed, gate_params(0));
});

chaos_test!(gate_random_chaos, |seed| {
	operators::gate::drive_random(seed);
});

#[test]
fn a_gate_latches_and_does_not_release_when_its_condition_stops_holding() {
	// The property that distinguishes a gate from a filter, asserted directly. A row admitted at 60
	// must stay in the view when it drops to 10, and its fall must be published as an update rather
	// than as a retraction. A filter would withdraw it here, and a filter-shaped oracle would agree.
	let mut harness = Harness::new(|runtime| operators::gate::build(50, runtime));
	let workload = GateWorkload {
		value_ceiling: 100,
	};
	let row = |number: u64, value: i64| GateRow {
		number: RowNumber(number),
		value,
	};

	let refused = harness.apply(workload.insert(&[row(1, 10)])).expect("apply must succeed");
	assert!(refused.diffs.is_empty(), "a row below the threshold must not be admitted, got {:?}", refused.diffs);

	let admitted = harness.apply(workload.update(&row(1, 10), &row(1, 60))).expect("apply must succeed");
	assert!(
		matches!(admitted.diffs.as_slice(), [Diff::Insert { .. }]),
		"crossing the threshold must admit the row as an insert, not an update over nothing, got {:?}",
		admitted.diffs
	);

	let fell = harness.apply(workload.update(&row(1, 60), &row(1, 10))).expect("apply must succeed");
	let [
		Diff::Update {
			post,
			..
		},
	] = fell.diffs.as_slice()
	else {
		panic!(
			"an admitted row falling below the threshold must stay in the view as an update, got {:?}",
			fell.diffs
		);
	};
	assert_eq!(payload(post, 0), 10, "and it must carry the value it fell to");

	let withdrawn = harness.apply(workload.remove(&row(1, 10))).expect("apply must succeed");
	assert!(
		matches!(withdrawn.diffs.as_slice(), [Diff::Remove { .. }]),
		"removal is the only thing that takes an admitted row out, got {:?}",
		withdrawn.diffs
	);

	let restarted = harness.apply(workload.insert(&[row(1, 10)])).expect("apply must succeed");
	assert!(
		restarted.diffs.is_empty(),
		"re-inserting below the threshold must start the latch over rather than resume it, got {:?}",
		restarted.diffs
	);
}

#[test]
fn the_gate_sweep_reaches_both_ends_of_its_threshold() {
	// The threshold decides which paths a corpus can reach at all: at the permissive end nothing is
	// ever admitted late, and at the strict end almost everything is. A generator that drifted to the
	// middle would leave both untested while every sweep still passed.
	let mut permissive = 0;
	let mut strict = 0;
	for seed in 0..400u64 {
		let (_, params) = operators::gate::random_params(seed);
		assert!(
			params.threshold <= params.value_ceiling,
			"a threshold above the ceiling admits nothing at all and tests only the empty view: {params:?}"
		);
		if params.threshold * 4 <= params.value_ceiling {
			permissive += 1;
		}
		if params.threshold * 4 >= params.value_ceiling * 3 {
			strict += 1;
		}
	}
	assert!(permissive > 0, "the sweep must sometimes admit most rows on arrival");
	assert!(strict > 0, "the sweep must sometimes force admissions to happen later, through updates");
}

fn take_params(limit: usize, max_live: usize) -> operators::take::Params {
	operators::take::Params {
		limit,
		value_ceiling: 40,
		steps: 60,
		max_batch: 5,
		max_live,
		remove_pct: 25,
		update_pct: 30,
	}
}

chaos_test!(take_limit_8_chaos, |seed| {
	// A live set four times the limit means most arrivals evict and most departures promote.
	operators::take::drive(seed, take_params(8, 32));
});

chaos_test!(take_limit_1_chaos, |seed| {
	// The sharpest form: one slot, so every arrival evicts the incumbent and every departure has to
	// promote the newest candidate back. Eviction and promotion on essentially every step.
	operators::take::drive(seed, take_params(1, 5));
});

chaos_test!(take_under_limit_chaos, |seed| {
	// The live set never reaches the limit, so nothing is ever evicted and take must behave as a
	// pass-through. A suite that only ever overflows would never notice take dropping a row it had
	// no reason to drop.
	operators::take::drive(seed, take_params(16, 10));
});

chaos_test!(take_at_the_boundary_chaos, |seed| {
	// The live set sits exactly at the limit, so the corpus spends its time crossing the one
	// threshold where the operator decides between admitting and evicting.
	operators::take::drive(seed, take_params(6, 6));
});

chaos_test!(take_random_chaos, |seed| {
	operators::take::drive_random(seed);
});

#[test]
fn a_take_operator_keeps_the_newest_rows_and_promotes_on_retraction() {
	// The contract asserted directly, so a failure says which half broke. With a limit of two, the
	// third arrival must evict the first; retracting a survivor must bring the evicted row back
	// rather than leaving the view short.
	let mut harness = Harness::new(|_| operators::take::build(2));
	let workload = TakeWorkload {
		value_ceiling: 100,
	};
	let row = |number: u64, value: i64| TakeRow {
		number: RowNumber(number),
		value,
	};
	let mut session = Session::new(&mut harness);

	session.apply(workload.insert(&[row(1, 10), row(2, 20)])).expect("apply must succeed");
	assert_eq!(retained_identities(session.view()), vec![1, 2], "both rows fit under the limit");

	session.apply(workload.insert(&[row(3, 30)])).expect("apply must succeed");
	assert_eq!(
		retained_identities(session.view()),
		vec![2, 3],
		"a third arrival against a limit of two must evict the oldest, not refuse the newcomer"
	);

	session.apply(workload.remove(&row(3, 30))).expect("apply must succeed");
	assert_eq!(
		retained_identities(session.view()),
		vec![1, 2],
		"a freed slot must promote the newest evicted row back rather than leave the view short"
	);
}

#[test]
fn an_update_does_not_make_a_row_newer_than_rows_that_arrived_after_it() {
	// The distinction the oracle overrides `Model::update` for. Row 1 is the eviction candidate; an
	// update to it must not reorder it ahead of row 2, or the next arrival would evict the wrong row.
	// Stated here so the override is pinned by an assertion rather than only by a comment.
	let mut harness = Harness::new(|_| operators::take::build(2));
	let workload = TakeWorkload {
		value_ceiling: 100,
	};
	let row = |number: u64, value: i64| TakeRow {
		number: RowNumber(number),
		value,
	};
	let mut session = Session::new(&mut harness);

	session.apply(workload.insert(&[row(1, 10), row(2, 20)])).expect("apply must succeed");
	session.apply(workload.update(&row(1, 10), &row(1, 99))).expect("apply must succeed");
	session.apply(workload.insert(&[row(3, 30)])).expect("apply must succeed");

	assert_eq!(
		retained_identities(session.view()),
		vec![2, 3],
		"row 1 was updated, not re-admitted, so it must still be the oldest and the one evicted"
	);
}

#[test]
fn take_never_publishes_more_than_its_limit_or_a_row_that_is_not_live() {
	// The lossy regime, which the exact oracle deliberately refuses to model: past `limit * 5` live
	// rows the candidate buffer prunes, and a pruned row can never be promoted back, so the view
	// becomes a function of eviction history. Two things must still hold, and they are what a
	// consumer actually depends on: the view never exceeds the limit, and it never holds a row that
	// is not live. Falling short of the limit is permitted here - that is the documented loss.
	const LIMIT: usize = 3;
	let mut harness = Harness::new(|_| operators::take::build(LIMIT));
	let workload = TakeWorkload {
		value_ceiling: 100,
	};
	let mut session = Session::new(&mut harness);

	let mut live: Vec<TakeRow> = Vec::new();
	for number in 1..=(LIMIT * 12) as u64 {
		let row = TakeRow {
			number: RowNumber(number),
			value: number as i64,
		};
		session.apply(workload.insert(std::slice::from_ref(&row))).expect("apply must succeed");
		live.push(row);
		assert!(
			session.view().len() <= LIMIT,
			"the view holds {} rows against a limit of {LIMIT}",
			session.view().len()
		);
	}

	// Retract from the newest end, which is where the retained rows are, so every retraction frees a
	// slot and reaches for a candidate.
	while let Some(row) = live.pop() {
		session.apply(workload.remove(&row)).expect("apply must succeed");
		assert!(
			session.view().len() <= LIMIT,
			"the view holds {} rows against a limit of {LIMIT} after a retraction",
			session.view().len()
		);
		let alive: BTreeSet<i32> = live.iter().map(|r| r.identity()).collect();
		let published = retained_identities(session.view());
		for identity in &published {
			assert!(
				alive.contains(identity),
				"the view holds row {identity}, which is not live: {published:?} against {alive:?}"
			);
		}
	}

	assert!(session.view().is_empty(), "every row was retracted, so the view must be empty");
	assert!(session.incoherent().is_empty(), "the diff stream must stay foldable: {:?}", session.incoherent());
}

/// The identity column of every row in a published view, ascending. Take carries the source row
/// number through, and the workload puts it in a column, so this names exactly which rows survived.
fn retained_identities(view: &MaterializedView) -> Vec<i32> {
	let mut out: Vec<i32> = view
		.rows
		.values()
		.map(|row| match row.get("g") {
			Some(Value::Int4(v)) => *v,
			other => panic!("every published row must carry an int4 identity column, got {other:?}"),
		})
		.collect();
	out.sort_unstable();
	out
}

#[test]
fn the_take_sweep_reaches_both_sides_of_its_limit() {
	// Take behaves completely differently above and below its limit, and a generator that drifted to
	// one side would leave the other untested while every sweep still passed.
	let mut over = 0;
	let mut under = 0;
	let mut single_slot = 0;
	for seed in 0..400u64 {
		let (_, params) = operators::take::random_params(seed);
		assert!(
			params.max_live <= operators::take::exact_oracle_ceiling(params.limit),
			"the generator must not draw a corpus the exact oracle cannot model: {params:?}"
		);
		if params.max_live > params.limit {
			over += 1;
		}
		if params.max_live <= params.limit {
			under += 1;
		}
		if params.limit == 1 {
			single_slot += 1;
		}
	}
	assert!(over > 0, "the sweep must sometimes overflow the limit, which is where eviction happens");
	assert!(under > 0, "the sweep must sometimes stay under it, where take is a pass-through");
	assert!(single_slot > 0, "the sweep must reach a limit of one, where every step evicts and promotes");
}

fn distinct_params(groups: i32, regroup_pct: u32) -> operators::distinct::Params {
	operators::distinct::Params {
		groups,
		value_ceiling: 12,
		regroup_pct,
		steps: 60,
		max_batch: 5,
		max_live: 40,
		remove_pct: 25,
		update_pct: 30,
	}
}

chaos_test!(distinct_keys_4_chaos, |seed| {
	operators::distinct::drive(seed, distinct_params(4, 0));
});

chaos_test!(distinct_single_key_chaos, |seed| {
	// One key means every row collides, so every arrival either displaces the visible row or is
	// suppressed by it, and every departure either promotes a successor or empties the key. The
	// promote-on-retract path is the one that leaves a stale payload behind when it is wrong.
	operators::distinct::drive(seed, distinct_params(1, 0));
});

chaos_test!(distinct_regrouping_chaos, |seed| {
	// An update that moves a row to a different key must retract from the old entry and publish into
	// the new one in the same step, and either half can promote or empty a key on its own. That is
	// the widest path in `process_update` and it is unreachable when an update only rewrites payload.
	operators::distinct::drive(seed, distinct_params(3, 60));
});

chaos_test!(distinct_wide_keys_chaos, |seed| {
	// The opposite end: keys outnumber the live rows, so most keys hold exactly one row and the
	// suite is about minting and retiring keys rather than about contention inside one.
	operators::distinct::drive(
		seed,
		operators::distinct::Params {
			groups: 32,
			max_live: 20,
			..distinct_params(32, 30)
		},
	);
});

chaos_test!(distinct_random_chaos, |seed| {
	operators::distinct::drive_random(seed);
});

#[test]
fn a_distinct_operator_publishes_one_row_per_key_and_promotes_on_retraction() {
	// The two halves of the contract, asserted directly rather than through a sweep, so a failure
	// says which half broke. Row 2 outranks row 1 on the same key, so it is the one published; when
	// it leaves, row 1 must be promoted rather than the key going dark or keeping row 2's payload.
	let mut harness = Harness::new(operators::distinct::build);
	let workload = DistinctWorkload {
		groups: 1,
		value_ceiling: 100,
		regroup_pct: 0,
	};
	let row = |number: u64, value: i64| DistinctRow {
		number: RowNumber(number),
		group: 1,
		value,
	};

	let out = harness.apply(workload.insert(&[row(1, 10), row(2, 20)])).expect("apply must succeed");
	let published: Vec<i64> = out
		.diffs
		.iter()
		.filter_map(|diff| diff.post())
		.flat_map(|post| (0..post.row_count()).map(|i| payload(post, i)).collect::<Vec<_>>())
		.collect();
	assert_eq!(
		published.last(),
		Some(&20),
		"the highest-numbered row of a key is the one published, got {published:?}"
	);

	let promoted = harness.apply(workload.remove(&row(2, 20))).expect("apply must succeed");
	let [
		Diff::Update {
			pre,
			post,
			..
		},
	] = promoted.diffs.as_slice()
	else {
		panic!(
			"retracting the visible row of a key that still holds another must promote it, got {:?}",
			promoted.diffs
		);
	};
	assert_eq!(payload(pre, 0), 20, "the retraction must carry the payload that was published");
	assert_eq!(payload(post, 0), 10, "and the promotion must carry the surviving row's payload");

	let emptied = harness.apply(workload.remove(&row(1, 10))).expect("apply must succeed");
	assert!(
		matches!(emptied.diffs.as_slice(), [Diff::Remove { .. }]),
		"retracting the last row of a key must withdraw it, got {:?}",
		emptied.diffs
	);
}

/// The payload column of one row of a `Columns`, looked up by name so a change in column order cannot
/// make an assertion read a different column and still pass.
fn payload(columns: &Columns, idx: usize) -> i64 {
	let names: Vec<String> = columns.names.iter().map(|name| name.text().to_string()).collect();
	let at = names
		.iter()
		.position(|name| name == "v")
		.unwrap_or_else(|| panic!("the published row must carry the payload column, got {names:?}"));
	match columns.columns[at].get_value(idx) {
		Value::Int8(v) => v,
		other => panic!("the payload must be an int8, got {other:?}"),
	}
}

#[test]
fn the_distinct_sweep_reaches_both_contention_and_regrouping() {
	// A generator can narrow silently while every test still passes. Distinct only does work when
	// rows collide on a key, so a sweep that drifted towards many keys would stop testing the
	// operator and nothing would say so.
	let mut colliding = 0;
	let mut regrouping = 0;
	let mut wide = 0;
	for seed in 0..400u64 {
		let (_, params) = operators::distinct::random_params(seed);
		if params.groups <= 2 {
			colliding += 1;
		}
		if params.regroup_pct >= 30 {
			regrouping += 1;
		}
		if params.groups >= 6 {
			wide += 1;
		}
	}
	assert!(colliding > 0, "the sweep must sometimes draw few enough keys to force contention");
	assert!(regrouping > 0, "the sweep must sometimes move rows between keys often");
	assert!(wide > 0, "the sweep must also reach the sparse end, where most keys hold one row");
}

#[test]
fn the_aggregate_sweep_reaches_every_monoid_and_the_tie_that_defeats_inversion() {
	// Same failure mode the other sweeps guard against: a generator can narrow silently while every
	// test still passes, leaving a monoid or the recompute path untested and nothing to say so.
	let mut seen: BTreeSet<&'static str> = BTreeSet::new();
	let mut tied = 0;
	let mut removing = 0;
	for seed in 0..400u64 {
		let (_, params) = operators::aggregate::random_params(seed);
		seen.insert(params.agg.label());
		if params.value_ceiling <= 2 {
			tied += 1;
		}
		if params.remove_pct >= 20 {
			removing += 1;
		}
	}
	assert_eq!(seen.len(), operators::aggregate::MATRIX.len(), "the sweep must reach every monoid, saw {seen:?}");
	assert!(tied > 0, "the sweep must sometimes draw a value space narrow enough to force ties");
	assert!(removing > 0, "the sweep must sometimes remove often enough to exercise retraction");
}

#[test]
fn the_join_and_append_sweeps_reach_the_shapes_their_operators_are_built_around() {
	// Same failure mode the window sweeps guard against: a generator can narrow silently while every
	// sweep still passes. A join that never sees both sides of a key cannot join, and for append two
	// inputs colliding on one source row number is the whole reason the input index is in the group key.
	const SEEDS: u64 = 512;

	let mut variants = std::collections::BTreeSet::new();
	let mut single_key = 0;
	let mut with_undefined = 0;
	let mut without_undefined = 0;
	for seed in 0..SEEDS {
		let (_, params) = operators::join::random_params(seed);
		variants.insert(format!("{:?}", params.variant));
		if params.keys == 1 {
			single_key += 1;
		}
		if params.none_pct > 0 {
			with_undefined += 1;
		} else {
			without_undefined += 1;
		}
		assert!(
			params.right_pct > 0 && params.right_pct < 100,
			"a sweep that starves one side can never join: {params:?}"
		);
		assert!(
			params.steps > 0 && params.max_batch > 0 && params.keys > 0 && params.max_live > 0,
			"degenerate draw: {params:?}"
		);
	}
	assert_eq!(variants.len(), 4, "the sweep stopped covering every strategy, only {variants:?}");
	assert!(single_key > 0, "no single-key draw in {SEEDS} seeds; the widest cartesian product is uncovered");
	assert!(with_undefined > 0, "no undefined-key draw in {SEEDS} seeds; the undefined handlers are uncovered");
	assert!(without_undefined > 0, "every draw carries undefined keys; the all-defined path is no longer isolated");

	let mut collides = 0;
	let mut input_counts = std::collections::BTreeSet::new();
	for seed in 0..SEEDS {
		let (_, params) = operators::append::random_params(seed);
		input_counts.insert(params.inputs);
		// A collision across inputs is near-certain once the row space is smaller than the rows
		// drawn into it.
		if params.row_space <= params.max_live as u64 {
			collides += 1;
		}
		assert!(params.inputs >= 2, "append requires at least two inputs: {params:?}");
		assert!(params.row_space > 0 && params.steps > 0, "degenerate draw: {params:?}");
	}
	assert!(input_counts.len() >= 3, "append input arity collapsed to {input_counts:?}");
	assert!(
		collides > SEEDS as usize / 2,
		"only {collides} of {SEEDS} draws crowd the row-number space; the same source row arriving on \
		 two inputs is what the group key exists to separate"
	);
}

chaos_test!(join_inner_none_pct_30_chaos, |seed| {
	// The control for join_matrix_definedness_flip_chaos: identical parameters with only the crossing
	// between defined and undefined withheld, so green here while the flip sweep is red pins a
	// failure to that crossing rather than to the undefined-key mix both carry.
	operators::join::drive(
		seed,
		operators::join::Params {
			variant: Variant::inner(),
			keys: 3,
			right_pct: 50,
			none_pct: 30,
			rekey_pct: 60,
			steps: 60,
			max_batch: 4,
			max_live: 24,
			remove_pct: 20,
			update_pct: 40,
			coord_span_ms: 400_000,
			left_ttl: None,
			right_ttl: None,
			static_right: 0,
		},
	);
});

chaos_test!(join_matrix_definedness_flip_chaos, |seed| {
	// Every strategy, one corpus, so the report names which of them the crossing actually breaks
	// rather than stopping at the first.
	let diverged: Vec<String> =
		[Variant::inner(), Variant::left(), Variant::inner().with_latest(), Variant::left().with_latest()]
			.into_iter()
			.filter_map(|variant| {
				let params = operators::join::Params {
					variant,
					keys: 3,
					right_pct: 50,
					none_pct: 30,
					rekey_pct: 60,
					steps: 60,
					max_batch: 4,
					max_live: 24,
					remove_pct: 20,
					update_pct: 40,
					coord_span_ms: 400_000,
					left_ttl: None,
					right_ttl: None,
					static_right: 0,
				};
				operators::join::divergence_with_definedness_flips(seed, params)
					.map(|report| format!("{variant:?}: {report}"))
			})
			.collect();

	assert!(
		diverged.is_empty(),
		"an update that moves a key between defined and undefined must leave the operator describing \
		 the same table as an operator that was handed the equivalent remove and insert, but:\n\n{}",
		diverged.join("\n\n")
	);
});

#[test]
fn the_join_oracles_are_not_interchangeable() {
	// An oracle that described no rows, a claim never compared, a corpus that never reached the
	// operator - all three look exactly like a passing suite. Requiring every cross pair to diverge
	// only holds if the four claims describe four different tables and are genuinely checked.
	let variants =
		[Variant::inner(), Variant::left(), Variant::inner().with_latest(), Variant::left().with_latest()];

	for operator in variants {
		for oracle in variants {
			let params = operators::join::Params {
				variant: operator,
				keys: 2,
				right_pct: 50,
				none_pct: 10,
				rekey_pct: 25,
				steps: 40,
				max_batch: 4,
				max_live: 20,
				remove_pct: 20,
				update_pct: 30,
				coord_span_ms: 400_000,
				left_ttl: None,
				right_ttl: None,
				static_right: 0,
			};
			let divergence = operators::join::divergence_checked_as(7, params, oracle);
			match operator == oracle {
				true => assert!(
					divergence.is_none(),
					"{operator:?} must satisfy its own oracle, but reported: {}",
					divergence.unwrap_or_default()
				),
				false => assert!(
					divergence.is_some(),
					"{operator:?} was accepted by the {oracle:?} oracle; the two claims do not \
					 describe different tables, so neither sweep proves anything"
				),
			}
		}
	}
}

#[test]
fn the_four_strategy_sweeps_drive_one_shared_corpus() {
	// The four fixed strategy sweeps are only comparable while they execute the identical operation
	// sequence, and nothing about the corpus is visible in a green run. The fingerprint mixes every
	// value the driver drew, so any parameter drifting apart moves it.
	const SEED: u64 = 20_260_730;

	let fingerprints: Vec<(Variant, u64)> =
		[Variant::inner(), Variant::left(), Variant::inner().with_latest(), Variant::left().with_latest()]
			.into_iter()
			.map(|variant| {
				(
					variant,
					operators::join::drive(SEED, operators::join::matched_params(variant))
						.fingerprint(),
				)
			})
			.collect();

	let (_, first) = fingerprints[0];
	assert!(
		fingerprints.iter().all(|(_, fingerprint)| *fingerprint == first),
		"the strategy sweeps have stopped sharing a corpus and can no longer be compared: {fingerprints:x?}"
	);
	assert_ne!(first, 0, "a corpus that drew nothing would trivially agree with itself");
}

chaos_test!(join_matrix_static_right_12_chaos, |seed| {
	// The eight-cell matrix: {inner, left} x {hash, latest} x {snapshot off, on}, every cell driven
	// against a frozen right side - the only shape that makes the snapshot cells answerable, and it
	// costs the other cells nothing, so all eight stay comparable.
	let diverged: Vec<String> = operators::join::MATRIX
		.into_iter()
		.filter_map(|variant| {
			operators::join::drive_static_right(seed, operators::join::static_right_params(variant))
				.divergence
				.map(|report| format!("{}: {report}", variant.label()))
		})
		.collect();

	assert!(diverged.is_empty(), "{}", diverged.join("\n\n"));
});

#[test]
fn snapshot_changes_when_work_happens_not_what_the_answer_is() {
	// `snapshot` is a statement about which changes are worth republishing, not about what the join
	// contains, so over a static right side it must leave the published table identical. Any
	// divergence means the flag is losing rows rather than losing work.
	const SEED: u64 = 20_260_731;

	for base in [
		operators::join::Variant::inner(),
		operators::join::Variant::left(),
		operators::join::Variant::inner().with_latest(),
		operators::join::Variant::left().with_latest(),
	] {
		let plain = operators::join::drive_static_right(SEED, operators::join::static_right_params(base));
		let snapshot = operators::join::drive_static_right(
			SEED,
			operators::join::static_right_params(base.with_snapshot()),
		);

		assert_eq!(plain.divergence, None, "{} diverged from its own oracle", base.label());
		assert_eq!(snapshot.divergence, None, "{} diverged from its own oracle", base.with_snapshot().label());
		assert_eq!(
			plain.view.rekey(&RowKey::columns(["lid", "other_rid"])),
			snapshot.view.rekey(&RowKey::columns(["lid", "other_rid"])),
			"{} answers differently with snapshot on; over a static right side the flag must only \
			 change how much is republished, never what the join holds",
			base.label()
		);
		assert!(!plain.view.is_empty(), "an empty view would let any two cells agree for free");
	}
}

#[test]
fn a_snapshot_join_stays_coherent_when_its_right_side_keeps_changing() {
	// The oracle cannot judge contents under a changing right side (suppressed emissions leave the
	// view deliberately behind), but coherence can be: a remove of a row never published, or an
	// update to an absent one, is wrong under any reading of what snapshot promises.
	const SEED: u64 = 20_260_732;

	for base in [
		operators::join::Variant::inner(),
		operators::join::Variant::left(),
		operators::join::Variant::inner().with_latest(),
		operators::join::Variant::left().with_latest(),
	] {
		let variant = base.with_snapshot();
		let outcome = operators::join::drive_interleaved(SEED, operators::join::matched_params(variant));
		assert!(
			outcome.view.incoherent.is_empty(),
			"{}: a changing right side made the join publish a diff stream a sink cannot fold: {:?}",
			variant.label(),
			outcome.view.incoherent
		);
	}
}

chaos_test!(join_matrix_snapshot_right_pct_50_chaos, |seed| {
	// The four snapshot cells with both sides interleaved, which the static-right matrix cannot reach:
	// the published table is a record of what each left row saw rather than a function of the live
	// sets, and only a changing right side puts that under strain.
	let diverged: Vec<String> =
		[Variant::inner(), Variant::left(), Variant::inner().with_latest(), Variant::left().with_latest()]
			.into_iter()
			.map(|base| base.with_snapshot())
			.filter_map(|variant| {
				operators::join::drive_interleaved(seed, operators::join::matched_params(variant))
					.divergence
					.map(|report| format!("{}: {report}", variant.label()))
			})
			.collect();

	assert!(diverged.is_empty(), "{}", diverged.join("\n\n"));
});

chaos_test!(sink_table_unpartitioned_chaos, |seed| {
	operators::sink::drive(seed, operators::sink::params(operators::sink::Kind::Table, Layout::Unpartitioned));
});

chaos_test!(sink_table_partitioned_chaos, |seed| {
	operators::sink::drive(seed, operators::sink::params(operators::sink::Kind::Table, Layout::Partitioned));
});

chaos_test!(sink_series_unpartitioned_chaos, |seed| {
	operators::sink::drive(seed, operators::sink::params(operators::sink::Kind::Series, Layout::Unpartitioned));
});

chaos_test!(sink_series_partitioned_chaos, |seed| {
	operators::sink::drive(seed, operators::sink::params(operators::sink::Kind::Series, Layout::Partitioned));
});

chaos_test!(sink_ring_roomy_unpartitioned_chaos, |seed| {
	// Nothing is ever evicted, so a divergence here is not an eviction bug.
	operators::sink::drive(
		seed,
		operators::sink::params(
			operators::sink::Kind::Ring {
				capacity: 64,
			},
			Layout::Unpartitioned,
		),
	);
});

chaos_test!(sink_ring_roomy_partitioned_chaos, |seed| {
	operators::sink::drive(
		seed,
		operators::sink::params(
			operators::sink::Kind::Ring {
				capacity: 64,
			},
			Layout::Partitioned,
		),
	);
});

chaos_test!(sink_ring_evicting_unpartitioned_chaos, |seed| {
	// Overrun continuously, so both eviction paths run rather than only the common one.
	operators::sink::drive(
		seed,
		operators::sink::params(
			operators::sink::Kind::Ring {
				capacity: 4,
			},
			Layout::Unpartitioned,
		),
	);
});

chaos_test!(sink_ring_evicting_partitioned_chaos, |seed| {
	// The same pressure per lane, which is the distinction the unpartitioned cell cannot make.
	operators::sink::drive(
		seed,
		operators::sink::params(
			operators::sink::Kind::Ring {
				capacity: 4,
			},
			Layout::Partitioned,
		),
	);
});

chaos_test!(sink_ring_capacity_1_chaos, |seed| {
	// The degenerate lane: every insert evicts its predecessor.
	operators::sink::drive(
		seed,
		operators::sink::params(
			operators::sink::Kind::Ring {
				capacity: 1,
			},
			Layout::Partitioned,
		),
	);
});

chaos_test!(sink_random_chaos, |seed| {
	operators::sink::drive_random(seed);
});

chaos_test!(source_series_chaos, |seed| {
	operators::source::drive(seed, operators::source::params(operators::source::Kind::Series));
});

chaos_test!(source_table_chaos, |seed| {
	operators::source::drive(seed, operators::source::params(operators::source::Kind::Table));
});

chaos_test!(source_view_chaos, |seed| {
	operators::source::drive(seed, operators::source::params(operators::source::Kind::View));
});

chaos_test!(source_ringbuffer_chaos, |seed| {
	operators::source::drive(seed, operators::source::params(operators::source::Kind::RingBuffer));
});

chaos_test!(source_random_chaos, |seed| {
	operators::source::drive_random(seed);
});

chaos_test!(window_session_rotating_chaos, |seed| {
	// Coordinates drawn far wider than the gap, so nearly every arrival opens a new session and the
	// rotation path carries the sweep.
	operators::window::session::drive(seed, operators::window::session::params(200, 20_000, Fold::Sum));
});

chaos_test!(window_session_extending_chaos, |seed| {
	// The mirror: a gap wider than the whole coordinate span keeps one session open per group, so
	// what is exercised is extension at both ends rather than rotation.
	operators::window::session::drive(seed, operators::window::session::params(20_000, 5_000, Fold::Sum));
});

chaos_test!(window_session_boundary_chaos, |seed| {
	// Gap and span the same order, which is the only band where rotation, backwards extension and
	// refusal all occur in one corpus.
	operators::window::session::drive(seed, operators::window::session::params(2_000, 6_000, Fold::Sum));
});

chaos_test!(window_session_zero_gap_chaos, |seed| {
	// A zero gap makes every distinct instant its own session while repeats still merge, so a
	// tracker that confused "no quiet period" with "same coordinate" collapses the corpus.
	operators::window::session::drive(seed, operators::window::session::params(0, 400, Fold::Sum));
});

chaos_test!(window_session_min_chaos, |seed| {
	// Min is not merely different arithmetic: it is non-invertible once the lateness is non-zero and takes
	// the full-recompute path, so a retraction inside a session is recomputed rather than subtracted.
	operators::window::session::drive(seed, operators::window::session::params(2_000, 6_000, Fold::Min));
});

chaos_test!(window_session_max_chaos, |seed| {
	operators::window::session::drive(seed, operators::window::session::params(2_000, 6_000, Fold::Max));
});

chaos_test!(window_session_random_chaos, |seed| {
	operators::window::session::drive_random(seed);
});
