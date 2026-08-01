// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "chaos")]

#[path = "chaos/framework/mod.rs"]
mod framework;
#[path = "chaos/operators/mod.rs"]
mod operators;

use reifydb_core::common::{WindowKind, WindowSize};
use reifydb_sub_flow::execution::reclaim::ReclaimBudget;
use reifydb_testing_chaos::{
	fuzz::run_reported,
	operator::{subject::Subject, view::RowKey, workload::Workload},
};
use reifydb_testing_macro::chaos_test;
use reifydb_value::value::{datetime::DateTime, duration::Duration, row_number::RowNumber};

use crate::{
	framework::{generator, harness::Harness},
	operators::{
		append::workload::{AppendRow, AppendWorkload},
		join::{
			Variant,
			workload::{JoinRow, JoinWorkload, Side},
		},
		window::{WindowSpec, build},
	},
};

// Not free to pick: an operator's own seal span overrides whatever the harness declares.
const WINDOW_SECS: i64 = 60;

fn tumbling_sum() -> WindowSpec {
	WindowSpec {
		kind: WindowKind::Tumbling {
			size: WindowSize::Duration(Duration::from_seconds(WINDOW_SECS).unwrap()),
		},
		group_by: "g",
		aggregations: "total: math::sum(v)",
		grace: Duration::default(),
	}
}

const SPAN_MS: u64 = WINDOW_SECS as u64 * 1_000;

// Sixteen buckets per horizon, so a 60s span grids at 3.75s.
const GRID_WIDTH_MS: u64 = SPAN_MS / 16;

// A seal at T only proves windows anchored at or before `T - span - 1` are closed, so a group in
// bucket zero comes due only once the anchor clears a full grid width.
const SEAL_MS: u64 = SPAN_MS + GRID_WIDTH_MS + 1;

// One millisecond short: a group is due only once its bucket falls strictly below the cutoff's.
const EARLY_SEAL_MS: u64 = SEAL_MS - 1;

// Bounds the identity phase only - the data phase is bounded by the seal ledger - so it must sit
// past the horizon without being what makes a group due.
const SWEEP_MS: u64 = SPAN_MS + GRID_WIDTH_MS;

#[test]
fn a_window_operator_can_be_built_and_driven() {
	let spec = tumbling_sum();
	let mut harness = Harness::new(|runtime| build(&spec, runtime));

	let at = DateTime::from_timestamp_millis(60_000).unwrap();
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
			grace_secs: 0,
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

chaos_test!(window_tumbling_grace_chaos, |seed| {
	operators::window::tumbling::drive(
		seed,
		operators::window::tumbling::Params {
			size_secs: 30,
			grace_secs: 45,
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

chaos_test!(window_tumbling_reclaim_chaos, |seed| {
	// Grace wider than the size keeps a window open past its own span, which is where a sweep and a
	// live window actually meet. Vacuity is not asserted per seed: the seal is the primary reclaimer,
	// so roughly one corpus in twenty correctly leaves the sweep nothing to do.
	operators::window::tumbling::drive_reclaiming(
		seed,
		operators::window::tumbling::Params {
			size_secs: 30,
			grace_secs: 45,
			groups: 3,
			steps: 60,
			max_batch: 4,
			coord_span_ms: 400_000,
			remove_pct: 25,
			update_pct: 15,
			seal_pct: 30,
		},
		20,
		true,
	);
});

chaos_test!(window_tumbling_reclaim_random_chaos, |seed| {
	operators::window::tumbling::drive_reclaiming_random(seed);
});

chaos_test!(window_sliding_reclaim_chaos, |seed| {
	// Only the seed varies, so this searches the neighbourhood of a shape that once diverged rather
	// than one point in it.
	operators::window::sliding::drive_reclaiming(
		seed,
		operators::window::sliding::Params {
			size_secs: 30,
			slide_secs: 10,
			grace_secs: 45,
			groups: 3,
			steps: 60,
			max_batch: 4,
			coord_span_ms: 400_000,
			remove_pct: 25,
			update_pct: 15,
			seal_pct: 30,
		},
		20,
		true,
	);
});

chaos_test!(window_sliding_reclaim_random_chaos, |seed| {
	operators::window::sliding::drive_reclaiming_random(seed);
});

chaos_test!(window_sliding_sum_chaos, |seed| {
	operators::window::sliding::drive(
		seed,
		operators::window::sliding::Params {
			size_secs: 60,
			slide_secs: 15,
			grace_secs: 0,
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
			grace_secs: 0,
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

chaos_test!(window_rolling_grace_chaos, |seed| {
	operators::window::rolling::drive(
		seed,
		operators::window::rolling::Params {
			size_secs: 30,
			grace_secs: 45,
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

chaos_test!(window_sliding_grace_chaos, |seed| {
	operators::window::sliding::drive(
		seed,
		operators::window::sliding::Params {
			size_secs: 30,
			slide_secs: 10,
			grace_secs: 45,
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
			grace_secs: 15,
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

	let mut zero_grace = 0;
	let mut grace_over_size = 0;
	let mut tumbling_sizes = std::collections::BTreeSet::new();
	for seed in 0..SEEDS {
		let (_, params) = operators::window::tumbling::random_params(seed);
		tumbling_sizes.insert(params.size_secs);
		if params.grace_secs == 0 {
			zero_grace += 1;
		}
		if params.grace_secs > params.size_secs {
			grace_over_size += 1;
		}
		assert!(
			params.seal_pct + params.remove_pct + params.update_pct <= 85,
			"inserts must keep at least a 15% share or the corpus never grows: {params:?}"
		);
		assert!(params.steps > 0 && params.max_batch > 0 && params.groups > 0, "degenerate draw: {params:?}");
	}
	assert!(zero_grace > 0, "no zero-grace draw in {SEEDS} seeds; the closes-immediately boundary is uncovered");
	assert!(
		grace_over_size > 0,
		"no grace-wider-than-size draw in {SEEDS} seeds; that band is where the rolling operator was \
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
fn a_harness_without_a_registered_grid_can_never_reclaim() {
	// The reclaim driver skips a node with no event grid silently, so a suite built on an ungridded
	// harness would report green while asserting nothing about reclamation.
	let plain = Harness::new(|_| operators::append::build(2));
	assert!(
		plain.activity_grid().event_grid().is_none(),
		"the default is the undeclared grid, which is exactly what the driver refuses to sweep"
	);

	// The grid comes from the operator, so a fixture declaring no ttl is perpetual and stays
	// ungridded.
	let span = Duration::from_seconds(16).expect("16s is representable");
	let declared = Harness::new(|_| operators::append::build_with_ttl(2, Some(span))).with_activity_grid();
	let grid = declared.activity_grid().event_grid().expect("a declared scale must grid in event time");
	assert_eq!(
		grid.width(),
		Duration::from_seconds(1).unwrap(),
		"sixteen buckets per scale, so a 16s ttl grids at one second here"
	);
}

#[test]
fn the_harness_sweep_retires_a_group_only_once_its_seal_ledger_clears_its_horizon() {
	// The frontier is the operator's seal ledger, not the instant the sweep is called at: nothing
	// unsealed is reclaimable. A sweep that retired nothing, or everything immediately, would be
	// indistinguishable from working code in every suite built on it.
	let mut harness =
		Harness::new(|runtime| operators::window::build(&tumbling_sum(), runtime)).with_activity_grid();

	let at = |ms: u64| DateTime::from_timestamp_millis(ms).unwrap();
	harness.apply(generator::insert(vec![generator::row(RowNumber(1), 1, 10, at(0))])).expect("apply must succeed");

	let unsealed = harness.reclaim(SWEEP_MS).expect("sweep must succeed");
	assert!(
		unsealed.is_empty(),
		"an operator that has sealed nothing has nothing reclaimable, but the sweep took {unsealed:?}"
	);

	harness.tick(EARLY_SEAL_MS).expect("seal must succeed");
	let early = harness.reclaim(SWEEP_MS).expect("sweep must succeed");
	assert!(
		early.is_empty(),
		"a group one millisecond inside its horizon must survive, but the sweep took {early:?}"
	);

	harness.tick(SEAL_MS).expect("seal must succeed");
	let due = harness.reclaim(SWEEP_MS).expect("sweep must succeed");
	assert!(!due.data.is_empty(), "once the seal clears bucket 0 the group must be retired");
}

#[test]
fn a_truncated_budget_leaves_the_rest_of_the_due_groups_for_the_next_sweep() {
	// The production budget is 256 groups per tick, which no chaos run approaches, so partial
	// reclamation has to be a scenario knob rather than a hope.
	let mut harness = Harness::new(|runtime| operators::window::build(&tumbling_sum(), runtime))
		.with_activity_grid()
		.with_reclaim_budget(ReclaimBudget {
			groups: 1,
			rows: 1_024,
		});

	let at = |ms: u64| DateTime::from_timestamp_millis(ms).unwrap();
	harness.apply(generator::insert(vec![
		generator::row(RowNumber(1), 1, 10, at(0)),
		generator::row(RowNumber(2), 2, 20, at(0)),
	]))
	.expect("apply must succeed");

	// Both groups are due only once the operator has sealed past their bucket, so without this tick
	// the budget assertions below pass vacuously at zero.
	harness.tick(SEAL_MS).expect("seal must succeed");

	let first = harness.reclaim(SWEEP_MS).expect("sweep must succeed");
	assert_eq!(first.data.len(), 1, "a one-group budget must stop after one group");

	let second = harness.reclaim(SWEEP_MS).expect("sweep must succeed");
	assert_eq!(second.data.len(), 1, "the group left behind is still due and goes on the next sweep");

	let third = harness.reclaim(SWEEP_MS).expect("sweep must succeed");
	assert!(third.is_empty(), "and a drained node must not keep offering the same groups back");
}

#[test]
fn a_harness_without_a_declared_horizon_sweeps_nothing() {
	// The default has to be inert, or every existing suite would start reclaiming underneath itself.
	// No declared horizon means no grid, which is the exact condition production skips a node on.
	let mut harness = Harness::new(|runtime| operators::window::build(&tumbling_sum(), runtime));
	let at = |ms: u64| DateTime::from_timestamp_millis(ms).unwrap();
	harness.apply(generator::insert(vec![generator::row(RowNumber(1), 1, 10, at(0))])).expect("apply must succeed");

	assert!(harness.reclaim(1_000_000).expect("sweep must succeed").is_empty());
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
			tick_pct: 0,
			sink_row_ttl: None,
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
			tick_pct: 0,
			sink_row_ttl: None,
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
					tick_pct: 0,
					sink_row_ttl: None,
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
				tick_pct: 0,
				sink_row_ttl: None,
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
