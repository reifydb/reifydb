// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "chaos")]

#[path = "chaos/framework/mod.rs"]
mod framework;
#[path = "chaos/operators/mod.rs"]
mod operators;

use reifydb_core::{
	common::{WindowKind, WindowSize},
	state::horizon::Horizon,
};
use reifydb_sub_flow::execution::reclaim::ReclaimBudget;
use reifydb_testing_chaos::{fuzz::run_reported, operator::workload::Workload};
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

fn tumbling_sum() -> WindowSpec {
	WindowSpec {
		kind: WindowKind::Tumbling {
			size: WindowSize::Duration(Duration::from_seconds(60).unwrap()),
		},
		group_by: "g",
		aggregations: "total: math::sum(v)",
		grace: Duration::default(),
		lateness: Duration::default(),
	}
}

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

// The sweeps above pin specific configurations and never move, so they stay comparable across
// commits. The three below draw their configuration from the seed as well, which is what actually
// found the grace-wider-than-interval band the rolling operator was mishandling. A failure here
// reports the RESOLVED parameters, and those are what a regression pins - never the master seed,
// which stops meaning the same thing the moment framework::fuzz changes.
// The mutation primitives that were only ever applied to guest operators, now applied to a host window.
// A duplicate update must net to no change in the aggregate, an update split into remove-then-insert
// must land the same total as the update would have, and a small live-row cap keeps both landing on
// rows the window has already published.
chaos_test!(window_tumbling_flow_shaped_chaos, |seed| {
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
	// Intent: a parameter generator can degenerate silently. Narrow a range, drop a ratio, and
	// every sweep still passes - it is just no longer testing the region that mattered. This
	// pins the regions by name so that shrinking one fails here rather than going quiet.
	//
	// grace > size is the band where a rolling coordinate is new enough to admit but already too
	// old to contribute, which is where the operator was withdrawing live groups. grace == 0 is
	// the opposite boundary, where a window closes the instant it ends. A slide that does not
	// divide its size gives non-uniform window coverage, which a slide of exactly size/2 never
	// exercises.
	// Mutation: collapse GRACE_RATIOS to [(0, 1)] and the grace assertions fail; clamp the slide
	// draw to size / 2 and the non-dividing assertion fails.
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

	// The count sweeps bucket on a per-group ordinal rather than a coordinate, so the regions
	// worth pinning are different ones. A size of 1 puts every row in its own window, which is
	// where an off-by-one in the ordinal division shows up first; a rolling capacity of 1 makes
	// almost every retraction target a row the buffer has already pushed out, which is the case a
	// capacity model is most likely to get wrong.
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
	// Intent: run_reported wraps every fuzzed sweep in catch_unwind so it can print the resolved
	// parameters before the panic escapes. If it ever swallowed the panic instead of re-raising
	// it, all three random sweeps would report green forever and nothing would say otherwise -
	// the worst failure mode this suite can have, because it is silent.
	// The report itself goes to stderr; run with --no-capture to eyeball that it stays
	// paste-ready.
	// Mutation: drop the resume_unwind in fuzz::run_reported and this fails.
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
	// Intent: the two inputs reach the operator as two inputs. A join reads the side off each diff's
	// origin, and a diff it cannot place is an error rather than a default - so if the corpus stopped
	// tagging origins, or tagged both sides the same, every sweep below would still run and would
	// simply never join anything. That failure is silent, and this is what makes it loud.
	let workload = JoinWorkload {
		keys: 1,
		right_pct: 0,
		none_pct: 0,
		rekey_pct: 0,
		flip_definedness: false,
	};
	let mut harness = Harness::with_engine(|engine, _| operators::join::build(engine, Variant::inner()));

	let left = JoinRow {
		side: Side::Left,
		number: RowNumber(1),
		key: Some(1),
		value: 10,
	};
	let right = JoinRow {
		side: Side::Right,
		number: RowNumber(2),
		key: Some(1),
		value: 20,
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
	// Intent: the reclaim driver skips a node whose activity buckets carry no event grid, and that
	// skip is silent - it increments no counter and logs nothing. A harness that never registers a
	// grid therefore does not sweep imprecisely, it does not sweep at all, and a suite built on one
	// would report green while asserting nothing about reclamation.
	// Mutation: drop the set_activity_grid call from with_activity_grid and the second half fails.
	let plain = Harness::new(|_| operators::append::build(2));
	assert!(
		plain.activity_grid().event_grid().is_none(),
		"the default is the undeclared grid, which is exactly what the driver refuses to sweep"
	);

	let span = Duration::from_seconds(16).expect("16s is representable");
	let declared = Harness::new(|_| operators::append::build(2)).with_activity_grid(Horizon::of(span));
	let grid = declared.activity_grid().event_grid().expect("a declared horizon must grid in event time");
	assert_eq!(
		grid.width(),
		Duration::from_seconds(1).unwrap(),
		"sixteen buckets per horizon, so the slack a sweep subtracts is one second here"
	);
}

#[test]
fn the_harness_sweep_retires_a_group_only_once_the_watermark_clears_its_horizon() {
	// Intent: the harness drives production's own `reclaim_nodes`, and it drives it correctly - the
	// cutoff it derives has to put a group on the right side of the horizon. Both halves matter. A
	// sweep that retired nothing whatever the watermark would be indistinguishable from working code
	// in every suite built on it, and a sweep that retired everything immediately would make every
	// later assertion about what survives vacuous.
	//
	// A 16s horizon grids at 1s (sixteen buckets per horizon), so a group stamped at t=0 sits in
	// bucket 0 and the data cutoff `watermark - 16s` only passes it once the watermark reaches 17s.
	let span = Duration::from_seconds(16).expect("16s is representable");
	let mut harness = Harness::new(|runtime| operators::window::build(&tumbling_sum(), runtime))
		.with_activity_grid(Horizon::of(span));

	let at = |ms: u64| DateTime::from_timestamp_millis(ms).unwrap();
	harness.apply(generator::insert(vec![generator::row(RowNumber(1), 1, 10, at(0))])).expect("apply must succeed");

	let early = harness.reclaim(16_999).expect("sweep must succeed");
	assert!(
		early.is_empty(),
		"a group one millisecond inside its horizon must survive, but the sweep took {early:?}"
	);

	let due = harness.reclaim(17_000).expect("sweep must succeed");
	assert!(!due.data.is_empty(), "once the cutoff clears bucket 0 the group must be retired");
}

#[test]
fn a_truncated_budget_leaves_the_rest_of_the_due_groups_for_the_next_sweep() {
	// The production budget is 256 groups per tick, which no chaos run approaches, so partial
	// reclamation would never occur by accident - and partial reclamation is where the invariants
	// with the most history live. Driving it has to be a scenario knob rather than a hope.
	let span = Duration::from_seconds(16).expect("16s is representable");
	let mut harness = Harness::new(|runtime| operators::window::build(&tumbling_sum(), runtime))
		.with_activity_grid(Horizon::of(span))
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

	let first = harness.reclaim(17_000).expect("sweep must succeed");
	assert_eq!(first.data.len(), 1, "a one-group budget must stop after one group");

	let second = harness.reclaim(17_000).expect("sweep must succeed");
	assert_eq!(second.data.len(), 1, "the group left behind is still due and goes on the next sweep");

	let third = harness.reclaim(17_000).expect("sweep must succeed");
	assert!(third.is_empty(), "and a drained node must not keep offering the same groups back");
}

#[test]
fn a_harness_without_a_declared_horizon_sweeps_nothing() {
	// The default has to be inert, or every existing suite would start reclaiming underneath itself
	// the moment the sweep was wired in. No declared horizon means no grid, and no grid is the exact
	// condition the production driver skips a node on - so reporting an empty sweep here is the same
	// answer production gives, not a harness shortcut.
	let mut harness = Harness::new(|runtime| operators::window::build(&tumbling_sum(), runtime));
	let at = |ms: u64| DateTime::from_timestamp_millis(ms).unwrap();
	harness.apply(generator::insert(vec![generator::row(RowNumber(1), 1, 10, at(0))])).expect("apply must succeed");

	assert!(harness.reclaim(1_000_000).expect("sweep must succeed").is_empty());
}

#[test]
fn an_append_operator_can_be_built_and_driven() {
	// Intent: same guard on the other multi-input operator. Append refuses a diff whose origin it
	// cannot resolve to one of its inputs, so a mistagged corpus fails here rather than quietly
	// driving nothing.
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
	// A single key puts every row on both sides into one bucket: the widest cartesian product a hash
	// join can be asked for, and the slot a latest join rewrites on every right arrival. The sweeps
	// above spread over three keys and rarely land there.
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
	// Intent: same failure mode the window sweeps guard against - a generator can narrow silently and
	// every sweep still passes while no longer covering what it was written for. These pin the regions
	// by name.
	//
	// A join that never sees both sides of a key cannot join; one that never sees several rows under
	// one key never reaches the cartesian paths; one that never draws an undefined key never reaches
	// the handlers that route it. For append, two inputs colliding on one source row number is the
	// whole reason the input index is in the group key.
	// Mutation: drop 1 from KEYS and the shared-bucket assertion fails; clamp right_pct to 0 and the
	// both-sides assertion fails; clamp append's row_space floor above 1 and the collision assertion
	// weakens.
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
		// With `inputs` inputs drawing from `row_space` numbers each, a collision across inputs is
		// near-certain over a run once the space is smaller than the rows drawn into it.
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
	// The control for join_matrix_definedness_flip_chaos: identical parameters, definedness held
	// fixed. A row's key may still be undefined from birth and may still move between defined keys -
	// only the crossing between the two is withheld. Green here while the flip sweep is red is what
	// pins a failure to that crossing rather than to the heavier undefined-key mix both of them carry.
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
	// Intent: every join sweep above is green, and green on its own is not evidence of anything. An
	// oracle that described no rows, a claim that was never compared, a corpus that never reached the
	// operator - all three look exactly like a passing suite. This drives each strategy against the
	// other three strategies' oracles and requires every one of those to come back divergent, which
	// is only possible if the claims genuinely describe four different tables and are genuinely
	// checked against what the operator published.
	// Mutation: make HashOracle ignore `left_outer`, or have any `claim()` return an empty view, and
	// the pairs that stop being distinguishable fail here.
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
	// Intent: the four fixed strategy sweeps are meant to be read against each other - the same rows
	// arriving in the same order, four different answers. That only holds while they execute the
	// identical operation sequence, and nothing about the corpus is visible in a green run, so this
	// is what holds them together. The fingerprint mixes every value the driver drew, so any
	// parameter drifting apart moves it, and a model that perturbed the driver by refusing a row
	// would move it too.
	// Mutation: change one field in one of the four sweeps and this fails; nothing else would notice.
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
	// against twelve right rows loaded up front and then frozen. That shape is what makes the
	// snapshot cells answerable at all - see drive_static_right - and it costs the non-snapshot cells
	// nothing, so all eight run the same corpus and stay comparable.
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
	// Intent: this is the whole promise of the flag. `snapshot` suppresses right-side emissions - it
	// is a statement about which changes are worth republishing, not about what the join contains. So
	// while the right side is static, turning it on must leave the published table byte-identical to
	// leaving it off. Any divergence means the flag is losing rows rather than losing work, and the
	// per-cell sweep above could not see it: both cells would simply be checked against their own
	// oracle and agree with it separately.
	// Mutation: make a snapshot right-side insert skip `add_to_state_entry_batch` as well as the
	// emission, and the pairs stop matching here.
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
			plain.view.rekey(&["lid".to_string(), "other_rid".to_string()]),
			snapshot.view.rekey(&["lid".to_string(), "other_rid".to_string()]),
			"{} answers differently with snapshot on; over a static right side the flag must only \
			 change how much is republished, never what the join holds",
			base.label()
		);
		assert!(!plain.view.is_empty(), "an empty view would let any two cells agree for free");
	}
}

#[test]
fn a_snapshot_join_stays_coherent_when_its_right_side_keeps_changing() {
	// Intent: the matrix above freezes the right side, which is the only shape a snapshot join has a
	// defined answer for - so it never actually reaches the suppression. This asks the other question:
	// when the right side DOES keep changing under a snapshot join, is the diff stream at least
	// something a sink can apply? The oracle cannot judge the contents here (suppressed emissions mean
	// the view is deliberately behind), but coherence is not a matter of interpretation: a remove of a
	// row that was never published, or an update to one that is absent, is wrong under any reading of
	// what snapshot promises.
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
	// The four snapshot cells with both sides interleaved, which the static-right matrix cannot reach.
	// A snapshot join takes the right side as it stands when a left row is touched and never revisits
	// that row afterwards, so the published table is a record of what each left row saw rather than a
	// function of the live sets - which is what SnapshotOracle tracks and what only a changing right
	// side can put under strain.
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
