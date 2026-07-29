// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "chaos")]

#[path = "chaos/framework/mod.rs"]
mod framework;
#[path = "chaos/operators/mod.rs"]
mod operators;

use reifydb_core::common::{TimeDomain, WindowKind, WindowSize};
use reifydb_testing_macro::chaos_test;
use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::{
	framework::{generator, harness::Harness},
	operators::window::{WindowSpec, build},
};

fn tumbling_sum() -> WindowSpec {
	WindowSpec {
		kind: WindowKind::Tumbling {
			size: WindowSize::Duration(Duration::from_seconds(60).unwrap()),
		},
		domain: TimeDomain::Event,
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
	let change = generator::insert(vec![generator::row(1, 1, 10, at), generator::row(2, 1, 5, at)]);

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
		framework::fuzz::run_reported("format_check", 1234, &params, || panic!("deliberate"));
	});
	std::panic::set_hook(previous);

	assert!(outcome.is_err(), "run_reported must re-raise, otherwise a failing sweep reports success");
}
