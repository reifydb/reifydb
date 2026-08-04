// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod reclaim;

use reifydb_core::common::{WindowKind, WindowSize};
use reifydb_testing_chaos::{
	corpus::Corpus,
	fuzz::{pick, run_reported, split},
	operator::{
		drive as driver,
		drive::DriveOutcome,
		scenario::{BatchSize, Scenario},
	},
};
use reifydb_value::value::duration::Duration;

use crate::{
	framework::{fuzz, harness::Harness, workload::WindowWorkload},
	operators::window::{
		WindowSpec, build,
		count::{CountOracle, Ordinals},
		grid::{Fold, Grid, GridOracle},
	},
};

#[derive(Debug, Clone)]
pub struct Params {
	pub size_secs: u64,
	pub grace_secs: u64,
	pub groups: i32,
	pub steps: u32,
	pub max_batch: u32,
	pub coord_span_ms: u64,
	pub remove_pct: u32,
	pub update_pct: u32,
	pub seal_pct: u32,
}

struct TumblingGrid {
	size_ms: u64,
}

impl Grid for TumblingGrid {
	fn windows_of(&self, coord_ms: u64) -> Vec<u64> {
		vec![(coord_ms / self.size_ms) * self.size_ms]
	}
}

/// The same corpus and oracle as `drive`, with the sweep wired into the step loop. Separate rather
/// than a flag because a reclaiming run takes a different path through the loop, and it returns the
/// whole outcome so a caller can refuse a run that swept nothing.
pub fn drive_reclaiming(seed: u64, params: Params, reclaim_pct: u32, sink_row_ttl: bool) -> DriveOutcome {
	let size_ms = params.size_secs * 1_000;
	let grace_ms = params.grace_secs * 1_000;

	let spec = WindowSpec {
		kind: WindowKind::Tumbling {
			size: WindowSize::Duration(Duration::from_seconds(params.size_secs as i64).unwrap()),
		},
		group_by: "g",
		aggregations: "total: math::sum(v)",
		grace: Duration::from_seconds(params.grace_secs as i64).unwrap(),
	};

	// A window seals on its own timer, so `resolve_horizon` overrides whatever is declared here with
	// size + grace; declaring the same value keeps the call site honest.
	let span = Duration::from_milliseconds((size_ms + grace_ms) as i64).expect("span is representable");

	let mut harness = Harness::new(|runtime| build(&spec, runtime)).with_activity_grid();
	if sink_row_ttl {
		harness = harness.with_sink_row_ttl(span);
	}
	let workload = WindowWorkload {
		groups: params.groups,
		coord_span_ms: params.coord_span_ms,
	};
	let mut model = GridOracle::new(
		TumblingGrid {
			size_ms,
		},
		size_ms,
		grace_ms,
	);

	driver::drive(
		seed,
		Scenario::windowed(
			params.steps,
			params.max_batch,
			params.coord_span_ms,
			params.coord_span_ms + size_ms + grace_ms + 10_000,
		)
		.with_mix(params.remove_pct, params.update_pct, params.seal_pct)
		.with_reclaim(reclaim_pct),
		&mut harness,
		&workload,
		&mut model,
	)
	.assert_clean()
}

pub fn drive(seed: u64, params: Params) -> Corpus {
	drive_folded(seed, params, Fold::Sum)
}

/// The same corpus and grid under a different fold.
///
/// Min and max are not just different arithmetic here: `AggregateSlot::invertible` calls them
/// invertible only when grace is zero, so a graced window runs them through the sealing accumulator
/// rather than the multiset. Driving both grace settings is what reaches both.
pub fn drive_folded(seed: u64, params: Params, fold: Fold) -> Corpus {
	let size_ms = params.size_secs * 1_000;
	let grace_ms = params.grace_secs * 1_000;

	let spec = WindowSpec {
		kind: WindowKind::Tumbling {
			size: WindowSize::Duration(Duration::from_seconds(params.size_secs as i64).unwrap()),
		},
		group_by: "g",
		aggregations: fold.rql(),
		grace: Duration::from_seconds(params.grace_secs as i64).unwrap(),
	};

	let mut harness = Harness::new(|runtime| build(&spec, runtime));
	let workload = WindowWorkload {
		groups: params.groups,
		coord_span_ms: params.coord_span_ms,
	};
	let mut model = GridOracle::new(
		TumblingGrid {
			size_ms,
		},
		size_ms,
		grace_ms,
	)
	.with_fold(fold);

	driver::drive(
		seed,
		Scenario::windowed(
			params.steps,
			params.max_batch,
			params.coord_span_ms,
			params.coord_span_ms + size_ms + grace_ms + 10_000,
		)
		.with_mix(params.remove_pct, params.update_pct, params.seal_pct),
		&mut harness,
		&workload,
		&mut model,
	)
	.assert_clean()
	.corpus
}

/// Sizes spanning two orders of magnitude: a one second window over a twenty second corpus makes
/// almost every event late, a two minute window makes almost none, and the seal machinery behaves
/// differently at both ends.
const SIZE_SECS: [u64; 6] = [1, 5, 15, 30, 60, 120];

pub fn random_params(seed: u64) -> (u64, Params) {
	let (mut rng, sequence_seed) = split(seed);
	let size_secs = pick(&mut rng, &SIZE_SECS);
	let grace_secs = fuzz::grace_secs(&mut rng, size_secs);
	let coord_span_ms = fuzz::coord_span_ms(&mut rng, size_secs);
	let mix = fuzz::mix(&mut rng);
	let params = Params {
		size_secs,
		grace_secs,
		groups: mix.groups,
		steps: mix.steps,
		max_batch: mix.max_batch,
		coord_span_ms,
		remove_pct: mix.remove_pct,
		update_pct: mix.update_pct,
		seal_pct: mix.seal_pct,
	};
	(sequence_seed, params)
}

pub fn drive_random(seed: u64) {
	let (sequence_seed, params) = random_params(seed);
	let run = params.clone();
	run_reported("window_tumbling_random_chaos", sequence_seed, &params, || {
		drive(sequence_seed, run);
	});
}

/// A reclaiming run over a configuration drawn from the seed: the sweep/window interaction is governed
/// by the ratio between horizon, grid width and corpus span, which no hand-picked triple covers.
/// Vacuity is reported rather than asserted, since a drawn configuration may legitimately reach nothing.
pub fn drive_reclaiming_random(seed: u64) {
	let (sequence_seed, params) = random_params(seed);
	let run = params.clone();
	run_reported("window_tumbling_reclaim_random_chaos", sequence_seed, &params, || {
		drive_reclaiming(sequence_seed, run, 20, true);
	});
}

#[derive(Debug, Clone)]
pub struct CountParams {
	pub size_count: u64,
	pub groups: i32,
	pub steps: u32,
	pub max_batch: u32,
	pub coord_span_ms: u64,
	pub remove_pct: u32,
	pub update_pct: u32,
}

struct TumblingOrdinals {
	size_count: u64,
}

impl Ordinals for TumblingOrdinals {
	fn windows_of(&self, ordinal: u64) -> Vec<u64> {
		vec![ordinal / self.size_count]
	}
}

pub fn drive_count(seed: u64, params: CountParams) -> Corpus {
	let spec = WindowSpec {
		kind: WindowKind::Tumbling {
			size: WindowSize::Count(params.size_count),
		},
		group_by: "g",
		aggregations: "total: math::sum(v)",
		// A count window forces grace to zero through its own accessor, so the sweep declares it
		// zero rather than pretending it is a knob.
		grace: Duration::default(),
	};

	let mut harness = Harness::new(|runtime| build(&spec, runtime));
	let workload = WindowWorkload {
		groups: params.groups,
		coord_span_ms: params.coord_span_ms,
	};
	let mut model = CountOracle::new(TumblingOrdinals {
		size_count: params.size_count,
	});

	driver::drive(
		seed,
		// Nothing seals, so a tick can only be a no-op. Spending steps on it would shrink
		// the corpus for no coverage.
		Scenario::windowed(params.steps, params.max_batch, params.coord_span_ms, params.coord_span_ms)
			.with_mix(params.remove_pct, params.update_pct, 0),
		&mut harness,
		&workload,
		&mut model,
	)
	.assert_clean()
	.corpus
}

/// Deliberately includes 1: a window of one row per bucket puts every row in its own window and is
/// where any off-by-one in the ordinal division shows up first.
const SIZE_COUNTS: [u64; 5] = [1, 2, 4, 8, 16];

pub fn random_count_params(seed: u64) -> (u64, CountParams) {
	let (mut rng, sequence_seed) = split(seed);
	let size_count = pick(&mut rng, &SIZE_COUNTS);
	let mix = fuzz::mix(&mut rng);
	let params = CountParams {
		size_count,
		groups: mix.groups,
		steps: mix.steps,
		max_batch: mix.max_batch,
		coord_span_ms: 400_000,
		remove_pct: mix.remove_pct,
		update_pct: mix.update_pct,
	};
	(sequence_seed, params)
}

pub fn drive_count_random(seed: u64) {
	let (sequence_seed, params) = random_count_params(seed);
	let run = params.clone();
	run_reported("window_tumbling_count_random_chaos", sequence_seed, &params, || {
		drive_count(sequence_seed, run);
	});
}

/// The guest-side mutation primitives applied to a host window: an identical update resent, an update
/// split into a remove and an insert, and mutations concentrated onto a handful of rows. A windowed
/// aggregate has to survive all three.
pub fn drive_flow_shaped(seed: u64, params: Params) -> Corpus {
	let size_ms = params.size_secs * 1_000;
	let grace_ms = params.grace_secs * 1_000;

	let spec = WindowSpec {
		kind: WindowKind::Tumbling {
			size: WindowSize::Duration(Duration::from_seconds(params.size_secs as i64).unwrap()),
		},
		group_by: "g",
		aggregations: "total: math::sum(v)",
		grace: Duration::from_seconds(params.grace_secs as i64).unwrap(),
	};

	let mut harness = Harness::new(|runtime| build(&spec, runtime));
	let workload = WindowWorkload {
		groups: params.groups,
		coord_span_ms: params.coord_span_ms,
	};
	let mut model = GridOracle::new(
		TumblingGrid {
			size_ms,
		},
		size_ms,
		grace_ms,
	);

	driver::drive(
		seed,
		Scenario {
			batch: BatchSize::Geometric {
				p: 0.45,
				max: params.max_batch,
			},
			..Scenario::windowed(
				params.steps,
				params.max_batch,
				params.coord_span_ms,
				params.coord_span_ms + size_ms + grace_ms + 10_000,
			)
			.with_mix(params.remove_pct, params.update_pct, params.seal_pct)
			.with_max_live(24)
			.with_duplicate_update_burst(0.5)
			.with_update_as_remove_insert(0.35)
		},
		&mut harness,
		&workload,
		&mut model,
	)
	.assert_clean()
	.corpus
}
