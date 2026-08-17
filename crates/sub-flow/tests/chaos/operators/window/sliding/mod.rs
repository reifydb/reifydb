// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use rand::RngExt;
use reifydb_core::common::{WindowKind, WindowSize};
use reifydb_testing_chaos::{
	corpus::Corpus,
	fuzz::{pick, run_reported, split},
	operator::{drive as driver, scenario::Scenario},
};
use reifydb_value::value::duration::Duration;

use crate::{
	framework::{fuzz, harness::Harness, workload::WindowWorkload},
	operators::window::{
		WindowSpec, build,
		count::{CountOracle, Ordinals},
		grid::{Grid, GridOracle},
	},
};

#[derive(Debug, Clone)]
pub struct Params {
	pub size_secs: u64,
	pub slide_secs: u64,
	pub lateness_secs: u64,
	pub groups: i32,
	pub steps: u32,
	pub max_batch: u32,
	pub coord_span_ms: u64,
	pub remove_pct: u32,
	pub update_pct: u32,
	pub seal_pct: u32,
}

struct SlidingGrid {
	size_ms: u64,
	slide_ms: u64,
}

impl Grid for SlidingGrid {
	fn windows_of(&self, coord_ms: u64) -> Vec<u64> {
		// The containment filter is the authority; the bounds only keep the range finite and are
		// deliberately loose so a wrong bound cannot silently drop a window.
		let lowest = coord_ms.saturating_sub(self.size_ms.saturating_sub(1)) / self.slide_ms;
		let highest = coord_ms / self.slide_ms;
		(lowest..=highest)
			.map(|wid| wid * self.slide_ms)
			.filter(|start| coord_ms >= *start && coord_ms < start + self.size_ms)
			.collect()
	}
}

pub fn drive(seed: u64, params: Params) -> Corpus {
	let size_ms = params.size_secs * 1_000;
	let slide_ms = params.slide_secs * 1_000;
	let lateness_ms = params.lateness_secs * 1_000;
	assert!(slide_ms < size_ms, "the sweep only covers overlapping sliding windows; slide must be < size");

	let spec = WindowSpec {
		kind: WindowKind::Sliding {
			size: WindowSize::Duration(Duration::from_seconds(params.size_secs as i64).unwrap()),
			slide: WindowSize::Duration(Duration::from_seconds(params.slide_secs as i64).unwrap()),
		},
		group_by: "g",
		aggregations: "total: math::sum(v)",
		lateness: Some(Duration::from_seconds(params.lateness_secs as i64).unwrap()),
	};

	let mut harness = Harness::new(|runtime| build(&spec, runtime));
	let workload = WindowWorkload {
		groups: params.groups,
		coord_span_ms: params.coord_span_ms,
	};
	let mut model = GridOracle::new(
		SlidingGrid {
			size_ms,
			slide_ms,
		},
		size_ms,
		lateness_ms,
	);

	driver::drive(
		seed,
		Scenario::windowed(
			params.steps,
			params.max_batch,
			params.coord_span_ms,
			params.coord_span_ms + size_ms + lateness_ms + 10_000,
		)
		.with_mix(params.remove_pct, params.update_pct, params.seal_pct),
		&mut harness,
		&workload,
		&mut model,
	)
	.assert_clean()
	.corpus
}

/// Each size has several divisors inside the slide range drawn below, so the draw lands on both
/// divisors and values that leave a remainder - window coverage is not uniform when
/// `size % slide != 0`, and only the second kind exercises that.
const SIZE_SECS: [u64; 4] = [12, 30, 60, 120];

pub fn random_params(seed: u64) -> (u64, Params) {
	let (mut rng, sequence_seed) = split(seed);
	let size_secs = pick(&mut rng, &SIZE_SECS);
	// Bounded below so a coordinate never lands in more than about eight windows at once; a slide of
	// one second against a two minute size puts every row in 120 of them.
	let slide_secs = rng.random_range((size_secs / 8).max(1)..size_secs);
	let lateness_secs = fuzz::lateness_secs(&mut rng, size_secs);
	let coord_span_ms = fuzz::coord_span_ms(&mut rng, size_secs);
	let mix = fuzz::mix(&mut rng);
	let params = Params {
		size_secs,
		slide_secs,
		lateness_secs,
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
	run_reported("window_sliding_random_chaos", sequence_seed, &params, || {
		drive(sequence_seed, run);
	});
}

#[derive(Debug, Clone)]
pub struct CountParams {
	pub size_count: u64,
	pub slide_count: u64,
	pub groups: i32,
	pub steps: u32,
	pub max_batch: u32,
	pub coord_span_ms: u64,
	pub remove_pct: u32,
	pub update_pct: u32,
}

struct SlidingOrdinals {
	size_count: u64,
	slide_count: u64,
}

impl Ordinals for SlidingOrdinals {
	fn windows_of(&self, ordinal: u64) -> Vec<u64> {
		// Stated from the definition rather than copied from the operator's own index arithmetic -
		// an oracle that reproduces the implementation cannot disagree with it.
		let lowest = ordinal.saturating_sub(self.size_count.saturating_sub(1)) / self.slide_count;
		let highest = ordinal / self.slide_count;
		(lowest..=highest)
			.filter(|window| {
				let start = window * self.slide_count;
				ordinal >= start && ordinal < start + self.size_count
			})
			.collect()
	}
}

pub fn drive_count(seed: u64, params: CountParams) -> Corpus {
	assert!(
		params.slide_count < params.size_count,
		"the sweep only covers overlapping sliding windows; the planner rejects slide >= size"
	);

	let spec = WindowSpec {
		kind: WindowKind::Sliding {
			size: WindowSize::Count(params.size_count),
			slide: WindowSize::Count(params.slide_count),
		},
		group_by: "g",
		aggregations: "total: math::sum(v)",
		lateness: None,
	};

	let mut harness = Harness::new(|runtime| build(&spec, runtime));
	let workload = WindowWorkload {
		groups: params.groups,
		coord_span_ms: params.coord_span_ms,
	};
	let mut model = CountOracle::new(SlidingOrdinals {
		size_count: params.size_count,
		slide_count: params.slide_count,
	});

	driver::drive(
		seed,
		Scenario::windowed(params.steps, params.max_batch, params.coord_span_ms, params.coord_span_ms)
			.with_mix(params.remove_pct, params.update_pct, 0),
		&mut harness,
		&workload,
		&mut model,
	)
	.assert_clean()
	.corpus
}

const SIZE_COUNTS: [u64; 4] = [2, 4, 8, 16];

pub fn random_count_params(seed: u64) -> (u64, CountParams) {
	let (mut rng, sequence_seed) = split(seed);
	let size_count = pick(&mut rng, &SIZE_COUNTS);
	// Strictly below the size, which is the region the planner allows; 1 is included so the
	// maximally-overlapping case where a row lands in `size` windows at once is covered.
	let slide_count = rng.random_range(1..size_count);
	let mix = fuzz::mix(&mut rng);
	let params = CountParams {
		size_count,
		slide_count,
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
	run_reported("window_sliding_count_random_chaos", sequence_seed, &params, || {
		drive_count(sequence_seed, run);
	});
}
