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
	framework::{
		fuzz,
		harness::Harness,
		workload::{MovingWindowWorkload, WindowWorkload},
	},
	operators::window::{
		WindowSpec, build_immutable,
		grid::{Fold, Grid, GridOracle},
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
	fn windows_of(&self, coord_ms: u64) -> Vec<i64> {
		// The containment filter is the authority; the bounds only keep the range finite and are
		// deliberately loose so a wrong bound cannot silently drop a window.
		let coord = coord_ms as i64;
		let size = self.size_ms as i64;
		let slide = self.slide_ms as i64;
		let lowest = (coord - (size - 1)).div_euclid(slide);
		let highest = coord.div_euclid(slide);
		(lowest..=highest)
			.map(|wid| wid * slide)
			.filter(|start| coord >= *start && coord < start + size)
			.collect()
	}
}

pub fn drive(seed: u64, params: Params) -> Corpus {
	drive_folded(seed, params, Fold::Sum)
}

pub fn drive_folded(seed: u64, params: Params, fold: Fold) -> Corpus {
	drive_with(seed, params, fold, None, 0)
}

pub fn drive_immutable(seed: u64, params: Params, immutable_ms: u64) -> Corpus {
	drive_with(seed, params, Fold::PercentileNextToMin, Some(immutable_ms), 0)
}

pub fn drive_moving(seed: u64, params: Params, fold: Fold, move_pct: u32) -> Corpus {
	drive_with(seed, params, fold, None, move_pct)
}

fn drive_with(seed: u64, params: Params, fold: Fold, immutable_ms: Option<u64>, move_pct: u32) -> Corpus {
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
		aggregations: fold.rql(),
		lateness: Some(Duration::from_seconds(params.lateness_secs as i64).unwrap()),
	};

	let immutable = immutable_ms.map(|ms| Duration::from_milliseconds(ms as i64).unwrap());
	let mut harness = Harness::new(|runtime| build_immutable(&spec, immutable, runtime));
	let workload = MovingWindowWorkload {
		inner: WindowWorkload {
			groups: params.groups,
			coord_span_ms: params.coord_span_ms,
		},
		move_pct,
	};
	let mut model = GridOracle::new(
		SlidingGrid {
			size_ms,
			slide_ms,
		},
		size_ms,
		lateness_ms,
	)
	.with_fold(fold);
	if let Some(immutable_ms) = immutable_ms {
		model = model.with_immutable(immutable_ms);
	}

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
