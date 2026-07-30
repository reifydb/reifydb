// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod regression;

use rand::RngExt;
use reifydb_core::common::{WindowKind, WindowSize};
use reifydb_testing_chaos::{
	corpus::Corpus,
	fuzz::{pick, run_reported, split},
};
use reifydb_value::value::duration::Duration;

use crate::{
	framework::{driver, fuzz},
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
	pub grace_secs: u64,
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
		// Windows start on multiples of the slide, so the candidates are bounded by the first
		// slide that could still reach coord_ms and the last one that has started by it. The
		// containment filter is the authority - the bounds are only there to keep the range
		// finite, deliberately loose so a wrong bound cannot silently drop a window.
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
	let grace_ms = params.grace_secs * 1_000;
	assert!(slide_ms < size_ms, "the sweep only covers overlapping sliding windows; slide must be < size");

	let spec = WindowSpec {
		kind: WindowKind::Sliding {
			size: WindowSize::Duration(Duration::from_seconds(params.size_secs as i64).unwrap()),
			slide: WindowSize::Duration(Duration::from_seconds(params.slide_secs as i64).unwrap()),
		},
		group_by: "g",
		aggregations: "total: math::sum(v)",
		grace: Duration::from_seconds(params.grace_secs as i64).unwrap(),
		lateness: Duration::default(),
	};

	driver::drive(
		seed,
		driver::Params {
			groups: params.groups,
			steps: params.steps,
			max_batch: params.max_batch,
			coord_span_ms: params.coord_span_ms,
			remove_pct: params.remove_pct,
			update_pct: params.update_pct,
			seal_pct: params.seal_pct,
			drain_at_ms: params.coord_span_ms + size_ms + grace_ms + 10_000,
		},
		|runtime| build(&spec, runtime),
		GridOracle::new(
			SlidingGrid {
				size_ms,
				slide_ms,
			},
			size_ms,
			grace_ms,
		),
	)
}

/// Every size here is divisible by 2, 3, 4 and 6, so the slide draw below lands on both divisors
/// of the size and values that leave a remainder - window coverage is not uniform when
/// `size % slide != 0`, and only the second kind exercises that.
const SIZE_SECS: [u64; 4] = [12, 30, 60, 120];

pub fn random_params(seed: u64) -> (u64, Params) {
	let (mut rng, sequence_seed) = split(seed);
	let size_secs = pick(&mut rng, &SIZE_SECS);
	// Bounded below so a coordinate never lands in more than about eight windows at once; an
	// unbounded slide of one second against a two minute size puts every row in 120 of them and
	// the sweep spends all its time in the accumulator.
	let slide_secs = rng.random_range((size_secs / 8).max(1)..size_secs);
	let grace_secs = fuzz::grace_secs(&mut rng, size_secs);
	let coord_span_ms = fuzz::coord_span_ms(&mut rng, size_secs);
	let mix = fuzz::mix(&mut rng);
	let params = Params {
		size_secs,
		slide_secs,
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
		// Window w starts at ordinal w * slide and spans `size` rows, so the n-th row of a group
		// belongs to every w whose span covers n. Stated from the definition rather than copied
		// from the operator's own index arithmetic - an oracle that reproduces the implementation
		// cannot disagree with it.
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
		grace: Duration::default(),
		lateness: Duration::default(),
	};

	driver::drive(
		seed,
		driver::Params {
			groups: params.groups,
			steps: params.steps,
			max_batch: params.max_batch,
			coord_span_ms: params.coord_span_ms,
			remove_pct: params.remove_pct,
			update_pct: params.update_pct,
			seal_pct: 0,
			drain_at_ms: params.coord_span_ms,
		},
		|runtime| build(&spec, runtime),
		CountOracle::new(SlidingOrdinals {
			size_count: params.size_count,
			slide_count: params.slide_count,
		}),
	)
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
