// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod oracle;

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
		grid::Fold,
		rolling::oracle::{CapacityOracle, Oracle},
	},
};

#[derive(Debug, Clone)]
pub struct Params {
	pub size_secs: u64,
	pub seal_secs: u64,
	pub groups: i32,
	pub steps: u32,
	pub max_batch: u32,
	pub coord_span_ms: u64,
	pub remove_pct: u32,
	pub update_pct: u32,
	pub seal_pct: u32,
}

pub fn drive(seed: u64, params: Params) -> Corpus {
	drive_folded(seed, params, Fold::Sum)
}

/// The rolling family under a different fold. Min and max are non-invertible whenever seal is
/// non-zero, and rolling is the only kind whose seal driver ages entries out of the seal tail, so
/// this is the one path that populates the sealing accumulator's sealed half.
pub fn drive_folded(seed: u64, params: Params, fold: Fold) -> Corpus {
	let size_ms = params.size_secs * 1_000;
	let seal_ms = params.seal_secs * 1_000;

	let spec = WindowSpec {
		kind: WindowKind::Rolling {
			size: WindowSize::Duration(Duration::from_seconds(params.size_secs as i64).unwrap()),
			lag: None,
		},
		group_by: "g",
		aggregations: fold.rql(),
		seal: Duration::from_seconds(params.seal_secs as i64).unwrap(),
	};

	let mut harness = Harness::new(|runtime| build(&spec, runtime));
	let workload = WindowWorkload {
		groups: params.groups,
		coord_span_ms: params.coord_span_ms,
	};
	let mut model = Oracle::new(size_ms, seal_ms).with_fold(fold);

	driver::drive(
		seed,
		Scenario::windowed(
			params.steps,
			params.max_batch,
			params.coord_span_ms,
			params.coord_span_ms + size_ms + seal_ms + 10_000,
		)
		.with_mix(params.remove_pct, params.update_pct, params.seal_pct),
		&mut harness,
		&workload,
		&mut model,
	)
	.assert_clean()
	.corpus
}

const SIZE_SECS: [u64; 6] = [1, 5, 15, 30, 60, 120];

pub fn random_params(seed: u64) -> (u64, Params) {
	let (mut rng, sequence_seed) = split(seed);
	let size_secs = pick(&mut rng, &SIZE_SECS);
	let seal_secs = fuzz::seal_secs(&mut rng, size_secs);
	let coord_span_ms = fuzz::coord_span_ms(&mut rng, size_secs);
	let mix = fuzz::mix(&mut rng);
	let params = Params {
		size_secs,
		seal_secs,
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
	run_reported("window_rolling_random_chaos", sequence_seed, &params, || {
		drive(sequence_seed, run);
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

pub fn drive_count(seed: u64, params: CountParams) -> Corpus {
	let spec = WindowSpec {
		kind: WindowKind::Rolling {
			size: WindowSize::Count(params.size_count),
			lag: None,
		},
		group_by: "g",
		aggregations: "total: math::sum(v)",
		seal: Duration::default(),
	};

	let mut harness = Harness::new(|runtime| build(&spec, runtime));
	let workload = WindowWorkload {
		groups: params.groups,
		coord_span_ms: params.coord_span_ms,
	};
	let mut model = CapacityOracle::new(params.size_count);

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

/// A capacity of 1 keeps only the newest row per group, so almost every retraction targets a row
/// the window has already pushed out - the case a capacity model is most likely to get wrong.
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
	run_reported("window_rolling_count_random_chaos", sequence_seed, &params, || {
		drive_count(sequence_seed, run);
	});
}
