// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod oracle;
pub mod regression;

use reifydb_core::common::{WindowKind, WindowSize};
use reifydb_value::value::duration::Duration;

use crate::{
	framework::{driver, fuzz},
	operators::window::{
		WindowSpec, build,
		rolling::oracle::{CapacityOracle, Oracle},
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

pub fn drive(seed: u64, params: Params) -> driver::Corpus {
	let size_ms = params.size_secs * 1_000;
	let grace_ms = params.grace_secs * 1_000;

	let spec = WindowSpec {
		kind: WindowKind::Rolling {
			size: WindowSize::Duration(Duration::from_seconds(params.size_secs as i64).unwrap()),
			lag: None,
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
		Oracle::new(size_ms, grace_ms),
	)
}

const SIZE_SECS: [u64; 6] = [1, 5, 15, 30, 60, 120];

pub fn random_params(seed: u64) -> (u64, Params) {
	let (mut rng, sequence_seed) = fuzz::split(seed);
	let size_secs = fuzz::pick(&mut rng, &SIZE_SECS);
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
	fuzz::run_reported("window_rolling_random_chaos", sequence_seed, &params, || {
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

pub fn drive_count(seed: u64, params: CountParams) -> driver::Corpus {
	let spec = WindowSpec {
		kind: WindowKind::Rolling {
			size: WindowSize::Count(params.size_count),
			lag: None,
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
		CapacityOracle::new(params.size_count),
	)
}

/// A capacity of 1 keeps only the newest row per group, so almost every retraction targets a row
/// the window has already pushed out - the case a capacity model is most likely to get wrong.
const SIZE_COUNTS: [u64; 5] = [1, 2, 4, 8, 16];

pub fn random_count_params(seed: u64) -> (u64, CountParams) {
	let (mut rng, sequence_seed) = fuzz::split(seed);
	let size_count = fuzz::pick(&mut rng, &SIZE_COUNTS);
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
	fuzz::run_reported("window_rolling_count_random_chaos", sequence_seed, &params, || {
		drive_count(sequence_seed, run);
	});
}
