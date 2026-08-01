// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Window-flavoured parameter draws. The shared seed-splitting, option-picking and reporting
//! primitives live in `reifydb_testing_chaos::fuzz`.

use rand::{RngExt, rngs::StdRng};
use reifydb_testing_chaos::fuzz::pick;

/// Grace as a ratio of the window size, not an absolute: only the ratio changes behaviour, and above
/// 1 a coordinate can be new enough to admit while already too old to contribute. No absolute range
/// lands in that band across sizes spanning two orders of magnitude.
const GRACE_RATIOS: [(u64, u64); 5] = [(0, 1), (1, 2), (1, 1), (2, 1), (3, 1)];

pub fn grace_secs(rng: &mut StdRng, size_secs: u64) -> u64 {
	let (numerator, denominator) = pick(rng, &GRACE_RATIOS);
	size_secs * numerator / denominator
}

/// The corpus spans a multiple of the window size, so the number of distinct windows stays bounded
/// however large the size is, and the share of the corpus that arrives late stays comparable.
pub fn coord_span_ms(rng: &mut StdRng, size_secs: u64) -> u64 {
	size_secs * 1_000 * rng.random_range(2..=20u64)
}

/// The knobs every window kind shares.
pub struct Mix {
	pub groups: i32,
	pub steps: u32,
	pub max_batch: u32,
	pub remove_pct: u32,
	pub update_pct: u32,
	pub seal_pct: u32,
}

/// Total row work is capped so one unlucky draw cannot dominate runtime, and the three mutation
/// shares are drawn against a shrinking remainder: drawn independently they can sum past 100, and a
/// sweep that seals and retracts more often than it inserts runs against a mostly empty operator.
pub fn mix(rng: &mut StdRng) -> Mix {
	const INSERT_FLOOR_PCT: u32 = 15;
	const MAX_ROWS: u32 = 240;

	let max_batch = rng.random_range(1..=6u32);
	let steps = rng.random_range(20..=80u32).min((MAX_ROWS / max_batch).max(20));
	let budget = 100 - INSERT_FLOOR_PCT;
	let seal_pct = rng.random_range(5..=40u32.min(budget));
	let remove_pct = rng.random_range(0..=30u32.min(budget - seal_pct));
	let update_pct = rng.random_range(0..=25u32.min(budget - seal_pct - remove_pct));

	Mix {
		groups: rng.random_range(1..=6i32),
		steps,
		max_batch,
		remove_pct,
		update_pct,
		seal_pct,
	}
}
