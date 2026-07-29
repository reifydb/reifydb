// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt,
	panic::{self, AssertUnwindSafe},
};

use rand::{RngExt, SeedableRng, rngs::StdRng};

/// Splits one master seed into an independent parameter stream and a sequence seed.
///
/// They must not share a stream. The parameters decide WHAT is under test and the sequence decides
/// the corpus; drawing both from one stream means widening a parameter range silently reshuffles
/// every corpus as well, so nothing can be reasoned about across a generator change.
pub fn split(seed: u64) -> (StdRng, u64) {
	let mut master = StdRng::seed_from_u64(seed);
	let parameters: u64 = master.random();
	let sequence: u64 = master.random();
	(StdRng::seed_from_u64(parameters), sequence)
}

pub fn pick<T: Copy>(rng: &mut StdRng, options: &[T]) -> T {
	options[rng.random_range(0..options.len() as u32) as usize]
}

/// Grace expressed as a RATIO of the window size, not as an absolute.
///
/// The ratio is what changes behaviour: at 0 a window closes the instant it ends, at 1 the seal
/// horizon is twice the window, and above 1 a coordinate can be new enough to admit while already
/// being too old to contribute. That last band is where the rolling operator was withdrawing live
/// groups, and no absolute range would reliably land in it across sizes spanning two orders of
/// magnitude.
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

/// Total row work is capped so one unlucky draw cannot dominate the suite's runtime, and the three
/// mutation shares are drawn against a shrinking remainder rather than independently. Drawn
/// independently they can sum past 100, and a sweep that seals and retracts more often than it
/// inserts spends its steps against an operator that is mostly empty.
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

/// Runs a fuzzed sweep and, on failure, prints the RESOLVED parameters as something that can be
/// pasted straight into a regression file.
///
/// A pinned regression must carry its parameters explicitly, never a master seed. Pinning the
/// master seed would re-point the test at a different configuration the moment anything in this
/// module changes - and it would keep passing, so nothing would report that the defect it was
/// written for is no longer covered.
pub fn run_reported<P: fmt::Debug>(label: &str, sequence_seed: u64, params: &P, run: impl FnOnce()) {
	if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(run)) {
		eprintln!(
			"\nCHAOS FAILURE {label}\n  pin this, not the master seed:\n\n\tdrive(\n\t\t{sequence_seed},\n{},\n\t);\n",
			tab_indent(params)
		);
		panic::resume_unwind(payload);
	}
}

/// Re-indents `{:#?}` output with tabs so the reported parameters paste straight into a regression
/// file without a reformat. Debug always emits four spaces per level; this codebase uses tabs.
fn tab_indent<P: fmt::Debug>(params: &P) -> String {
	format!("{params:#?}")
		.lines()
		.map(|line| format!("\t\t{}", line.replace("    ", "\t")))
		.collect::<Vec<_>>()
		.join("\n")
}
