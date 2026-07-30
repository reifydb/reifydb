// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use rand::{RngExt, rngs::StdRng};

/// How many rows one insert carries.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BatchSize {
	/// Always this many. What a suite wants when it is isolating one variable.
	Constant(u32),
	/// Uniform over `1..=max`.
	Uniform(u32),
	/// Geometric with the given success probability, clamped to `1..=max`.
	///
	/// Long-tailed on purpose: most batches are small and a few are large, which is what a real feed
	/// looks like and what finds the defects that only appear when a batch spans a boundary.
	Geometric { p: f64, max: u32 },
}

impl BatchSize {
	pub fn draw(&self, rng: &mut StdRng) -> u32 {
		match *self {
			BatchSize::Constant(n) => n.max(1),
			// Byte-identical to the draw this replaced, so the window regressions keep their pins.
			BatchSize::Uniform(max) => rng.random_range(1..=max.max(1)),
			BatchSize::Geometric { p, max } => {
				let max = max.max(1);
				let mut count = 1;
				while count < max && rng.random::<f64>() < p {
					count += 1;
				}
				count
			}
		}
	}
}

/// One chaos scenario: the operation mix and the mutation primitives applied to it.
///
/// This is the union of what the two families historically generated separately. A window family
/// rolled a mix of seal ticks, retractions and value updates over event coordinates; a guest family
/// rolled inserts, updates and removes and then mutated them the way an upstream flow does - resending
/// an identical update, splitting an update into a remove and an insert, colliding two keys onto one
/// row. Neither set was reachable from the other, so an operator was only ever exercised by half of
/// what the workspace knew how to generate.
///
/// The primitives all default to off, and an off primitive draws nothing from the RNG. That is what
/// lets a scenario expressed in the old window terms produce the exact sequence it used to, which is
/// load-bearing: five pinned regressions record a fingerprint of that sequence.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Scenario {
	pub steps: u32,
	pub batch: BatchSize,
	pub remove_pct: u32,
	pub update_pct: u32,
	/// Share of steps that advance the clock instead of mutating rows. Zero for a family whose operator
	/// has no time dimension.
	pub tick_pct: u32,
	/// Upper bound on how far a tick may jump; also the span coordinates are drawn from.
	pub coord_span_ms: u64,
	/// Where the final drain ticks to.
	pub drain_at_ms: u64,
	/// Cap on rows eligible for later mutation. `None` lets the corpus grow without bound, which keeps
	/// a window's coordinate space wide; a small cap concentrates mutations onto few rows, which is
	/// what makes conflicts and re-publishes frequent.
	pub max_live: Option<usize>,
	/// Probability an update is followed by an identical no-op update.
	///
	/// A LEFT JOIN re-emits a row unchanged when its other side moves. An operator that adds on every
	/// arrival rather than diffing counts that twice, which is invisible to a suite that never sends
	/// one.
	pub duplicate_update_burst: f64,
	/// Probability an update is sent as a remove of the pre-image followed by an insert of the post.
	///
	/// Semantically the same transition, structurally a different diff stream. An operator that handles
	/// one path and not the other diverges only here.
	pub update_as_remove_insert: f64,
}

impl Scenario {
	/// A scenario in the terms the window families use: a seal-tick share, no mutation primitives, and
	/// a uniform batch. Produces the same RNG draws as the mix this replaced.
	pub fn windowed(steps: u32, max_batch: u32, coord_span_ms: u64, drain_at_ms: u64) -> Self {
		Self {
			steps,
			batch: BatchSize::Uniform(max_batch),
			remove_pct: 0,
			update_pct: 0,
			tick_pct: 0,
			coord_span_ms,
			drain_at_ms,
			max_live: None,
			duplicate_update_burst: 0.0,
			update_as_remove_insert: 0.0,
		}
	}

	pub fn with_mix(mut self, remove_pct: u32, update_pct: u32, tick_pct: u32) -> Self {
		self.remove_pct = remove_pct;
		self.update_pct = update_pct;
		self.tick_pct = tick_pct;
		self
	}

	pub fn with_max_live(mut self, max_live: usize) -> Self {
		self.max_live = Some(max_live);
		self
	}

	pub fn with_duplicate_update_burst(mut self, p: f64) -> Self {
		self.duplicate_update_burst = p;
		self
	}

	pub fn with_update_as_remove_insert(mut self, p: f64) -> Self {
		self.update_as_remove_insert = p;
		self
	}

	/// Whether a probability is live. An off primitive must not draw, or every scenario expressed
	/// without it would produce a different sequence than it used to.
	pub(crate) fn rolls(p: f64) -> bool {
		p > 0.0
	}
}

#[cfg(test)]
mod tests {
	use rand::SeedableRng;

	use super::*;

	fn rng(seed: u64) -> StdRng {
		StdRng::seed_from_u64(seed)
	}

	#[test]
	fn a_uniform_batch_draws_exactly_what_the_windowed_mix_used_to() {
		// The window regressions pin a fingerprint of their operation sequence, and the batch size is
		// one of its draws. If this stopped matching `random_range(1..=max)` every one of those pins
		// would point at a different corpus while still passing.
		let mut a = rng(99);
		let mut b = rng(99);
		let drawn: Vec<u32> = (0..32).map(|_| BatchSize::Uniform(6).draw(&mut a)).collect();
		let expected: Vec<u32> = (0..32).map(|_| b.random_range(1..=6u32)).collect();
		assert_eq!(drawn, expected, "a uniform batch must be the same draw it replaced");
	}

	#[test]
	fn a_constant_batch_draws_nothing_from_the_stream() {
		// Isolating one variable means the batch size must not consume randomness, or changing it would
		// reshuffle everything downstream of it.
		let mut stream = rng(7);
		let before: u64 = stream.random();
		let mut probe = rng(7);
		let _: u64 = probe.random();
		assert_eq!(BatchSize::Constant(4).draw(&mut probe), 4);
		let after: u64 = probe.random();
		let mut control = rng(7);
		let _: u64 = control.random();
		let control_next: u64 = control.random();
		assert_eq!(after, control_next, "a constant batch must leave the stream untouched");
		assert_eq!(before,
			{ let mut c = rng(7); c.random::<u64>() }, "sanity: same seed, same first draw");
	}

	#[test]
	fn a_geometric_batch_stays_inside_its_bound_and_favours_small_batches() {
		// The point of the long tail is that most batches are small; if it degenerated to always-max it
		// would stop exercising the single-row path, and if it degenerated to always-1 it would stop
		// exercising batches spanning a boundary.
		let mut stream = rng(4242);
		let drawn: Vec<u32> = (0..400).map(|_| BatchSize::Geometric { p: 0.4, max: 8 }.draw(&mut stream)).collect();
		assert!(drawn.iter().all(|n| (1..=8).contains(n)), "every draw must respect the bound");
		let ones = drawn.iter().filter(|n| **n == 1).count();
		let maxed = drawn.iter().filter(|n| **n == 8).count();
		assert!(ones > maxed, "a geometric batch must favour small batches, got {ones} ones and {maxed} maxed");
		assert!(maxed > 0, "the tail must still reach the bound sometimes");
	}

	#[test]
	fn primitives_are_off_by_default_so_a_windowed_scenario_draws_nothing_extra() {
		// This is the property that keeps the five pinned window regressions valid across the merge.
		let scenario = Scenario::windowed(10, 6, 1_000, 2_000);
		assert!(!Scenario::rolls(scenario.duplicate_update_burst));
		assert!(!Scenario::rolls(scenario.update_as_remove_insert));
		assert_eq!(scenario.max_live, None);
		assert_eq!(scenario.batch, BatchSize::Uniform(6));
	}
}
