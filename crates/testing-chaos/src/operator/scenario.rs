// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use rand::{RngExt, rngs::StdRng};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BatchSize {
	Constant(u32),

	Uniform(u32),

	Geometric {
		p: f64,
		max: u32,
	},
}

impl BatchSize {
	pub fn draw(&self, rng: &mut StdRng) -> u32 {
		match *self {
			BatchSize::Constant(n) => n.max(1),

			BatchSize::Uniform(max) => rng.random_range(1..=max.max(1)),
			BatchSize::Geometric {
				p,
				max,
			} => {
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Scenario {
	pub steps: u32,
	pub batch: BatchSize,
	pub remove_pct: u32,
	pub update_pct: u32,

	pub tick_pct: u32,

	pub coord_span_ms: u64,

	pub drain_at_ms: u64,

	pub max_live: Option<usize>,

	pub duplicate_update_burst: f64,

	pub update_as_remove_insert: f64,

	pub mixed_batches: bool,
}

impl Scenario {
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
			mixed_batches: false,
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

	pub fn with_mixed_batches(mut self) -> Self {
		self.mixed_batches = true;
		self
	}

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
		assert_eq!(
			before,
			{
				let mut c = rng(7);
				c.random::<u64>()
			},
			"sanity: same seed, same first draw"
		);
	}

	#[test]
	fn a_geometric_batch_stays_inside_its_bound_and_favours_small_batches() {
		// The point of the long tail is that most batches are small; if it degenerated to always-max it
		// would stop exercising the single-row path, and if it degenerated to always-1 it would stop
		// exercising batches spanning a boundary.
		let mut stream = rng(4242);
		let drawn: Vec<u32> = (0..400)
			.map(|_| {
				BatchSize::Geometric {
					p: 0.4,
					max: 8,
				}
				.draw(&mut stream)
			})
			.collect();
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
