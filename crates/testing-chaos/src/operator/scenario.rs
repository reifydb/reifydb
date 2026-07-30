// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use rand::{RngExt, rngs::StdRng};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BatchSize {
	Constant(u32),

	Uniform {
		min: u32,
		max: u32,
	},

	Geometric {
		p: f64,
		max: u32,
	},
}

impl BatchSize {
	pub fn draw(&self, rng: &mut StdRng) -> u32 {
		match *self {
			BatchSize::Constant(n) => n.max(1),

			BatchSize::Uniform {
				min,
				max,
			} => {
				let min = min.max(1);
				rng.random_range(min..=max.max(min))
			}
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

#[derive(Debug, Clone, Copy)]
pub struct SupportedOps {
	pub insert: bool,
	pub update: bool,
	pub remove: bool,
}

impl Default for SupportedOps {
	fn default() -> Self {
		Self::all()
	}
}

impl SupportedOps {
	pub const fn all() -> Self {
		Self {
			insert: true,
			update: true,
			remove: true,
		}
	}

	pub const fn insert_only() -> Self {
		Self {
			insert: true,
			update: false,
			remove: false,
		}
	}

	pub const fn no_remove() -> Self {
		Self {
			insert: true,
			update: true,
			remove: false,
		}
	}

	pub const fn no_update() -> Self {
		Self {
			insert: true,
			update: false,
			remove: true,
		}
	}

	pub const fn is_reachable(&self) -> bool {
		self.insert || !(self.update || self.remove)
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
			batch: BatchSize::Uniform {
				min: 1,
				max: max_batch,
			},
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

	pub fn mixed(steps: u32) -> Self {
		Self {
			steps,
			batch: BatchSize::Geometric {
				p: 0.4,
				max: 8,
			},
			remove_pct: 25,
			update_pct: 30,
			tick_pct: 0,
			coord_span_ms: 0,
			drain_at_ms: 0,
			max_live: Some(50),
			duplicate_update_burst: 0.3,
			update_as_remove_insert: 0.1,
			mixed_batches: true,
		}
	}

	pub fn with_batch(mut self, batch: BatchSize) -> Self {
		self.batch = batch;
		self
	}

	pub fn with_ops(mut self, ops: SupportedOps) -> Self {
		self.remove_pct = if ops.remove {
			25
		} else {
			0
		};
		self.update_pct = if ops.update {
			30
		} else {
			0
		};
		self
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
		let drawn: Vec<u32> = (0..32)
			.map(|_| {
				BatchSize::Uniform {
					min: 1,
					max: 6,
				}
				.draw(&mut a)
			})
			.collect();
		let expected: Vec<u32> = (0..32).map(|_| b.random_range(1..=6u32)).collect();
		assert_eq!(drawn, expected, "a uniform batch must be the same draw it replaced");
	}

	#[test]
	fn a_uniform_batch_never_draws_below_its_floor() {
		// A floor above one is how a suite forces batches that span a boundary. If it were ignored the
		// draw would collapse to 1..=max, and every such suite would silently spend most of its steps on
		// the single-row path it was written to avoid.
		let mut stream = rng(2024);
		let drawn: Vec<u32> = (0..400)
			.map(|_| {
				BatchSize::Uniform {
					min: 5,
					max: 20,
				}
				.draw(&mut stream)
			})
			.collect();
		assert!(drawn.iter().all(|n| (5..=20).contains(n)), "a draw escaped the floor: {drawn:?}");
		assert!(drawn.iter().any(|n| *n < 20), "the draw must vary, not pin to the ceiling");
	}

	#[test]
	fn an_inverted_uniform_bound_still_draws_its_floor() {
		// max below min is a caller mistake, and the honest response is the floor rather than a panic
		// deep inside a chaos run that would read as a driver defect.
		let mut stream = rng(5);
		assert_eq!(
			BatchSize::Uniform {
				min: 9,
				max: 2,
			}
			.draw(&mut stream),
			9
		);
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
		assert_eq!(
			scenario.batch,
			BatchSize::Uniform {
				min: 1,
				max: 6,
			}
		);
	}

	#[test]
	fn a_mixed_scenario_has_no_clock_dimension_to_advance() {
		// The mixed corpus drives operators that never see a tick, so any non-zero clock share would
		// spend steps sealing a watermark nothing reads and starve the diff mix it exists to generate.
		let scenario = Scenario::mixed(200);
		assert_eq!(scenario.tick_pct, 0);
		assert_eq!(scenario.coord_span_ms, 0);
		assert_eq!(scenario.drain_at_ms, 0);
		assert!(scenario.mixed_batches, "a mixed scenario must pack its operations into one change");
	}

	#[test]
	fn disabling_an_operation_zeroes_its_share_rather_than_leaving_it_enabled() {
		// insert_only exists so a suite can isolate the accumulate path. A leftover remove or update
		// share would quietly break that isolation while the suite still passed.
		let insert_only = Scenario::mixed(10).with_ops(SupportedOps::insert_only());
		assert_eq!(insert_only.remove_pct, 0);
		assert_eq!(insert_only.update_pct, 0);

		let no_remove = Scenario::mixed(10).with_ops(SupportedOps::no_remove());
		assert_eq!(no_remove.remove_pct, 0);
		assert!(no_remove.update_pct > 0);

		let all = Scenario::mixed(10).with_ops(SupportedOps::all());
		assert!(all.remove_pct > 0 && all.update_pct > 0);
	}

	#[test]
	fn enabling_a_mutation_without_insert_is_unreachable() {
		// Nothing can be updated or removed before something is inserted, so this combination describes a
		// run that can never do anything. Catching it at configuration time is the difference between a
		// clear rejection and a suite that silently exercises an empty corpus.
		assert!(!SupportedOps {
			insert: false,
			update: true,
			remove: false,
		}
		.is_reachable());
		assert!(!SupportedOps {
			insert: false,
			update: false,
			remove: true,
		}
		.is_reachable());
		assert!(SupportedOps::all().is_reachable());
		assert!(SupportedOps::insert_only().is_reachable());
		assert!(
			SupportedOps {
				insert: false,
				update: false,
				remove: false,
			}
			.is_reachable(),
			"an all-disabled configuration is useless but not unreachable"
		);
	}
}
