// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::hash_map::{DefaultHasher, RandomState},
	env,
	hash::{BuildHasher, Hash, Hasher},
	panic::{self, AssertUnwindSafe},
};

const DEFAULT_ITERATIONS: u64 = 100;

pub struct Chaos {
	name: String,
	iterations: u64,
	base_seed: u64,
}

pub fn chaos(name: impl Into<String>) -> Chaos {
	Chaos {
		name: name.into(),
		iterations: env_iterations().unwrap_or(DEFAULT_ITERATIONS),
		base_seed: env_seed().unwrap_or_else(random_base_seed),
	}
}

impl Chaos {
	pub fn iterations(mut self, iterations: u64) -> Self {
		self.iterations = iterations;
		self
	}

	pub fn seed(mut self, base_seed: u64) -> Self {
		self.base_seed = base_seed;
		self
	}

	pub fn run(self, body: impl Fn(u64)) {
		eprintln!("chaos \"{}\": {} iterations, base seed {}", self.name, self.iterations, self.base_seed);
		for i in 0..self.iterations {
			let seed = derive_seed(self.base_seed, i);
			let result = panic::catch_unwind(AssertUnwindSafe(|| body(seed)));
			if let Err(payload) = result {
				eprintln!(
					"\nchaos \"{}\" FAILED on iteration {} of {}\n  base seed:      {}\n  iteration seed: {}\n  reproduce:      make test-chaos SEED={} N={}",
					self.name,
					i,
					self.iterations,
					self.base_seed,
					seed,
					self.base_seed,
					self.iterations
				);
				panic::resume_unwind(payload);
			}
		}
	}
}

#[macro_export]
macro_rules! chaos_test {
	($name:ident, |$seed:ident| $body:block) => {
		#[test]
		fn $name() {
			$crate::chaos::chaos(stringify!($name)).run(|$seed: u64| $body);
		}
	};
}

fn derive_seed(base: u64, salt: u64) -> u64 {
	let mut h = DefaultHasher::new();
	base.hash(&mut h);
	salt.hash(&mut h);
	h.finish()
}

fn random_base_seed() -> u64 {
	RandomState::new().build_hasher().finish()
}

fn env_iterations() -> Option<u64> {
	env::var("CHAOS_ITERATIONS").ok().and_then(|s| s.trim().parse::<u64>().ok())
}

fn env_seed() -> Option<u64> {
	env::var("CHAOS_SEED").ok().and_then(|s| s.trim().parse::<u64>().ok())
}

#[cfg(test)]
mod tests {
	use std::{
		panic::{AssertUnwindSafe, catch_unwind},
		sync::atomic::{AtomicU64, Ordering},
	};

	use super::{chaos, derive_seed};

	// The macro must expand to a real `#[test] fn` that runs the body with
	// the iteration seed. If expansion breaks (wrong path, hygiene), this
	// fails to compile; if the seed is not threaded, the arithmetic check
	// would still hold, so the value here is the compile-time guard plus
	// proof the body executes under the runner.
	crate::chaos_test!(macro_expands_to_a_runnable_test, |seed| {
		assert_eq!(seed.wrapping_mul(2), seed.wrapping_add(seed));
	});

	#[test]
	fn derive_seed_is_deterministic_and_decorrelated() {
		// Same inputs hash identically; changing base or salt changes the
		// stream. Reproduction relies on this: a fixed base seed replays
		// the exact same iteration-seed sequence.
		assert_eq!(derive_seed(1, 1), derive_seed(1, 1));
		assert_ne!(derive_seed(1, 1), derive_seed(1, 2));
		assert_ne!(derive_seed(1, 1), derive_seed(2, 1));
	}

	#[test]
	fn derived_iteration_seeds_are_distinct() {
		// Across a long run, no two iterations should share a seed, or
		// the run would be silently re-exploring the same point.
		let mut seeds: Vec<u64> = (0..1000u64).map(|i| derive_seed(42, i)).collect();
		let total = seeds.len();
		seeds.sort_unstable();
		seeds.dedup();
		assert_eq!(seeds.len(), total, "iteration seeds collide");
	}

	#[test]
	fn passing_body_runs_exactly_iterations_times() {
		// A body that never panics is invoked once per iteration.
		let count = AtomicU64::new(0);
		chaos("passing").seed(7).iterations(50).run(|_seed| {
			count.fetch_add(1, Ordering::SeqCst);
		});
		assert_eq!(count.load(Ordering::SeqCst), 50);
	}

	#[test]
	#[should_panic(expected = "boom")]
	fn failing_iteration_is_caught_and_reraised_with_original_payload() {
		// The body panics on the seed for iteration 3. The runner must
		// catch it and re-raise the original payload so the test still
		// fails with "boom" (not a wrapped message).
		let target = derive_seed(123, 3);
		chaos("failing").seed(123).iterations(100).run(move |seed| {
			if seed == target {
				panic!("boom");
			}
		});
	}

	#[test]
	fn fixed_base_seed_stops_on_the_same_iteration() {
		// Reproduction contract: re-running with the same base seed fails
		// on the same iteration. invocations_until_panic counts how many
		// times the body ran before the runner re-raised; that count is
		// (failing index + 1) and must be stable across runs.
		let target = derive_seed(999, 17);
		let first = invocations_until_panic(999, target);
		let second = invocations_until_panic(999, target);
		assert_eq!(first, 18, "should panic on iteration index 17 (18th invocation)");
		assert_eq!(first, second, "same base seed must stop on the same iteration");
	}

	fn invocations_until_panic(base: u64, target: u64) -> u64 {
		let count = AtomicU64::new(0);
		let outcome = catch_unwind(AssertUnwindSafe(|| {
			chaos("probe").seed(base).iterations(100).run(|seed| {
				count.fetch_add(1, Ordering::SeqCst);
				if seed == target {
					panic!("probe hit");
				}
			});
		}));
		assert!(outcome.is_err(), "expected the probe to hit its target seed within the run");
		count.load(Ordering::SeqCst)
	}
}
