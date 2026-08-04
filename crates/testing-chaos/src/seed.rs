// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::hash_map::{DefaultHasher, RandomState},
	env,
	hash::{BuildHasher, Hash, Hasher},
	panic::{self, AssertUnwindSafe},
	sync::LazyLock,
};

static PROCESS_BASE_SEED: LazyLock<u64> = LazyLock::new(random_base_seed);

pub fn run_iteration(name: &str, index: u64, body: fn(u64)) {
	let seed = iteration_seed(name, index);
	let outcome = panic::catch_unwind(AssertUnwindSafe(|| body(seed)));
	if let Err(payload) = outcome {
		report_failure(name, index, seed);
		panic::resume_unwind(payload);
	}
}

fn iteration_seed(name: &str, index: u64) -> u64 {
	resolve_seed(env_seed(), workload_base(*PROCESS_BASE_SEED, name), index)
}

fn workload_base(base: u64, name: &str) -> u64 {
	let mut h = DefaultHasher::new();
	base.hash(&mut h);
	name.hash(&mut h);
	h.finish()
}

fn resolve_seed(pinned: Option<u64>, base: u64, index: u64) -> u64 {
	pinned.unwrap_or_else(|| derive_seed(base, index))
}

fn report_failure(name: &str, index: u64, seed: u64) {
	eprintln!(
		"\nchaos \"{name}\" iteration {index} FAILED\n  seed:      {seed}\n  reproduce: make test-chaos SEED={seed} FILTER={name}_{index}"
	);
}

pub fn derive_seed(base: u64, salt: u64) -> u64 {
	let mut h = DefaultHasher::new();
	base.hash(&mut h);
	salt.hash(&mut h);
	h.finish()
}

fn random_base_seed() -> u64 {
	RandomState::new().build_hasher().finish()
}

fn env_seed() -> Option<u64> {
	env::var("SEED").ok().and_then(|s| s.trim().parse::<u64>().ok())
}

#[cfg(test)]
mod tests {
	use super::{derive_seed, resolve_seed, workload_base};

	#[test]
	fn same_index_seeds_differ_across_workloads() {
		// Under nextest every test owns a process, so a fresh PROCESS_BASE_SEED per process hid
		// that the seed ignored the workload name. One shared process (cargo test) exposes it:
		// without the name in the base, aggregate_count_chaos_5 and distinct_chaos_5 explore the
		// exact same point and the suite silently loses half its coverage.
		let base = 7;
		assert_ne!(
			resolve_seed(None, workload_base(base, "aggregate_count_chaos"), 5),
			resolve_seed(None, workload_base(base, "distinct_chaos"), 5)
		);
	}

	#[test]
	fn a_workload_base_is_stable_for_replay() {
		// Replay reproduces from the printed seed, so the same base and name must always derive
		// the same value; a hasher randomised per process would break every reported repro.
		assert_eq!(workload_base(7, "aggregate_count_chaos"), workload_base(7, "aggregate_count_chaos"));
		assert_ne!(workload_base(7, "aggregate_count_chaos"), workload_base(8, "aggregate_count_chaos"));
	}

	#[test]
	fn a_pinned_seed_still_overrides_the_workload_name() {
		// SEED= pins one exact point for reproduction; if the name salt leaked past the pin, a
		// reported failure would replay under a different seed than the one it printed.
		assert_eq!(resolve_seed(Some(42), workload_base(7, "aggregate_count_chaos"), 5), 42);
		assert_eq!(resolve_seed(Some(42), workload_base(9, "distinct_chaos"), 3), 42);
	}

	#[test]
	fn derive_seed_is_deterministic_and_decorrelated() {
		// Replay depends on this: a fixed base seed must reproduce the exact per-index seed.
		assert_eq!(derive_seed(1, 1), derive_seed(1, 1));
		assert_ne!(derive_seed(1, 1), derive_seed(1, 2));
		assert_ne!(derive_seed(1, 1), derive_seed(2, 1));
	}

	#[test]
	fn derived_iteration_seeds_are_distinct() {
		// Two iterations sharing a seed means the suite silently re-explores the same point.
		let mut seeds: Vec<u64> = (0..1000u64).map(|i| derive_seed(42, i)).collect();
		let total = seeds.len();
		seeds.sort_unstable();
		seeds.dedup();
		assert_eq!(seeds.len(), total, "iteration seeds collide");
	}

	#[test]
	fn pinned_seed_reproduces_exactly() {
		// A pin has to override the index, or replaying a reported failure would need to know
		// which index originally ran it.
		assert_eq!(resolve_seed(Some(42), 7, 3), 42);
		assert_eq!(resolve_seed(Some(42), 0, 0), 42);
		assert_eq!(resolve_seed(Some(42), 999, 31), 42);
	}

	#[test]
	fn unpinned_seed_is_per_index_and_per_base() {
		// Varying with the base is what makes a fresh base per run explore new seeds under the
		// same test name.
		assert_eq!(resolve_seed(None, 7, 3), derive_seed(7, 3));
		assert_ne!(resolve_seed(None, 7, 3), resolve_seed(None, 7, 4));
		assert_ne!(resolve_seed(None, 7, 3), resolve_seed(None, 8, 3));
	}
}
