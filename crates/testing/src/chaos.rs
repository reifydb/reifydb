// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(target_os = "linux")]
use std::fs;
use std::{
	collections::hash_map::{DefaultHasher, RandomState},
	env,
	hash::{BuildHasher, Hash, Hasher},
	panic::{self, AssertUnwindSafe},
	sync::LazyLock,
};

static PROCESS_BASE_SEED: LazyLock<u64> = LazyLock::new(random_base_seed);

pub fn run_iteration(name: &str, index: u64, body: fn(u64)) {
	let seed = iteration_seed(index);
	#[cfg(target_os = "linux")]
	let fds_before = open_fd_count();
	let outcome = panic::catch_unwind(AssertUnwindSafe(|| body(seed)));
	if let Err(payload) = outcome {
		report_failure(name, index, seed);
		panic::resume_unwind(payload);
	}
	#[cfg(target_os = "linux")]
	assert_no_fd_leak(name, index, seed, fds_before);
}

#[cfg(target_os = "linux")]
fn open_fd_count() -> usize {
	fs::read_dir("/proc/self/fd").map(|d| d.count()).unwrap_or(0)
}

#[cfg(target_os = "linux")]
fn assert_no_fd_leak(name: &str, index: u64, seed: u64, fds_before: usize) {
	const FD_SLACK: usize = 64;
	let fds_after = open_fd_count();
	assert!(
		fds_after <= fds_before + FD_SLACK,
		"chaos \"{name}\" iteration {index}: open file descriptors grew from {fds_before} to {fds_after} (slack \
		 {FD_SLACK}) across one iteration; a database lifecycle is leaking fds, the SQLITE_CANTOPEN failure mode \
		 (reproduce: make test-chaos SEED={seed} FILTER={name}_{index})"
	);
}

fn iteration_seed(index: u64) -> u64 {
	resolve_seed(env_seed(), *PROCESS_BASE_SEED, index)
}

fn resolve_seed(pinned: Option<u64>, base: u64, index: u64) -> u64 {
	pinned.unwrap_or_else(|| derive_seed(base, index))
}

fn report_failure(name: &str, index: u64, seed: u64) {
	eprintln!(
		"\nchaos \"{name}\" iteration {index} FAILED\n  seed:      {seed}\n  reproduce: make test-chaos SEED={seed} FILTER={name}_{index}"
	);
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

fn env_seed() -> Option<u64> {
	env::var("CHAOS_SEED").ok().and_then(|s| s.trim().parse::<u64>().ok())
}

#[cfg(test)]
mod tests {
	use reifydb_testing_macro::chaos_test;

	use super::{derive_seed, resolve_seed};

	// The macro must expand to real `#[test] fn`s, one per index, each running
	// the body with its iteration seed. An explicit count (3-arg form) pins this
	// self-test to 3 generated cases regardless of CHAOS_ITERATIONS. If expansion
	// breaks (wrong path, hygiene, or proc-macro wiring) this fails to compile;
	// the arithmetic on `seed` proves the seed is threaded into the body.
	chaos_test!(macro_expands_to_a_runnable_test, 3, |seed| {
		assert_eq!(seed.wrapping_mul(2), seed.wrapping_add(seed));
	});

	#[test]
	fn derive_seed_is_deterministic_and_decorrelated() {
		// Same inputs hash identically; changing base or salt changes the
		// stream. Reproduction relies on this: a fixed base seed replays the
		// exact same per-index seed.
		assert_eq!(derive_seed(1, 1), derive_seed(1, 1));
		assert_ne!(derive_seed(1, 1), derive_seed(1, 2));
		assert_ne!(derive_seed(1, 1), derive_seed(2, 1));
	}

	#[test]
	fn derived_iteration_seeds_are_distinct() {
		// Across many indices from one base, no two iterations should share a
		// seed, or the suite would silently re-explore the same point.
		let mut seeds: Vec<u64> = (0..1000u64).map(|i| derive_seed(42, i)).collect();
		let total = seeds.len();
		seeds.sort_unstable();
		seeds.dedup();
		assert_eq!(seeds.len(), total, "iteration seeds collide");
	}

	#[test]
	fn pinned_seed_reproduces_exactly() {
		// With CHAOS_SEED set, every index resolves to that exact seed, so a
		// reported failure replays by pinning the printed value - regardless
		// of which index originally ran it.
		assert_eq!(resolve_seed(Some(42), 7, 3), 42);
		assert_eq!(resolve_seed(Some(42), 0, 0), 42);
		assert_eq!(resolve_seed(Some(42), 999, 31), 42);
	}

	#[test]
	fn unpinned_seed_is_per_index_and_per_base() {
		// Without a pin, each index derives a distinct seed from the base,
		// and a different base yields a different seed for the same index.
		// The latter is the deterministic core of "different seeds every
		// run": random_base_seed() supplies a fresh base per run, so the
		// same-named test explores a new seed each time.
		assert_eq!(resolve_seed(None, 7, 3), derive_seed(7, 3));
		assert_ne!(resolve_seed(None, 7, 3), resolve_seed(None, 7, 4));
		assert_ne!(resolve_seed(None, 7, 3), resolve_seed(None, 8, 3));
	}
}
