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
	use super::{derive_seed, resolve_seed};

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
