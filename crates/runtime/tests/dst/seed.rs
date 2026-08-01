// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::testing::dst_seed;

#[test]
fn dst_seed_reproduces_from_env_var() {
	// SAFETY: nextest runs each test in its own process, so no other thread reads or writes
	// the environment concurrently.
	unsafe {
		std::env::set_var("REIFYDB_DST_SEED", "424242");
	}

	assert_eq!(dst_seed(), 424242);

	// SAFETY: same single-threaded process, no concurrent environment access.
	unsafe {
		std::env::remove_var("REIFYDB_DST_SEED");
	}
}

#[test]
fn dst_seed_falls_back_to_random_without_env_var() {
	// SAFETY: nextest runs each test in its own process, so no other thread reads or writes
	// the environment concurrently.
	unsafe {
		std::env::remove_var("REIFYDB_DST_SEED");
	}

	// The value is unconstrained; only that resolving a seed without the env var set is not
	// a panic.
	let _seed = dst_seed();
}
