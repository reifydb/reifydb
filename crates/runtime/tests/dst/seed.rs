// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::testing::dst_seed;

#[test]
fn dst_seed_reproduces_from_env_var() {
	// SAFETY: nextest runs each test in its own process, so this does not
	// race with other tests reading REIFYDB_DST_SEED.
	unsafe {
		std::env::set_var("REIFYDB_DST_SEED", "424242");
	}

	assert_eq!(dst_seed(), 424242);

	unsafe {
		std::env::remove_var("REIFYDB_DST_SEED");
	}
}

#[test]
fn dst_seed_falls_back_to_random_without_env_var() {
	unsafe {
		std::env::remove_var("REIFYDB_DST_SEED");
	}

	// No assertion on the value itself - only that resolving a seed
	// without REIFYDB_DST_SEED set does not panic.
	let _seed = dst_seed();
}
