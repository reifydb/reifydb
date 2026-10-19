// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::testing::dst_seed;

#[test]
fn dst_seed_reads_the_env_var_and_falls_back_to_random_without_it() {
	// Both cases share one env var, so they must run in one test or they race on it under a threaded runner.
	let seeded = {
		// SAFETY: this is the only test that touches REIFYDB_DST_SEED, so nothing reads or writes it
		// concurrently.
		unsafe {
			std::env::set_var("REIFYDB_DST_SEED", "424242");
		}
		dst_seed()
	};
	assert_eq!(seeded, 424242);

	// SAFETY: same single test, no concurrent environment access.
	unsafe {
		std::env::remove_var("REIFYDB_DST_SEED");
	}
	let _seed = dst_seed();
}
